package buffer

import (
	"container/list"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"log"
	"sync"
	"sync/atomic"
)

type bufferHeader struct {
	desc     *bufferSlotDescriptor
	dirty    bool
	refcount int32
}

// add headers index to speed up lookups
type bufferHeaderManager struct {
	cap     int
	modLock sync.RWMutex
	// use list here because cap is already passed (no need to use hash-table, because no grow expected and slot id
	// directly points to node with no need in hash-function usage)
	idx  []*list.Element
	hdrs *list.List
}

func newBufferHeaderManager(slotsCap int) *bufferHeaderManager {
	var m bufferHeaderManager
	m.cap = slotsCap
	m.idx = make([]*list.Element, slotsCap, slotsCap)
	m.hdrs = list.New()
	return &m
}

func (m *bufferHeaderManager) getHdrBySlotId(slotId int) *bufferHeader {
	e := m.idx[slotId]
	if e == nil {
		return nil
	}
	return e.Value.(*bufferHeader)
}

func (m *bufferHeaderManager) pin(slotId int) {
	hdr := m.getHdrBySlotId(slotId)
	atomic.AddInt32(&hdr.refcount, 1)
}

func (m *bufferHeaderManager) unpin(slotId int) {
	hdr := m.getHdrBySlotId(slotId)
	atomic.AddInt32(&hdr.refcount, -1)
}

// victim uses an optimistic LRU prune algorithm
// returned slotId maybe already acquired, so new victim retrieval round will be started
func (m *bufferHeaderManager) victim() *bufferSlotDescriptor {
	// returns slotId
	m.modLock.RLock()
	defer m.modLock.RUnlock()
	for e := m.hdrs.Back(); e != nil; e = e.Prev() {
		hdr := e.Value.(*bufferHeader)
		if hdr.refcount == 0 {
			// pin
			atomic.AddInt32(&hdr.refcount, 1)
			return hdr.desc
		}
	}
	return nil
}

func (m *bufferHeaderManager) replaceAndElevateSlot(slotId int, pos int64) {
	m.modLock.Lock()
	defer m.modLock.Unlock()
	e := m.idx[slotId]
	if e != nil {
		m.hdrs.Remove(e)
	}
	var hdr bufferHeader
	hdr.desc = &bufferSlotDescriptor{pos, slotId, concurrency.NewLock()}
	m.idx[slotId] = m.hdrs.PushFront(&hdr)
}

func (m *bufferHeaderManager) elevateSlot(slotId int) {
	m.modLock.Lock()
	defer m.modLock.Unlock()
	e := m.idx[slotId]
	if e == nil {
		log.Panic("slot not found")
	}
	hdr := m.hdrs.Remove(e)
	m.idx[slotId] = m.hdrs.PushFront(hdr)
}

type bufferSlotDescriptor struct {
	pos    int64
	slotId int
	lock   *concurrency.Lock
}

type bufferSlotManager struct {
	bufHdrMgr    *bufferHeaderManager
	memPool      []byte
	cap          int
	slotSize     int
	activeSlots  int32
	storage      *storage.StorageManager
	pruneLock    sync.Mutex
	posToSlotMap sync.Map
}

func newBufferSlotManager(storage *storage.StorageManager, slots int, slotSize int) *bufferSlotManager {
	var m bufferSlotManager
	m.bufHdrMgr = newBufferHeaderManager(slots)
	m.memPool = make([]byte, slots*slotSize, slots*slotSize)
	m.cap = slots
	m.slotSize = slotSize
	m.storage = storage
	return &m
}

func (m *bufferSlotManager) getDescFromPos(pos int64) (*bufferSlotDescriptor, bool) {
	e, found := m.posToSlotMap.LoadOrStore(pos, nil)
	if !found {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.(*bufferSlotDescriptor), true
}

func (m *bufferSlotManager) getBlockBySlotId(slotId int) []byte {
	pageStart := slotId * m.slotSize
	pageEnd := pageStart + m.slotSize
	return m.memPool[pageStart:pageEnd]
}

func (m *bufferSlotManager) Pin(pos int64) {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panicf("page not found %v", pos)
	}
	m.bufHdrMgr.pin(desc.slotId)
}

func (m *bufferSlotManager) Unpin(pos int64) {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panicf("page not found %v", pos)
	}
	m.bufHdrMgr.unpin(desc.slotId)
}
func (m *bufferSlotManager) acquireSlotId() int {
	// m.activeSlots can't be decreased, so try to acquire slot until achieve m.cap
	for {
		curSlotId := m.activeSlots
		if int(curSlotId) == m.cap {
			return -1
		}
		nextSlotId := curSlotId + 1
		if atomic.CompareAndSwapInt32(&m.activeSlots, curSlotId, nextSlotId) {
			return int(curSlotId)
		}
	}
}

func (m *bufferSlotManager) storeOrWaitDesc(pos int64) *bufferSlotDescriptor {
	for {
		// spinlock here; wait for pos to be fetched to slot
		if desc, found := m.getDescFromPos(pos); found {
			if desc != nil {
				return desc
			}
		} else {
			return nil
		}
	}
}

// TODO: make transaction-safe (pos lock is required at the moment)
func (m *bufferSlotManager) Fetch(pos int64) {
	desc := m.storeOrWaitDesc(pos)
	if desc != nil {
		return
	}
	slotId := m.acquireSlotId()
	if slotId == -1 {
		var desc *bufferSlotDescriptor
		for {
			desc = m.bufHdrMgr.victim()
			// TODO: busy waiting here. Better use cond var
			if desc == nil {
				continue
			}
			slotId = desc.slotId
			if !desc.lock.TryLock(concurrency.ExclusiveMode) {
				m.bufHdrMgr.unpin(slotId)
				continue
			}
			break
		}
		defer desc.lock.Unlock()
		defer m.bufHdrMgr.unpin(slotId)
		m.posToSlotMap.Delete(desc.pos)
	}
	m.bufHdrMgr.replaceAndElevateSlot(slotId, pos)
	// read block to slot
	m.storage.ReadBlock(pos, m.getBlockBySlotId(slotId))
	// slotId is has exclusive access here and is pinned
	m.posToSlotMap.Store(pos, &bufferSlotDescriptor{pos, slotId, concurrency.NewLock()})
}

func (m *bufferSlotManager) ReadPageAtPos(pos int64) *storage.HeapPage {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panicf("page not found %v", pos)
	}
	block := m.getBlockBySlotId(desc.slotId)
	var page storage.HeapPage
	if unmarshalErr := page.UnmarshalBinary(block); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return &page
}

func (m *bufferSlotManager) WritePageAtPos(page *storage.HeapPage, pos int64) {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panicf("page not found %v", pos)
	}
	newBlock, marshalErr := page.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	desc.lock.YieldLock(concurrency.ExclusiveMode)
	defer desc.lock.Unlock()
	m.bufHdrMgr.getHdrBySlotId(desc.slotId).dirty = true
	oldBlock := m.getBlockBySlotId(desc.slotId)
	copy(oldBlock, newBlock)
}

func (m *bufferSlotManager) Flush(pos int64) {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panicf("page not found %v", pos)
	}
	desc.lock.YieldLock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	if hdr := m.bufHdrMgr.getHdrBySlotId(desc.slotId); hdr.dirty {
		m.storage.WriteBlock(pos, m.getBlockBySlotId(desc.slotId))
		hdr.dirty = false
	}
}
