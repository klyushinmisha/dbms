package buffer

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"log"
	"sync"
	"sync/atomic"
)

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

// TODO: make transaction-safe (pos lock is required at the moment)
func (m *bufferSlotManager) Fetch(pos int64) {
	if desc := m.storeOrWaitDesc(pos); desc != nil {
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

func (m *bufferSlotManager) Flush(pos int64) {
	desc := m.waitNotNilDesc(pos)
	desc.lock.YieldLock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	if hdr := m.bufHdrMgr.getHdrBySlotId(desc.slotId); hdr.dirty {
		m.storage.WriteBlock(pos, m.getBlockBySlotId(desc.slotId))
		hdr.dirty = false
	}
}

func (m *bufferSlotManager) Pin(pos int64) {
	desc := m.waitNotNilDesc(pos)
	m.bufHdrMgr.pin(desc.slotId)
}

func (m *bufferSlotManager) Unpin(pos int64) {
	desc := m.waitNotNilDesc(pos)
	m.bufHdrMgr.unpin(desc.slotId)
}

func (m *bufferSlotManager) ReadPageAtPos(pos int64) *storage.HeapPage {
	desc := m.waitNotNilDesc(pos)
	block := m.getBlockBySlotId(desc.slotId)
	page := new(storage.HeapPage)
	if unmarshalErr := page.UnmarshalBinary(block); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return page
}

func (m *bufferSlotManager) WritePageAtPos(page *storage.HeapPage, pos int64) {
	desc := m.waitNotNilDesc(pos)
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

func (m *bufferSlotManager) getBlockBySlotId(slotId int) []byte {
	pageStart := slotId * m.slotSize
	pageEnd := pageStart + m.slotSize
	return m.memPool[pageStart:pageEnd]
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
		if e, found := m.posToSlotMap.LoadOrStore(pos, nil); found {
			if e != nil {
				return e.(*bufferSlotDescriptor)
			}
		} else {
			return nil
		}
	}
}

func (m *bufferSlotManager) waitNotNilDesc(pos int64) *bufferSlotDescriptor {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panic("Nil descriptor unexpected")
	}
	return desc
}
