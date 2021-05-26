package buffer

import (
	"dbms/internal/core/concurrency"
	"dbms/internal/core/storage"
	"log"
	"sync"
)

type bufferSlotDescriptor struct {
	pos    int64
	slotId int
	lock   *concurrency.Lock
}

type BufferSlotManager struct {
	bufHdrMgr    *bufferHeaderManager
	memPool      []byte
	cap          int
	slotSize     int
	storage      *storage.StorageManager
	posToSlotMap sync.Map
}

func NewBufferSlotManager(storage *storage.StorageManager, slots int, slotSize int) *BufferSlotManager {
	var m BufferSlotManager
	m.bufHdrMgr = newBufferHeaderManager(slots)
	m.memPool = make([]byte, slots*slotSize, slots*slotSize)
	m.cap = slots
	m.slotSize = slotSize
	m.storage = storage
	return &m
}

// TODO: make transaction-safe (pos lock is required at the moment)
func (m *BufferSlotManager) Fetch(pos int64) {
	if desc := m.storeOrWaitDesc(pos); desc != nil {
		return
	}
	slotId := m.bufHdrMgr.allocateSlot()
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

func (m *BufferSlotManager) Flush(pos int64) {
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	if hdr := m.bufHdrMgr.getHdrBySlotId(desc.slotId); hdr.dirty {
		m.storage.WriteBlock(pos, m.getBlockBySlotId(desc.slotId))
		hdr.dirty = false
	}
}

func (m *BufferSlotManager) Deallocate(pos int64) {
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.ExclusiveMode)
	defer desc.lock.Unlock()
	if hdr := m.bufHdrMgr.getHdrBySlotId(desc.slotId); hdr.refCtr.Value() != 0 {
		return
	}
	m.bufHdrMgr.deallocateSlot(desc.slotId)
	m.posToSlotMap.Delete(desc.pos)
}

func (m *BufferSlotManager) Pin(pos int64) {
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	m.bufHdrMgr.pin(desc.slotId)
}

func (m *BufferSlotManager) Unpin(pos int64) {
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	m.bufHdrMgr.unpin(desc.slotId)
}

func (m *BufferSlotManager) ReadPageAtPos(pos int64) *storage.HeapPage {
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	block := m.getBlockBySlotId(desc.slotId)
	page := new(storage.HeapPage)
	if unmarshalErr := page.UnmarshalBinary(block); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return page
}

func (m *BufferSlotManager) WritePageAtPos(page *storage.HeapPage, pos int64) {
	newBlock, marshalErr := page.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.ExclusiveMode)
	defer desc.lock.Unlock()
	m.bufHdrMgr.getHdrBySlotId(desc.slotId).dirty = true
	oldBlock := m.getBlockBySlotId(desc.slotId)
	copy(oldBlock, newBlock)
}

// ReadPageAtPos modification; returns nil if page is not dirty (for logging purposes)
func (m *BufferSlotManager) ReadPageIfDirty(pos int64) *storage.HeapPage {
	desc := m.waitNotNilDesc(pos)
	desc.lock.Lock(concurrency.SharedMode)
	defer desc.lock.Unlock()
	if hdr := m.bufHdrMgr.getHdrBySlotId(desc.slotId); !hdr.dirty {
		return nil
	}
	block := m.getBlockBySlotId(desc.slotId)
	page := new(storage.HeapPage)
	if unmarshalErr := page.UnmarshalBinary(block); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return page
}

func (m *BufferSlotManager) getBlockBySlotId(slotId int) []byte {
	pageStart := slotId * m.slotSize
	pageEnd := pageStart + m.slotSize
	return m.memPool[pageStart:pageEnd]
}

func (m *BufferSlotManager) storeOrWaitDesc(pos int64) *bufferSlotDescriptor {
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

func (m *BufferSlotManager) waitNotNilDesc(pos int64) *bufferSlotDescriptor {
	desc := m.storeOrWaitDesc(pos)
	if desc == nil {
		log.Panic("Nil descriptor unexpected")
	}
	return desc
}
