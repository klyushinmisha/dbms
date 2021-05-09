package buffer

import (
	"container/list"
	"dbms/pkg/atomic"
	"dbms/pkg/core/concurrency"
	"log"
	"sync"
)

type bufferHeader struct {
	desc   *bufferSlotDescriptor
	dirty  bool
	refCtr atomic.AtomicCounter
}

// add headers index to speed up lookups
type bufferHeaderManager struct {
	cap     int
	modLock sync.RWMutex
	// use list here because cap is already passed (no need to use hash-table, because no grow expected and slot id
	// directly points to node with no need in hash-function usage)
	idx      []*list.Element
	hdrs     *list.List
	freeList *list.List
}

func newBufferHeaderManager(slotsCap int) *bufferHeaderManager {
	var m bufferHeaderManager
	m.cap = slotsCap
	m.idx = make([]*list.Element, slotsCap, slotsCap)
	m.hdrs = list.New()
	m.freeList = list.New()
	for slotId := 0; slotId < slotsCap; slotId++ {
		m.freeList.PushFront(slotId)
	}
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
	hdr.refCtr.Incr()
}

func (m *bufferHeaderManager) unpin(slotId int) {
	hdr := m.getHdrBySlotId(slotId)
	hdr.refCtr.Decr()
}

// victim uses an optimistic LRU prune algorithm
// returned slotId maybe already acquired, so new victim retrieval round will be started
func (m *bufferHeaderManager) victim() *bufferSlotDescriptor {
	// returns slotId
	m.modLock.RLock()
	defer m.modLock.RUnlock()
	for e := m.hdrs.Back(); e != nil; e = e.Prev() {
		hdr := e.Value.(*bufferHeader)
		if hdr.refCtr.Value() == 0 {
			// pin
			hdr.refCtr.Incr()
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

func (m *bufferHeaderManager) allocateSlot() int {
	m.modLock.Lock()
	defer m.modLock.Unlock()
	if m.freeList.Len() == 0 {
		return -1
	}
	e := m.freeList.Front()
	m.freeList.Remove(e)
	return e.Value.(int)
}

func (m *bufferHeaderManager) deallocateSlot(slotId int) {
	m.modLock.Lock()
	defer m.modLock.Unlock()
	e := m.idx[slotId]
	if e == nil {
		log.Panic("slot not found")
	}
	m.hdrs.Remove(e)
	m.idx[slotId] = nil
	m.freeList.PushFront(slotId)
}
