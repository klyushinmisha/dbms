package buffer

import (
	"container/list"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"log"
	"sync"
	"sync/atomic"
)

type bufferSlot struct {
	pos   int64
	page  *storage.HeapPage
	dirty bool
	// pin allows to freeze page's position in list (not delete element before everyone release it)
	pin int32
}

func (s *bufferSlot) Pinned() bool {
	return s.pin != 0
}

/*
NOTE: sometimes cache can fail to store items (if all pages are pinned);
so need to check some page access stats to queue clients if can't put everything in cache

TODO: move slot upper if can access

TODO: split slot modification operations and cache list and index modification operations

GOTCHA: pin/unpin - for cache-level locking to prevent page removal from cache
concurrency.LockTable for transaction-level locking to prevent concurrent access to page content


rwlock are acquired in table for given pos
so no need for slotsLock

concurrency occurres at slots mod level, not at slot level

Main case:

page := Fetch(pos)
Pin(pos)

ReadPage(page)
WritePage(page)
ReadPage(page)
WritePage(page)
ReadPage(page)
WritePage(page)

Unpin(pos)
Flush(pos)
Deallocate(pos)

*/

// TODO: Fetch new pages written directly

type Buffer struct {
	sharedLockTable *concurrency.LockTable
	slotsCap        int
	// activeSlots used for concurrent access during pruning to prevent cache overflow in case of multiple Fetch() calls
	// first pruned to insert -> second found empty slot -> first made insert and made cache overflow
	activeSlots int32
	index       sync.Map
	storage     *storage.HeapPageStorage
	slotsLock   sync.Mutex
	slots       *list.List
}

func NewBuffer(storage *storage.HeapPageStorage, sharedLockTable *concurrency.LockTable, slotsCap int) *Buffer {
	var b Buffer
	b.sharedLockTable = sharedLockTable
	b.slotsCap = slotsCap
	b.storage = storage
	b.slots = list.New()
	return &b
}

// pos expected to be locked by concurrency.LockTable;
// so slot is locked by design
func (b *Buffer) Fetch(pos int64) {
	var e *list.Element
	interfaceE, found := b.index.Load(pos)
	if found {
		e = interfaceE.(*list.Element)
		slot := e.Value.(*bufferSlot)
		func() {
			b.slotsLock.Lock()
			defer b.slotsLock.Unlock()
			b.slots.Remove(e)
			e = b.slots.PushFront(slot)
			b.index.Store(pos, e)
		}()
	} else {
		// pre-increment to force other Fetch() clients to prune at their call instead of highjacking pruned slot
		activeSlots := atomic.AddInt32(&b.activeSlots, 1)
		if int(activeSlots) > b.slotsCap {
			if !b.prune(pos) {
				log.Panic("Cache overflow")
			}
			atomic.AddInt32(&b.activeSlots, -1)
		}
		var slot bufferSlot
		slot.page = b.storage.ReadPageAtPos(pos)
		slot.pos = pos
		func() {
			b.slotsLock.Lock()
			defer b.slotsLock.Unlock()
			e = b.slots.PushFront(&slot)
			b.index.Store(pos, e)
		}()
	}
}

// pos expected to be locked by concurrency.LockTable;
// so slot is locked by design
func (b *Buffer) Deallocate(pos int64) {
	e, found := b.index.Load(pos)
	if !found {
		return
	}
	if b.deallocateSlotElement(e.(*list.Element)) {
		atomic.AddInt32(&b.activeSlots, -1)
	}
}

func (b *Buffer) Flush(pos int64) {
	slot := b.loadSlot(pos)
	if slot == nil {
		log.Panic("Trying to flush not-existing slot")
	}
	b.flushSlot(slot)
}

func (b *Buffer) Pin(pos int64) {
	slot := b.loadSlot(pos)
	if slot == nil {
		log.Panic("Trying pin not-existing slot")
	}
	atomic.AddInt32(&slot.pin, 1)
}

func (b *Buffer) Unpin(pos int64) {
	slot := b.loadSlot(pos)
	if slot == nil {
		log.Panic("Trying unpin not-existing slot")
	}
	atomic.AddInt32(&slot.pin, -1)
}

func (b *Buffer) ReadPage(pos int64) *storage.HeapPage {
	slot := b.loadSlot(pos)
	if slot == nil {
		return nil
	}
	return slot.page
}

func (b *Buffer) WritePage(page *storage.HeapPage, pos int64) {
	slot := b.loadSlot(pos)
	if slot == nil {
		return
	}
	slot.dirty = true
	slot.page = page
}

func (b *Buffer) loadSlot(pos int64) *bufferSlot {
	e, found := b.index.Load(pos)
	if !found {
		return nil
	}
	return e.(*list.Element).Value.(*bufferSlot)
}

func (b *Buffer) flushSlot(slot *bufferSlot) {
	if slot.pin == 0 {
		if slot.dirty {
			b.storage.WritePageAtPos(slot.page, slot.pos)
		}
	}
}

func (b *Buffer) prune(noLockPos int64) bool {
	if int(b.activeSlots) != b.slotsCap {
		return false
	}
	var e *list.Element
	for {
		func() {
			// prevent invalid neighbour retrieval
			b.slotsLock.Lock()
			defer b.slotsLock.Unlock()
			if e == nil {
				e = b.slots.Back()
			} else {
				e = e.Prev()
			}
		}()
		if e == nil {
			return false
		}
		if func() bool {
			pos := e.Value.(*bufferSlot).pos
			if pos != noLockPos {
				if b.sharedLockTable != nil {
					b.sharedLockTable.YieldLock(pos, concurrency.ExclusiveMode)
					defer func() {
						b.sharedLockTable.Unlock(pos)
					}()
				}
			}
			return b.deallocateSlotElement(e)
		}() {
			return true
		}
	}
}

func (b *Buffer) deallocateSlotElement(e *list.Element) bool {
	slot := e.Value.(*bufferSlot)
	if slot.pin != 0 {
		return false
	}
	b.index.Delete(slot.pos)
	func() {
		// prevent concurrent list access
		b.slotsLock.Lock()
		defer b.slotsLock.Unlock()
		b.slots.Remove(e)
	}()
	b.flushSlot(slot)
	return true
}

/*

проблема: синхронизация нескольких выталкиваний

*/
