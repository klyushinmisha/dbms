package buffer

/*import (
	"container/list"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"log"
	"sync"
	"sync/atomic"
)

// 1. allocate large chunk (cap * slotSize)
// 2. add storage.ReadBlock() which points to real buffer in mem (some sort of mem mapping)
// 3.

type bufferSlot struct {
	pos   int64
	page  *storage.HeapPage
	dirty bool
	// refcount allows to freeze page's position in list (not delete element before everyone release it)
	refcount int32
}

type Buffer struct {
	slotLockTable *concurrency.LockTable
	slotsCap        int
	// activeSlots used for concurrent access during pruning to prevent cache overflow in case of multiple Fetch() calls
	// first pruned to insert -> second found empty slotPos -> first made insert and made cache overflow
	activeSlots int32
	index       sync.Map
	storage     *storage.HeapPageStorage
	slotsLock   sync.Mutex
	slots       *list.List
}

func NewBuffer(storage *storage.HeapPageStorage, slotsCap int) *Buffer {
	var b Buffer
	b.slotLockTable = concurrency.NewLockTable()
	b.slotsCap = slotsCap
	b.storage = storage
	b.slots = list.New()
	return &b
}

func (b *Buffer) elevateSlotElement(e *list.Element) *list.Element {
	slot := e.Value.(*bufferSlot)
	b.slotsLock.Lock()
	defer b.slotsLock.Unlock()
	b.slots.Remove(e)
	return b.slots.PushFront(slot)
}

func (b *Buffer) pruneAndFetch(fetchPos int64) *list.Element {
	var fetchedE *list.Element
	var iterE *list.Element
	for {
		b.slotsLock.Lock()
		if iterE == nil {
			iterE = b.slots.Back()
		} else {
			iterE = iterE.Prev()
		}
		b.slotsLock.Unlock()
		if iterE == nil {
			return nil
		}
		slot := iterE.Value.(*bufferSlot)
		if slot.pos != fetchPos {
			b.slotLockTable.YieldLock(slot.pos, concurrency.ExclusiveMode)
			if b.deallocateSlotElement(iterE) {
				fetchedE = b.storage.ReadPageAtPosToSlot(fetchPos)
			}
			b.slotLockTable.Unlock(slot.pos)
			if fetchedE != nil {
				return fetchedE
			}
		}
	}
}

func (b *Buffer) Fetch1(pos int64) {
	b.slotLockTable.YieldLock(pos, concurrency.ExclusiveMode)
	defer b.slotsLock.Unlock()
	interfaceE, found := b.index.Load(pos)
	var e *list.Element
	if found {
		e = b.elevateSlotElement(interfaceE.(*list.Element))
	} else {
		atomic.AddInt32(&b.activeSlots, 1)
		e = b.pruneAndFetch(pos)
	}
	b.index.Store(pos, e)
}

// pos expected to be locked by concurrency.LockTable;
// so slotPos is locked by design
func (b *Buffer) Deallocate(pos int64) {
	b.slotLockTable.YieldLock(pos, concurrency.ExclusiveMode)
	defer b.slotsLock.Unlock()
	e, found := b.index.Load(pos)
	if !found {
		return
	}
	if b.deallocateSlotElement(e.(*list.Element)) {
		atomic.AddInt32(&b.activeSlots, -1)
	}
}

func (b *Buffer) Flush(pos int64) {
	b.slotLockTable.YieldLock(pos, concurrency.SharedMode)
	defer b.slotsLock.Unlock()
	slot := b.loadSlot(pos)
	if slot == nil {
		log.Panic("Trying to Flush not-existing slotPos")
	}
	b.flushSlot(slot)
}

func (b *Buffer) Pin(pos int64) {
	b.slotLockTable.YieldLock(pos, concurrency.SharedMode)
	defer b.slotsLock.Unlock()
	slot := b.loadSlot(pos)
	if slot == nil {
		log.Panic("Trying refcount not-existing slotPos")
	}
	atomic.AddInt32(&slot.refcount, 1)
}

func (b *Buffer) Unpin(pos int64) {
	b.slotLockTable.YieldLock(pos, concurrency.SharedMode)
	defer b.slotsLock.Unlock()
	slot := b.loadSlot(pos)
	if slot == nil {
		log.Panic("Trying Unpin not-existing slotPos")
	}
	atomic.AddInt32(&slot.refcount, -1)
}

func (b *Buffer) ReadPage(pos int64) *storage.HeapPage {
	b.slotLockTable.YieldLock(pos, concurrency.SharedMode)
	defer b.slotsLock.Unlock()
	slot := b.loadSlot(pos)
	if slot == nil {
		return nil
	}
	return slot.page
}

func (b *Buffer) WritePage(page *storage.HeapPage, pos int64) {
	b.slotLockTable.YieldLock(pos, concurrency.ExclusiveMode)
	defer b.slotsLock.Unlock()
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
	if slot.refcount == 0 {
		if slot.dirty {
			b.storage.WritePageAtPos(slot.page, slot.pos)
			slot.dirty = false
		}
	}
}

func (b *Buffer) prune(noLockPos int64) bool {
	if int(b.activeSlots) != b.slotsCap {
		return false
	}
	var e *list.Element
	for {
		b.slotsLock.Lock()
		if e == nil {
			e = b.slots.Back()
		} else {
			e = e.Prev()
		}
		b.slotsLock.Unlock()
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
	if slot.refcount != 0 {
		return false
	}
	b.index.Delete(slot.pos)
	b.slotsLock.Lock()
	b.slots.Remove(e)
	b.slotsLock.Unlock()
	b.flushSlot(slot)
	return true
}
*/
