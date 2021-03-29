package bp_tree

import (
	"dbms/pkg/storage"
)

type BPTreeAdapter struct {
	storage *storage.HeapPageStorage
}

func NewBPTreeAdapter(storage *storage.HeapPageStorage) *BPTreeAdapter {
	var ba BPTreeAdapter
	ba.storage = storage
	return &ba
}

func (ba *BPTreeAdapter) ReadNodeAtPos(pos int64) *BPTreeNode {
	if ba.storage.LockTable() != nil {
		ba.storage.LockTable().YieldLock(pos)
		defer ba.storage.LockTable().Unlock(pos)
	}
	page := ba.storage.ReadPageAtPos(pos)
	bpa := newBPTreePageAdapter(page)
	return bpa.ReadNode()
}

func (ba *BPTreeAdapter) WriteNodeAtPos(node *BPTreeNode, pos int64) {
	if ba.storage.LockTable() != nil {
		ba.storage.LockTable().YieldLock(pos)
		defer ba.storage.LockTable().Unlock(pos)
	}
	page := storage.AllocatePage(ba.storage.PageSize())
	bpa := newBPTreePageAdapter(page)
	bpa.WriteNode(node)
	ba.storage.WritePageAtPos(page, pos)
}

func (ba *BPTreeAdapter) WriteNode(node *BPTreeNode) int64 {
	page := storage.AllocatePage(ba.storage.PageSize())
	bpa := newBPTreePageAdapter(page)
	bpa.WriteNode(node)
	pos := ba.storage.WritePage(page)
	if ba.storage.LockTable() != nil {
		defer ba.storage.LockTable().Unlock(pos)
	}
	return pos
}

func (ba *BPTreeAdapter) ReleaseNode(pos int64) {
	if ba.storage.LockTable() != nil {
		ba.storage.LockTable().YieldLock(pos)
		defer ba.storage.LockTable().Unlock(pos)
	}
	ba.storage.ReleaseNode(pos)
}

func (ba *BPTreeAdapter) Empty() bool {
	return ba.storage.Empty()
}
