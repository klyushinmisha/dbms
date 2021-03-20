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
	page := ba.storage.ReadPage(pos)
	bpa := newBPTreePageAdapter(page)
	return bpa.ReadNode()
}

func (ba *BPTreeAdapter) WriteNodeAtPos(node *BPTreeNode, pos int64) {
	page := storage.AllocatePage(ba.storage.PageSize())
	bpa := newBPTreePageAdapter(page)
	bpa.WriteNode(node)
	ba.storage.WritePage(page, pos)
}

func (ba *BPTreeAdapter) WriteNode(node *BPTreeNode) int64 {
	page := storage.AllocatePage(ba.storage.PageSize())
	bpa := newBPTreePageAdapter(page)
	bpa.WriteNode(node)
	pos := ba.storage.GetFreePagePosition()
	ba.storage.WritePage(page, pos)
	return pos
}

func (ba *BPTreeAdapter) Empty() bool {
	return ba.storage.Empty()
}
