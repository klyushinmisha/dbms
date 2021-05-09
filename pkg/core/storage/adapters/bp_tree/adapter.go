package bp_tree

import (
	"dbms/pkg/core/storage"
	"dbms/pkg/core/transaction"
)

type BPTreeAdapter struct {
	tx *transaction.Tx
}

func NewBPTreeAdapter(tx *transaction.Tx) *BPTreeAdapter {
	var ba BPTreeAdapter
	ba.tx = tx
	return &ba
}

func (ba *BPTreeAdapter) ReadNodeAtPos(pos int64) *BPTreeNode {
	page := ba.tx.ReadPageAtPos(pos)
	bpa := newBPTreePageAdapter(page)
	return bpa.ReadNode()
}

func (ba *BPTreeAdapter) WriteNodeAtPos(node *BPTreeNode, pos int64) {
	page := storage.AllocatePage(ba.tx.StorageManager().PageSize())
	bpa := newBPTreePageAdapter(page)
	bpa.WriteNode(node)
	ba.tx.WritePageAtPos(page, pos)
}

func (ba *BPTreeAdapter) WriteNode(node *BPTreeNode) int64 {
	page := storage.AllocatePage(ba.tx.StorageManager().PageSize())
	bpa := newBPTreePageAdapter(page)
	bpa.WriteNode(node)
	return ba.tx.WritePage(page)
}

func (ba *BPTreeAdapter) Empty() bool {
	return ba.tx.StorageManager().Empty()
}
