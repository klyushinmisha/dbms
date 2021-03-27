package bp_tree

import (
	"dbms/pkg/storage/adapters/bp_tree"
)

type bpTreeReaderWriter struct {
	t  int
	ba *bp_tree.BPTreeAdapter
}

func NewBPTreeReaderWriter(t int, ba *bp_tree.BPTreeAdapter) *bpTreeReaderWriter {
	var rw bpTreeReaderWriter
	rw.t = t
	rw.ba = ba
	return &rw
}

func (rw *bpTreeReaderWriter) ReadNodeFromStorage(pos int64) *BPTreeNode {
	var n BPTreeNode
	storeNode := rw.ba.ReadNodeAtPos(pos)
	n.Leaf = storeNode.Leaf()
	n.Parent = storeNode.Parent
	n.Left = storeNode.Left
	n.Right = storeNode.Right
	n.Size = int(storeNode.Size)
	n.Keys = make([]string, 2*rw.t, 2*rw.t)
	n.Pointers = make([]int64, 2*rw.t+1, 2*rw.t+1)
	copy(n.Keys, storeNode.Keys)
	copy(n.Pointers, storeNode.Pointers)
	return &n
}

func marshalToStoreNode(n *BPTreeNode) *bp_tree.BPTreeNode {
	var storeNode bp_tree.BPTreeNode
	storeNode.SetLeaf(n.Leaf)
	storeNode.Parent = n.Parent
	storeNode.Left = n.Left
	storeNode.Right = n.Right
	storeNode.Size = int32(n.Size)
	storeNode.Keys = n.Keys[:n.Size]
	storeNode.Pointers = n.Pointers[:n.Size+1]
	return &storeNode
}

func (rw *bpTreeReaderWriter) WriteNodeToStorage(n *BPTreeNode, pos int64) {
	rw.ba.WriteNodeAtPos(marshalToStoreNode(n), pos)
}

func (rw *bpTreeReaderWriter) AppendNodeToStorage(n *BPTreeNode) int64 {
	return rw.ba.WriteNode(marshalToStoreNode(n))
}

// ReleaseNodeInStorage marks node as free to use it for other nodes
func (rw *bpTreeReaderWriter) ReleaseNodeInStorage(pos int64) {
	rw.ba.ReleaseNode(pos)
}

func (rw *bpTreeReaderWriter) Empty() bool {
	return rw.ba.Empty()
}
