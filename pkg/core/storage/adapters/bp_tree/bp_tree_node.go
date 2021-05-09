package bp_tree

import "dbms/pkg/core/storage"

// bool + int64 + int64 + int64 + int32
var bpTreeNodeHeaderSize = 29

type bpTreeNodeHeader struct {
	Flags  storage.BitArray
	Parent int64
	Left   int64
	Right  int64
	Size   int32
}

func (hd *bpTreeNodeHeader) Leaf() bool {
	return hd.Flags.Get(0)
}

func (hd *bpTreeNodeHeader) SetLeaf(leaf bool) {
	hd.Flags.Set(leaf, 0)
}

type BPTreeNode struct {
	bpTreeNodeHeader
	Keys     []string
	Pointers []int64
}
