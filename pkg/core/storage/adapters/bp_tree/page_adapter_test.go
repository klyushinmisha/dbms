package bp_tree

import (
	"dbms/pkg/core/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBPTreePageAdapter_ReadWriteIndexNode(t *testing.T) {
	p := storage.AllocatePage(1024)
	bpa := newBPTreePageAdapter(p)

	var node BPTreeNode
	node.SetLeaf(true)
	node.Parent = -1
	node.Left = 2
	node.Right = 4
	node.Size = 1
	node.Keys = []string{"HELLO"}
	node.Pointers = []int64{1, 1}

	bpa.WriteNode(&node)
	nodeCopy := bpa.ReadNode()
	assert.Equal(t, node, *nodeCopy)
}
