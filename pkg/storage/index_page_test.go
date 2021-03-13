package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndexPage_ReadWriteIndexNode(t *testing.T) {
	p := AllocateIndexPage(1024)

	var node BPlusTreeNode
	node.Leaf = true
	node.Parent = -1
	node.Left = 2
	node.Right = 4
	node.Size = 1
	node.Keys = []string{"HELLO"}
	node.Pointers = []int64{1, 1}

	p.WriteIndexNode(&node)
	nodeCopy := p.ReadIndexNode()
	assert.Equal(t, node, *nodeCopy)
}
