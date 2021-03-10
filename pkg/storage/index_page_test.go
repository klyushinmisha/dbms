package storage

import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestIndexPage_ReadWriteIndexNode(t *testing.T) {
	data := make([]byte, 1024, 1024)
	var p IndexPage
	err := p.UnmarshalBinary(data)
	if err != nil {
		log.Panic(err)
	}
	p.Init()

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
