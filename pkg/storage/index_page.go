package storage

import (
	"bytes"
	"encoding/binary"
	"log"
)

// bool + int64 + int64 + int64 + int32
var bPlusTreeNodeHeaderSize = 29

type bPlusTreeNodeHeader struct {
	Leaf   bool
	Parent int64
	Left   int64
	Right  int64
	Size   int32
}

type BPlusTreeNode struct {
	bPlusTreeNodeHeader
	Keys     []string
	Pointers []int64
}

const INDEX_PAGE byte = 1

type IndexPage struct {
	*HeapPage
}

func AllocateIndexPage(pageSize int) *IndexPage {
	var p IndexPage
	p.HeapPage = AllocatePage(pageSize, INDEX_PAGE)
	return &p
}

func IndexPageFromHeapPage(p *HeapPage) *IndexPage {
	var convPage IndexPage
	convPage.HeapPage = p
	return &convPage
}

func (p *IndexPage) WriteIndexNode(node *BPlusTreeNode) {
	var writeErr error
	headerWritePos := len(p.data) - bPlusTreeNodeHeaderSize
	p.FreeSpace = int32(headerWritePos - 4)
	buffer := bytes.NewBuffer(p.data[headerWritePos:headerWritePos])
	writeErr = binary.Write(buffer, binary.LittleEndian, node.bPlusTreeNodeHeader)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	p.writePointer(0, int32(headerWritePos))
	keyEnd := headerWritePos
	var keyStart int
	for i := 0; i < int(node.Size); i++ {
		bytesKey := []byte(node.Keys[i])
		keyStart = keyEnd - len(bytesKey)
		p.FreeSpace -= int32(keyEnd - keyStart + 4)
		if p.FreeSpace < 0 {
			log.Panic("implementation limitation: can't fit index")
		}
		copy(p.data[keyStart:keyEnd], bytesKey)
		keyEnd = keyStart
		p.writePointer(i+1, int32(keyStart))
	}
	var pointerStart int
	if node.Size == 0 {
		pointerStart = headerWritePos - 8
	} else {
		pointerStart = keyStart - 8
	}
	for i := 0; i < int(node.Size)+1; i++ {
		p.FreeSpace -= 12
		if p.FreeSpace < 0 {
			log.Panic("implementation limitation: can't fit index")
		}
		buffer = bytes.NewBuffer(p.data[pointerStart:pointerStart])
		writeErr = binary.Write(buffer, binary.LittleEndian, node.Pointers[i])
		if writeErr != nil {
			log.Panic(writeErr)
		}
		p.writePointer(i+int(node.Size)+1, int32(pointerStart))
		pointerStart -= 8
	}
	// TODO: calculate free space during write and panic if limit exceeded
}

func (p *IndexPage) ReadIndexNode() *BPlusTreeNode {
	var readErr error
	var node BPlusTreeNode
	headerStart := p.readPointer(0)
	reader := bytes.NewReader(p.data[headerStart:])
	readErr = binary.Read(reader, binary.LittleEndian, &node.bPlusTreeNodeHeader)
	if readErr != nil {
		log.Panic(readErr)
	}
	node.Keys = make([]string, node.Size)
	node.Pointers = make([]int64, node.Size+1)
	keyEnd := headerStart
	for i := 0; i < int(node.Size); i++ {
		keyStart := p.readPointer(i + 1)
		// TODO: maybe []byte?
		node.Keys[i] = string(p.data[keyStart:keyEnd])
		keyEnd = keyStart
	}
	for i := 0; i < int(node.Size)+1; i++ {
		pointerStart := p.readPointer(int(node.Size) + i + 1)
		reader = bytes.NewReader(p.data[pointerStart : pointerStart+8])
		readErr = binary.Read(reader, binary.LittleEndian, &node.Pointers[i])
		if readErr != nil {
			log.Panic(readErr)
		}
	}
	return &node
}
