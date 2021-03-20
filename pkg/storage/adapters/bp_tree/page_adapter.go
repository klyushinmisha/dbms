package bp_tree

import (
	"bytes"
	"dbms/pkg/storage"
	"encoding/binary"
	"log"
)

var (
	bpTreePointerSize = 8
)

type bpTreePageAdapter struct {
	page *storage.HeapPage
}

func newBPTreePageAdapter(page *storage.HeapPage) *bpTreePageAdapter {
	var bpa bpTreePageAdapter
	bpa.page = page
	return &bpa
}

func (bpa *bpTreePageAdapter) WriteNode(node *BPTreeNode) {
	hdrBuf := make([]byte, bpTreeNodeHeaderSize, bpTreeNodeHeaderSize)
	// TODO: remove writer write klutch
	writer := bytes.NewBuffer(hdrBuf[0:0])
	if writeErr := binary.Write(writer, binary.LittleEndian, node.bpTreeNodeHeader); writeErr != nil {
		log.Panic(writeErr)
	}
	bpa.page.AppendData(hdrBuf)
	for i := 0; i < int(node.Size); i++ {
		bpa.page.AppendData([]byte(node.Keys[i]))
		if bpa.page.FreeSpace() < 0 {
			log.Panic("implementation limitation: can't fit index")
		}
	}
	ptrBuf := make([]byte, bpTreePointerSize, bpTreePointerSize)
	// TODO: remove writer write klutch
	writer = bytes.NewBuffer(ptrBuf[0:0])
	for i := 0; i < int(node.Size)+1; i++ {
		if writeErr := binary.Write(writer, binary.LittleEndian, node.Pointers[i]); writeErr != nil {
			log.Panic(writeErr)
		}
		bpa.page.AppendData(ptrBuf)
		if bpa.page.FreeSpace() < 0 {
			log.Panic("implementation limitation: can't fit index")
		}
		writer.Reset()
	}
}

func (bpa *bpTreePageAdapter) ReadNode() *BPTreeNode {
	heapRecPtr := 0
	hdrData := bpa.page.ReadData(heapRecPtr)
	heapRecPtr++
	reader := bytes.NewReader(hdrData)
	node := new(BPTreeNode)
	if readErr := binary.Read(reader, binary.LittleEndian, &node.bpTreeNodeHeader); readErr != nil {
		log.Panic(readErr)
	}
	node.Keys = make([]string, node.Size)
	node.Pointers = make([]int64, node.Size+1)
	for i := 0; i < int(node.Size); i++ {
		node.Keys[i] = string(bpa.page.ReadData(heapRecPtr))
		heapRecPtr++
	}
	for i := 0; i < int(node.Size)+1; i++ {
		ptrData := bpa.page.ReadData(heapRecPtr)
		reader = bytes.NewReader(ptrData)
		if readErr := binary.Read(reader, binary.LittleEndian, &node.Pointers[i]); readErr != nil {
			log.Panic(readErr)
		}
		heapRecPtr++
	}
	return node
}
