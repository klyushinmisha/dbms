package access

import "dbms/pkg/storage"

type indexReaderWriter struct {
	t    int
	disk *storage.IndexDiskIO
}

func NewIndexReaderWriter(t int, disk *storage.IndexDiskIO) *indexReaderWriter {
	var rw indexReaderWriter
	rw.t = t
	rw.disk = disk
	return &rw
}

func (rw *indexReaderWriter) ReadNodeFromDisk(pos int64) *BPlusTreeNode {
	var n BPlusTreeNode
	diskNode := rw.disk.ReadPage(pos).ReadIndexNode()
	n.Leaf = diskNode.Leaf()
	n.Parent = diskNode.Parent
	n.Left = diskNode.Left
	n.Right = diskNode.Right
	n.Size = int(diskNode.Size)
	n.Keys = make([]string, 2*rw.t, 2*rw.t)
	n.Pointers = make([]int64, 2*rw.t+1, 2*rw.t+1)
	copy(n.Keys, diskNode.Keys)
	copy(n.Pointers, diskNode.Pointers)
	return &n
}

func (rw *indexReaderWriter) WriteNodeOnDisk(n *BPlusTreeNode, pos int64) {
	var diskNode storage.BPlusTreeNode
	diskNode.SetLeaf(n.Leaf)
	diskNode.Parent = n.Parent
	diskNode.Left = n.Left
	diskNode.Right = n.Right
	diskNode.Size = int32(n.Size)
	diskNode.Keys = n.Keys[:n.Size]
	diskNode.Pointers = n.Pointers[:n.Size+1]
	page := storage.AllocateIndexPage(rw.disk.PageSize())
	page.WriteIndexNode(&diskNode)
	rw.disk.WritePage(pos, page)
}

func (rw *indexReaderWriter) Empty() bool {
	return rw.disk.IsFileEmpty()
}

func (rw *indexReaderWriter) GetNextPagePosition() int64 {
	return rw.disk.GetNextPagePosition()
}
