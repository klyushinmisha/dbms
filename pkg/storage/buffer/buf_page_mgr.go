package buffer

import "dbms/pkg/storage"

// BufferPageManager is and adapter for bufferSlotManager
type BufferPageManager struct {
	bufHdrMgr  *bufferHeaderManager
	bufSlotMgr *bufferSlotManager
}

func NewBufferPageManager(bufSlotMgr *bufferSlotManager) *BufferPageManager {
	var bufPageMgr BufferPageManager
	bufPageMgr.bufSlotMgr = bufSlotMgr
	return &bufPageMgr
}

func (m *BufferPageManager) ReadPageAtPos(pos int64) *storage.HeapPage {
	panic("implement me")
}

func (m *BufferPageManager) WritePageAtPos(page *storage.HeapPage, pos int64) {
	panic("implement me")
}

func (m *BufferPageManager) WritePage(page *storage.HeapPage) int64 {
	panic("implement me")
}

func (m *BufferPageManager) ClearPage(pos int64) {
	panic("implement me")
}
