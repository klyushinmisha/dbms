package storage

type PageManager interface {
	ReadPageAtPos(pos int64) *HeapPage
	WritePageAtPos(page *HeapPage, pos int64)
	WritePage(page *HeapPage) int64
	ClearPage(pos int64)
}
