package storage

/*
import (
	"dbms/pkg/concurrency"
	"os"
)

type HeapPageStorageBuilder struct {
	// required args
	file     *os.File
	pageSize int
	// optional args
	sharedPageLockTable *concurrency.LockTable
}

func NewHeapPageStorageBuilder(file *os.File, pageSize int) *HeapPageStorageBuilder {
	var builder HeapPageStorageBuilder
	builder.file = file
	builder.pageSize = pageSize
	return &builder
}

func (b *HeapPageStorageBuilder) UseLockTable(t *concurrency.LockTable) *HeapPageStorageBuilder {
	b.sharedPageLockTable = t
	return b
}

func (b *HeapPageStorageBuilder) Build() *HeapPageStorage {
	return NewHeapPageStorage(b.file, b.pageSize, b.sharedPageLockTable)
}
*/
