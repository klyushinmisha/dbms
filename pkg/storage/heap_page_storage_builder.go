package storage

import (
	"dbms/pkg/cache"
	"dbms/pkg/concurrency"
	"os"
)

type HeapPageStorageBuilder struct {
	// required args
	file     *os.File
	pageSize int
	// optional args
	sharedPageLockTable *concurrency.LockTable
	fsm                 *FSM
	cache               cache.Cache
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

func (b *HeapPageStorageBuilder) UseCache(c cache.Cache) *HeapPageStorageBuilder {
	b.cache = c
	return b
}

func (b *HeapPageStorageBuilder) UseFSM(fsm *FSM) *HeapPageStorageBuilder {
	b.fsm = fsm
	return b
}

func (b *HeapPageStorageBuilder) Build() *HeapPageStorage {
	return NewHeapPageStorage(b.file, b.pageSize, b.cache, b.sharedPageLockTable, b.fsm)
}
