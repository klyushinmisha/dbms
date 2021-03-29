package pkg

import (
	"dbms/pkg/access"
	"dbms/pkg/access/bp_tree"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	bpAdapter "dbms/pkg/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/storage/adapters/data"
	"log"
)

type Executor struct {
	index           access.Index
	da              *dataAdapter.DataAdapter
	recordLockTable *concurrency.LockTable
}

func InitExecutor(
	indexStorage *storage.HeapPageStorage,
	dataStorage *storage.HeapPageStorage,
) *Executor {
	var e Executor
	e.recordLockTable = concurrency.NewLockTable()
	e.da = dataAdapter.NewDataAdapter(dataStorage)
	e.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(indexStorage))
	e.index.Init()
	return &e
}

func (e *Executor) Get(key string) ([]byte, bool) {
	e.recordLockTable.YieldLock(key)
	defer e.recordLockTable.Unlock(key)
	pos, findErr := e.index.Find(key)
	if findErr == bp_tree.ErrKeyNotFound {
		return nil, false
	}
	if findErr != nil {
		log.Panic(findErr)
	}
	data, findErr := e.da.FindAtPos(key, pos)
	if findErr == dataAdapter.ErrRecordNotFound {
		log.Panic("index and data page mismatch")
	}
	return data, true
}

func (e *Executor) Set(key string, data []byte) {
	// leads to deadlock
	e.recordLockTable.YieldLock(key)
	defer e.recordLockTable.Unlock(key)
	pos, findErr := e.index.Find(key)
	if findErr == nil {
		writeErr := e.da.WriteAtPos(key, data, pos, true)
		if writeErr == dataAdapter.ErrPageIsFull {
			return
		}
		if writeErr != nil {
			log.Panic(writeErr)
		}
	} else if findErr != bp_tree.ErrKeyNotFound {
		log.Panic(findErr)
	}
	writePos, writeErr := e.da.Write(key, data)
	if writeErr == dataAdapter.ErrPageIsFull {
		log.Panic("can't fit value on free page")
	}
	if writeErr != nil {
		log.Panicf("%v%v", key, data)
	}
	e.index.Insert(key, writePos)
}

func (e *Executor) Delete(key string) bool {
	e.recordLockTable.YieldLock(key)
	defer e.recordLockTable.Unlock(key)
	pos, findErr := e.index.Find(key)
	if findErr == bp_tree.ErrKeyNotFound {
		return false
	}
	if delErr := e.da.DeleteAtPos(key, pos); delErr == dataAdapter.ErrRecordNotFound {
		log.Panic("index and data page mismatch")
	}
	if delErr := e.index.Delete(key); delErr == bp_tree.ErrKeyNotFound {
		log.Panic("index and data page mismatch")
	}
	return true
}
