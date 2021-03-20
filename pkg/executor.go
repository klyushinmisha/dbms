package pkg

import (
	"dbms/pkg/access"
	"dbms/pkg/access/bp_tree"
	"dbms/pkg/storage"
	bpAdapter "dbms/pkg/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/storage/adapters/data"
	"log"
)

type Executor struct {
	index access.Index
	da    *dataAdapter.DataAdapter
}

func InitExecutor(
	indexStorage *storage.HeapPageStorage,
	dataStorage *storage.HeapPageStorage,
) *Executor {
	var e Executor
	e.da = dataAdapter.NewDataAdapter(dataStorage)
	e.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(indexStorage))
	e.index.Init()
	return &e
}

func (e *Executor) Get(key string) ([]byte, bool) {
	pos, findErr := e.index.Find(key)
	if findErr == bp_tree.ErrKeyNotFound {
		return nil, false
	}
	data, findErr := e.da.FindAtPos(key, pos)
	if findErr != nil {
		log.Panic("index and data page mismatch")
	}
	return data, true
}

func (e *Executor) Set(key string, data []byte) {
	pos, findErr := e.index.Find(key)
	if findErr != bp_tree.ErrKeyNotFound {
		if delErr := e.da.DeleteAtPos(key, pos); delErr != dataAdapter.ErrRecordNotFound {
			log.Panic("index and data page mismatch")
		}
		writeErr := e.da.WriteAtPos(key, data, pos)
		if writeErr == nil {
			return
		}
	}
	// TODO: instead of allocation use free space map
	writePos, writeErr := e.da.Write(key, data)
	if writeErr != nil {
		log.Panic("can't fit value on free page")
	}
	e.index.Insert(key, writePos)
}

func (e *Executor) Delete(key string) bool {
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
