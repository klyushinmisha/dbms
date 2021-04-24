package pkg

import (
	"dbms/pkg/access/bp_tree"
	bpAdapter "dbms/pkg/storage/adapters/bp_tree"
	dataAdapter "dbms/pkg/storage/adapters/data"
	"dbms/pkg/transaction"
	"log"
)

type Executor struct {
	tx    *transaction.Transaction
	index *bp_tree.BPTree
	da    *dataAdapter.DataAdapter
}

func NewExecutor(tx *transaction.Transaction) *Executor {
	e := new(Executor)
	e.tx = tx
	e.index = bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(tx))
	e.da = dataAdapter.NewDataAdapter(tx)
	return e
}

func (e *Executor) Init() {
	e.index.Init()
}

func (e *Executor) Get(key string) ([]byte, bool) {
	defer e.tx.DowngradeLocks()
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
	defer e.tx.DowngradeLocks()
	pos, findErr := e.index.Find(key)
	if findErr == nil {
		writeErr := e.da.WriteAtPos(key, data, pos)
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
	defer e.tx.DowngradeLocks()
	pos, findErr := e.index.Find(key)
	if findErr == bp_tree.ErrKeyNotFound {
		return false
	}
	if e.da.DeleteAtPos(key, pos) == dataAdapter.ErrRecordNotFound {
		log.Panic("index and data page mismatch")
	}
	if e.index.Delete(key) == bp_tree.ErrKeyNotFound {
		log.Panic("index and data page mismatch")
	}
	return true
}
