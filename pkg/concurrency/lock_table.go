package concurrency

import (
	"log"
	"sync"
)

const (
	SharedMode    = 0
	ExclusiveMode = 1
)

var locksCompatMatrix = [][]bool{
	{true, false},
	{false, false},
}

type lockTableRecord struct {
	mode      int
	acquirers int
}

type LockTable struct {
	// table related data
	tableMux sync.Mutex
	table    map[interface{}]*lockTableRecord
	// row lock ability related data
	rowLockCondMux sync.Mutex
	rowLockCond    *sync.Cond
}

func NewLockTable() *LockTable {
	var t LockTable
	t.rowLockCond = sync.NewCond(&t.rowLockCondMux)
	t.table = make(map[interface{}]*lockTableRecord)
	return &t
}

func (t *LockTable) TryLock(key interface{}, mode int) bool {
	t.tableMux.Lock()
	defer t.tableMux.Unlock()
	rec, found := t.table[key]
	if found {
		if !locksCompatMatrix[rec.mode][mode] {
			return false
		}
		rec.acquirers++
	} else {
		var newRec lockTableRecord
		newRec.mode = mode
		newRec.acquirers = 1
		t.table[key] = &newRec
	}
	return true
}

func (t *LockTable) YieldLock(key interface{}, mode int) {
	for !t.TryLock(key, mode) {
		// allow other goroutines to work if can't lock row
		func() {
			t.rowLockCondMux.Lock()
			defer t.rowLockCondMux.Unlock()
			t.rowLockCond.Wait()
		}()
	}
}

func (t *LockTable) Unlock(key interface{}) {
	t.tableMux.Lock()
	defer func() {
		t.tableMux.Unlock()
		t.rowLockCondMux.Lock()
		defer t.rowLockCondMux.Unlock()
		t.rowLockCond.Broadcast()
	}()
	rec, found := t.table[key]
	if found {
		rec.acquirers--
	} else {
		log.Panicf("Trying unlock unlocked key %v", key)
	}
	if rec.acquirers == 0 {
		delete(t.table, key)
	}
}
