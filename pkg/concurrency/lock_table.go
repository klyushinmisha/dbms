package concurrency

import (
	"sync"
)

type LockTable struct {
	// table related data
	tableMux sync.Mutex
	table    map[interface{}]bool
	// row lock ability related data
	rowLockCondMux sync.Mutex
	rowLockCond    *sync.Cond
}

func NewLockTable() *LockTable {
	var t LockTable
	t.rowLockCond = sync.NewCond(&t.rowLockCondMux)
	t.table = make(map[interface{}]bool)
	return &t
}

func (t *LockTable) TryLock(key interface{}) bool {
	t.tableMux.Lock()
	defer t.tableMux.Unlock()
	locked, found := t.table[key]
	if found && locked {
		return false
	}
	t.table[key] = true
	return true
}

func (t *LockTable) YieldLock(key interface{}) {
	for !t.TryLock(key) {
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
	delete(t.table, key)
}
