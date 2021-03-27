package concurrency

import (
	"runtime"
	"sync"
)

type LockTable struct {
	mux   sync.Mutex
	table map[int64]bool
}

func NewLockTable() *LockTable {
	var t LockTable
	t.table = make(map[int64]bool)
	return &t
}

func (t *LockTable) tryLock(key int64) bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	locked, found := t.table[key]
	if found && locked {
		return false
	}
	t.table[key] = true
	return true
}

func (t *LockTable) YieldLock(key int64) {
	for !t.tryLock(key) {
		// allow other goroutines to work if can't lock page
		// TODO: maybe use conditional variable (set after Unlock; check and reset if set here)
		runtime.Gosched()
	}
}

func (t *LockTable) Unlock(key int64) {
	t.mux.Lock()
	defer t.mux.Unlock()
	delete(t.table, key)
}
