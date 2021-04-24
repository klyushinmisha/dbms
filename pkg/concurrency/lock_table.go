package concurrency

import (
	"errors"
	"log"
	"sync"
	"time"
)

const lockTimeout = 1 * time.Second

var ErrTxLockTimeout = errors.New("page lock timeout")

type lockTableRecord struct {
	mode       int
	acquirers  int
	updateTxId int
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
		newRec := new(lockTableRecord)
		newRec.mode = mode
		newRec.acquirers = 1
		t.table[key] = newRec
	}
	return true
}

func (t *LockTable) YieldLock(key interface{}, mode int) {
	start := time.Now()
	t.rowLockCondMux.Lock()
	defer t.rowLockCondMux.Unlock()
	for !t.TryLock(key, mode) {
		// allow other goroutines to work if can't lock row
		t.rowLockCond.Wait()
		if time.Now().Sub(start) > lockTimeout {
			panic(ErrTxLockTimeout)
		}
	}
}

func (t *LockTable) UpgradeLock(key interface{}, txId int) {
	start := time.Now()
	t.rowLockCondMux.Lock()
	defer t.rowLockCondMux.Unlock()
	for {
		mustRet := func() bool {
			t.tableMux.Lock()
			defer t.tableMux.Unlock()
			rec, found := t.table[key]
			if !found {
				log.Panicf("%v key not found", key)
			}
			if rec.mode == SharedMode {
				rec.mode = UpdateMode
				rec.updateTxId = txId
			} else if rec.mode == UpdateMode {
				return rec.updateTxId == txId
			}
			return true
		}()
		if mustRet {
			return
		}
		t.rowLockCond.Wait()
		if time.Now().Sub(start) > lockTimeout {
			panic(ErrTxLockTimeout)
		}
	}
}

func (t *LockTable) DowngradeLock(key interface{}) {
	t.tableMux.Lock()
	defer func() {
		t.tableMux.Unlock()
		t.rowLockCond.Broadcast()
	}()
	rec, found := t.table[key]
	if !found {
		log.Panicf("%v key not found", key)
	}
	if rec.mode == UpdateMode {
		rec.mode = SharedMode
	}
}

func (t *LockTable) Unlock(key interface{}) {
	t.tableMux.Lock()
	defer func() {
		t.tableMux.Unlock()
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
