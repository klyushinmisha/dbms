package concurrency

import (
	"errors"
	"log"
	"sync"
	"time"
)

const lockTimeout = 10 * time.Second

var ErrTxLockTimeout = errors.New("page lock timeout")

type lockTableRecord struct {
	mode       int
	acquirers  int
	updateTxId int
}

// TODO: return cond variables on lock/unlock
type LockTable struct {
	// table related data
	tableMux sync.Mutex
	table    map[interface{}]*lockTableRecord
}

func NewLockTable() *LockTable {
	var t LockTable
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
	for !t.TryLock(key, mode) {
		if time.Now().Sub(start) > lockTimeout {
			panic(ErrTxLockTimeout)
		}
	}
}

func (t *LockTable) UpgradeLock(key interface{}, txId int) {
	start := time.Now()
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
		if time.Now().Sub(start) > lockTimeout {
			panic(ErrTxLockTimeout)
		}
	}
}

func (t *LockTable) DowngradeLock(key interface{}) {
	t.tableMux.Lock()
	defer func() {
		t.tableMux.Unlock()
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
