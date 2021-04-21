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

type Lock struct {
	mux        sync.Mutex
	mode       int
	refcount   int
	updateCond *sync.Cond
}

func NewLock() *Lock {
	var l Lock
	l.updateCond = sync.NewCond(&l.mux)
	return &l
}

func (l *Lock) TryLock(mode int) bool {
	l.mux.Lock()
	defer l.mux.Unlock()
	if l.refcount == 0 {
		l.mode = mode
		l.refcount = 1
	} else {
		if !locksCompatMatrix[l.mode][mode] {
			return false
		}
		l.refcount++
	}
	return true
}

func (l *Lock) YieldLock(mode int) {
	for !l.TryLock(mode) {
		// allow other goroutines to work if can't lock row
		l.mux.Lock()
		l.updateCond.Wait()
		l.mux.Unlock()
	}
}

func (l *Lock) Unlock() {
	l.mux.Lock()
	defer func() {
		l.updateCond.Broadcast()
		l.mux.Unlock()
	}()
	if l.refcount == 0 {
		log.Panicf("Trying unlock unlocked lock")
	}
	l.refcount--
}
