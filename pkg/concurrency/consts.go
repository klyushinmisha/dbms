package concurrency

import (
	"errors"
	"time"
)

const (
	SharedMode    = 0
	ExclusiveMode = 1
	UpdateMode    = 2
)

var locksCompatMatrix = [][]bool{
	{true, false, true},
	{false, false, false},
	{true, false, false},
}

const lockTimeout = 10 * time.Second

var ErrTxLockTimeout = errors.New("page lock timeout")
