package atomic

import (
	"sync/atomic"
)

type AtomicCounter struct {
	counter int64
}

func (c *AtomicCounter) Init(value int) {
	atomic.StoreInt64(&c.counter, int64(value))
}

func (c *AtomicCounter) Incr() int {
	return int(atomic.AddInt64(&c.counter, 1))
}

func (c *AtomicCounter) Decr() int {
	return int(atomic.AddInt64(&c.counter, -1))
}

func (c *AtomicCounter) Value() int {
	return int(c.counter)
}
