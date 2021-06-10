package server

import (
	"context"
	"golang.org/x/sync/semaphore"
	"net"
)

type ConnLimiter struct {
	ln  net.Listener
	sem *semaphore.Weighted
	ctx context.Context
}

func NewConnLimiter(ln net.Listener, maxConn int) *ConnLimiter {
	l := new(ConnLimiter)
	l.ln = ln
	l.sem = semaphore.NewWeighted(int64(maxConn))
	// no deadline or cancel expected; goroutine must be parked until semaphore can be acquired
	l.ctx = context.Background()
	return l
}

func (l *ConnLimiter) Accept() (net.Conn, error) {
	l.sem.Acquire(l.ctx, 1)
	return l.ln.Accept()
}

func (l *ConnLimiter) Release() {
	l.sem.Release(1)
}
