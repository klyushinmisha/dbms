package server

import (
	"bufio"
	"context"
	"dbms/pkg/config"
	"dbms/pkg/core/transaction"
	"dbms/pkg/transfer"
	"errors"
	"fmt"
	"golang.org/x/sync/semaphore"
	"io"
	"log"
	"net"
	"strings"
)

// TxProxy handles tx lifecycle (init and finalization)
type TxProxy struct {
	txMgr *transaction.TxManager
	tx    *transaction.Tx
}

func NewTxProxy(txMgr *transaction.TxManager) *TxProxy {
	p := new(TxProxy)
	p.txMgr = txMgr
	return p
}

func (p *TxProxy) Tx() *transaction.Tx {
	return p.tx
}

var (
	ErrTxStarted = errors.New("tx is already started")
)

func (p *TxProxy) Init(mode int) error {
	if p.tx != nil {
		return ErrTxStarted
	}
	p.tx = p.txMgr.InitTx(mode)
	return nil
}

func (p *TxProxy) Commit() {
	if p.tx != nil {
		p.tx.Commit()
		p.tx = nil
	}
}

func (p *TxProxy) Abort() {
	if p.tx != nil {
		p.tx.Abort()
		p.tx = nil
	}
}

type ConnLimiter struct {
	ln  net.Listener
	sem *semaphore.Weighted
	ctx context.Context
}

func NewConnLimiter(ln net.Listener, maxConn int) *ConnLimiter {
	l := new(ConnLimiter)
	l.ln = ln
	l.sem = semaphore.NewWeighted(int64(maxConn))
	l.ctx = context.TODO()
	return l
}

func (l *ConnLimiter) Accept() (net.Conn, error) {
	l.sem.Acquire(l.ctx, 1)
	return l.ln.Accept()
}

func (l *ConnLimiter) Release() {
	l.sem.Release(1)
}

type ConnServer struct {
	cfg    *config.ServerConfig
	parser Parser
	txMgr  *transaction.TxManager
}

func NewConnServer(cfg *config.ServerConfig, parser Parser, txMgr *transaction.TxManager) *ConnServer {
	s := new(ConnServer)
	s.cfg = cfg
	s.parser = parser
	s.txMgr = txMgr
	return s
}

func (s *ConnServer) Run() {
	ln, err := net.Listen(s.cfg.TransportProtocol, fmt.Sprintf(":%d", s.cfg.Port))
	defer ln.Close()
	if err != nil {
		log.Panic(err)
	}
	lim := NewConnLimiter(ln, s.cfg.MaxConnections)
	log.Printf("Server is up on port %d", s.cfg.Port)
	for {
		conn, err := lim.Accept()
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Accepted connection with host %s", conn.RemoteAddr())
		go func() {
			defer func() {
				log.Printf("Release connection with host %s", conn.RemoteAddr())
				lim.Release()
			}()
			s.serve(conn)
		}()
	}
}

func (s *ConnServer) serve(conn net.Conn) {
	txProxy := NewTxProxy(s.txMgr)
	defer txProxy.Abort()
	cmdFact := NewCommandFactory(txProxy)
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		strCmd, err := reader.ReadString('\n')
		if err == io.EOF {
			return
		} else if err != nil {
			log.Panic(err)
		}
		cmd, parseErr := s.parser.Parse(strings.TrimSpace(strCmd))
		var res *transfer.Result
		if parseErr != nil {
			res = transfer.ErrResult(parseErr)
		} else {
			res = cmdFact.Create(cmd)()
		}
		resp, marshalErr := res.MarshalBinary()
		if marshalErr != nil {
			log.Panic(marshalErr)
		}
		if _, writeErr := writer.Write(resp); writeErr != nil {
			log.Panic(writeErr)
		}
		writer.Flush()
	}
}
