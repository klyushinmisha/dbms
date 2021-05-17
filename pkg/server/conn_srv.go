package server

import (
	"bufio"
	"context"
	"dbms/pkg/config"
	"dbms/pkg/core/transaction"
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
	log.Printf("Server is up on port %d", s.cfg.Port)
	sem := semaphore.NewWeighted(int64(s.cfg.MaxConnections))
	ctx := context.TODO()
	for {
		// acquire weighted semaphore to reduce concurrency
		sem.Acquire(ctx, 1)
		conn, err := ln.Accept()
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Accepted connection with host %s", conn.RemoteAddr())
		go func() {
			defer func() {
				log.Printf("Release connection with host %s", conn.RemoteAddr())
				sem.Release(1)
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
		var res *Result
		if parseErr != nil {
			res = new(Result)
			res.err = parseErr
		} else {
			res = cmdFact.Create(cmd).Execute()
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
