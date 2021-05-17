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
	tx := (*transaction.Tx)(nil)
	defer func() {
		if tx != nil {
			tx.Abort()
		}
	}()
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
		var resp string
		if parseErr != nil {
			resp = fmt.Sprintf("%s", parseErr)
		} else {
			res := NewCommandFactory(tx, s.txMgr).Create(cmd).Execute()
			if res.err != nil {
				resp = fmt.Sprintf("%s", res.err)
			} else if res.value != nil {
				resp = string(res.value)
			} else {
				resp = "OK"
			}
		}
		if _, writeErr := writer.Write([]byte(fmt.Sprintf("%s\n", resp))); writeErr != nil {
			log.Panic(writeErr)
		}
		writer.Flush()
	}
}
