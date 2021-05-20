package server

import (
	"dbms/pkg/config"
	"dbms/pkg/core/transaction"
	"dbms/pkg/transfer"
	"errors"
	"fmt"
	"log"
	"net"
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
	DumbRawCmdStreamProcessorFromConn(conn).Pipe(func(rawCmd string) *transfer.Result {
		if cmd, err := s.parser.Parse(rawCmd); err != nil {
			return transfer.ErrResult(err)
		} else {
			return cmdFact.Create(*cmd)()
		}
	})
}
