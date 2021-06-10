package server

import (
	"dbms/internal/config"
	"dbms/internal/core"
	"dbms/internal/parser"
)

type DBMSServerFactory interface {
	ConnSrv() *ConnServer
}

type DefaultDBMSServerFactory struct {
	cfg      *config.ServerConfig
	coreFactory core.DBMSCoreFactory
}

func NewDefaultDBMSServerFactory(
	cfg *config.ServerConfig,
	coreFactory core.DBMSCoreFactory,
) *DefaultDBMSServerFactory {
	c := new(DefaultDBMSServerFactory)
	c.cfg = cfg
	c.coreFactory = coreFactory
	return c
}

func (c *DefaultDBMSServerFactory) ConnSrv() *ConnServer {
	return NewConnServer(
		c.cfg,
		parser.NewDumbSingleLineParser(),
		c.coreFactory.TxMgr(),
	)
}
