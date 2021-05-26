package server

import (
	"dbms/internal/config"
	"dbms/internal/core"
	"dbms/internal/parser"
)

type DBMSServerConfigurator interface {
	ConnSrv() *ConnServer
}

type DefaultDBMSServerConfigurator struct {
	cfg      *config.ServerConfig
	coreCfgr core.DBMSCoreConfigurator
}

func NewDefaultDBMSServerConfigurator(
	cfg *config.ServerConfig,
	coreCfgr core.DBMSCoreConfigurator,
) *DefaultDBMSServerConfigurator {
	c := new(DefaultDBMSServerConfigurator)
	c.cfg = cfg
	c.coreCfgr = coreCfgr
	return c
}

func (c *DefaultDBMSServerConfigurator) ConnSrv() *ConnServer {
	return NewConnServer(
		c.cfg,
		parser.NewDumbSingleLineParser(),
		c.coreCfgr.TxMgr(),
	)
}
