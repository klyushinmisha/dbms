package server

import (
	"dbms/pkg/config"
	"dbms/pkg/core"
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
		NewDumbSingleLineParser(),
		c.coreCfgr.TxMgr(),
	)
}
