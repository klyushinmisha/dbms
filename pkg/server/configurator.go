package server

import (
	"dbms/pkg/config"
	"dbms/pkg/core"
)

type DBMSServerConfigurator interface {
	TxSrv() *TxServer
	ConnSrv() *ConnServer
}

type DefaultDBMSServerConfigurator struct {
	cfg      *config.ServerConfig
	coreCfgr core.DBMSCoreConfigurator
	txSrv    *TxServer
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

func (c *DefaultDBMSServerConfigurator) TxSrv() *TxServer {
	// singleton
	if c.txSrv == nil {
		c.txSrv = NewTxServer(c.coreCfgr.TxMgr())
	}
	return c.txSrv
}

func (c *DefaultDBMSServerConfigurator) ConnSrv() *ConnServer {
	return NewConnServer(
		c.cfg,
		NewDumbSingleLineParser(),
		c.TxSrv(),
	)
}
