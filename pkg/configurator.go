package pkg

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/config"
	"dbms/pkg/logging"
	"dbms/pkg/recovery"
	"dbms/pkg/storage"
	"dbms/pkg/storage/buffer"
	"dbms/pkg/transaction"
	"os"
)

type DBMSCoreConfigurator interface {
	TxMgr() *transaction.TxManager
	LogMgr() *logging.LogManager
	RecMgr() *recovery.RecoveryManager
}

type DBMSServerConfigurator interface {
	TxSrv() *TxServer
	ConnSrv() *ConnServer
}

// dataFile and logFile must be unique per configuration to prevent multiple access to same files
type DefaultDBMSCoreConfigurator struct {
	cfg        *config.CoreConfig
	dataFile   *os.File
	logFile    *os.File
	bufSlotMgr *buffer.BufferSlotManager
	txMgr      *transaction.TxManager
	logMgr     *logging.LogManager
}

func NewDefaultDBMSCoreConfigurator(cfg *config.CoreConfig, dataFile *os.File, logFile *os.File) *DefaultDBMSCoreConfigurator {
	c := new(DefaultDBMSCoreConfigurator)
	c.cfg = cfg
	c.dataFile = dataFile
	c.logFile = logFile
	c.bufSlotMgr = buffer.NewBufferSlotManager(
		storage.NewStorageManager(dataFile, cfg.PageSize),
		cfg.BufCap,
		cfg.PageSize,
	)
	c.logMgr = nil
	return c
}

func (c *DefaultDBMSCoreConfigurator) TxMgr() *transaction.TxManager {
	// singleton
	if c.txMgr == nil {
		c.txMgr = transaction.NewTxManager(
			0,
			c.bufSlotMgr,
			c.LogMgr(),
			concurrency.NewLockTable(),
		)
	}
	return c.txMgr
}

func (c *DefaultDBMSCoreConfigurator) LogMgr() *logging.LogManager {
	// singleton
	if c.logMgr == nil {
		c.logMgr = logging.NewLogManager(c.logFile, c.cfg.PageSize)
	}
	return c.logMgr
}

func (c *DefaultDBMSCoreConfigurator) RecMgr() *recovery.RecoveryManager {
	return recovery.NewRecoveryManager(c.LogMgr())
}

type DefaultDBMSServerConfigurator struct {
	cfg      *config.ServerConfig
	coreCfgr DBMSCoreConfigurator
	txSrv    *TxServer
}

func NewDefaultDBMSServerConfigurator(cfg *config.ServerConfig, coreCfgr DBMSCoreConfigurator) *DefaultDBMSServerConfigurator {
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
