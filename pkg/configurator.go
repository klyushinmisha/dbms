package pkg

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/recovery"
	"dbms/pkg/storage"
	"dbms/pkg/storage/buffer"
	"dbms/pkg/transaction"
	"os"
)

type DBMSCoreConfigurator interface {
	TxMgr() *transaction.TransactionManager
	LogMgr() *logging.LogManager
	RecMgr() *recovery.RecoveryManager
}

type DBMSServerConfigurator interface {
	TxSrv() *TxServer
	ConnSrv() *ConnServer
}

// dataFile and logFile must be unique per configuration to prevent multiple access to same files
type DefaultDBMSCoreConfigurator struct {
	cfg        *CoreConfig
	dataFile   *os.File
	logFile    *os.File
	bufSlotMgr *buffer.BufferSlotManager
	txMgr      *transaction.TransactionManager
	logMgr     *logging.LogManager
}

func NewDefaultDBMSCoreConfigurator(cfg *CoreConfig, dataFile *os.File, logFile *os.File) *DefaultDBMSCoreConfigurator {
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

func (c *DefaultDBMSCoreConfigurator) TxMgr() *transaction.TransactionManager {
	// singleton
	if c.txMgr == nil {
		c.txMgr = transaction.NewTransactionManager(
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
	coreCfgr DBMSCoreConfigurator
	txSrv    *TxServer
}

func NewDefaultDBMSServerConfigurator(coreCfgr DBMSCoreConfigurator) *DefaultDBMSServerConfigurator {
	c := new(DefaultDBMSServerConfigurator)
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
		NewDumbSingleLineParser(),
		c.TxSrv(),
	)
}
