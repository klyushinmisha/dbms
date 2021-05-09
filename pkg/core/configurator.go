package core

import (
	"dbms/pkg/config"
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/logging"
	"dbms/pkg/core/recovery"
	"dbms/pkg/core/storage"
	"dbms/pkg/core/storage/buffer"
	"dbms/pkg/core/transaction"
	"os"
)

type DBMSCoreConfigurator interface {
	TxMgr() *transaction.TxManager
	LogMgr() *logging.LogManager
	RecMgr() *recovery.RecoveryManager
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