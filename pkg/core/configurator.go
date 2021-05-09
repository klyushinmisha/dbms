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

// dataFile must be unique per configuration to prevent multiple access to same files
type DefaultDBMSCoreConfigurator struct {
	cfg        *config.CoreConfig
	dataFile   *os.File
	strgMgr    *storage.StorageManager
	bufSlotMgr *buffer.BufferSlotManager
	txMgr      *transaction.TxManager
	segMgr     *logging.SegmentManager
	logMgr     *logging.LogManager
}

func NewDefaultDBMSCoreConfigurator(cfg *config.CoreConfig, dataFile *os.File) *DefaultDBMSCoreConfigurator {
	c := new(DefaultDBMSCoreConfigurator)
	c.cfg = cfg
	c.dataFile = dataFile
	c.strgMgr = storage.NewStorageManager(dataFile, storage.NewHeapPageAllocator(c.cfg.PageSize))
	c.bufSlotMgr = buffer.NewBufferSlotManager(
		c.strgMgr,
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
			c.strgMgr,
			c.bufSlotMgr,
			c.LogMgr(),
			concurrency.NewLockTable(),
			storage.NewHeapPageAllocator(c.cfg.PageSize),
		)
	}
	return c.txMgr
}

func (c *DefaultDBMSCoreConfigurator) SegMgr() *logging.SegmentManager {
	// singleton
	if c.segMgr == nil {
		c.segMgr = logging.NewSegmentManager(c.cfg.LogPath(), c.cfg.LogSegCap)
	}
	return c.segMgr
}

func (c *DefaultDBMSCoreConfigurator) LogMgr() *logging.LogManager {
	// singleton
	if c.logMgr == nil {
		c.logMgr = logging.NewLogManager(c.SegMgr())
	}
	return c.logMgr
}

func (c *DefaultDBMSCoreConfigurator) RecMgr() *recovery.RecoveryManager {
	return recovery.NewRecoveryManager(c.LogMgr())
}
