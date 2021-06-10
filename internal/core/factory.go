package core

import (
	"dbms/internal/config"
	"dbms/internal/core/concurrency"
	"dbms/internal/core/logging"
	"dbms/internal/core/recovery"
	"dbms/internal/core/storage"
	"dbms/internal/core/transaction"
	"os"
)

type DBMSCoreFactory interface {
	TxMgr() *transaction.TxManager
	SegMgr() *logging.SegmentManager
	LogMgr() *logging.LogManager
	RecMgr() *recovery.RecoveryManager
	BtstpMgr() *BootstrapManager
}

// dataFile must be unique per configuration to prevent multiple access to same files
type DefaultDBMSCoreFactory struct {
	cfg        *config.CoreConfig
	dataFile   *os.File
	strgMgr    *storage.StorageManager
	bufSlotMgr *storage.BufferSlotManager
	txMgr      *transaction.TxManager
	segMgr     *logging.SegmentManager
	logMgr     *logging.LogManager
	btstpMgr   *BootstrapManager
}

func NewDefaultDBMSCoreFactory(cfg *config.CoreConfig) *DefaultDBMSCoreFactory {
	c := new(DefaultDBMSCoreFactory)
	c.cfg = cfg
	c.logMgr = nil
	return c
}

func (c *DefaultDBMSCoreFactory) TxMgr() *transaction.TxManager {
	// singleton
	if c.txMgr == nil {
		c.strgMgr = storage.NewStorageManager(c.BtstpMgr().StrgFile(), storage.NewHeapPageAllocator(c.cfg.PageSize))
		c.bufSlotMgr = storage.NewBufferSlotManager(
			c.strgMgr,
			c.cfg.BufCap,
			c.cfg.PageSize,
		)
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

func (c *DefaultDBMSCoreFactory) SegMgr() *logging.SegmentManager {
	// singleton
	if c.segMgr == nil {
		c.segMgr = logging.NewSegmentManager(c.cfg.LogPath(), c.cfg.LogSegCap)
	}
	return c.segMgr
}

func (c *DefaultDBMSCoreFactory) LogMgr() *logging.LogManager {
	// singleton
	if c.logMgr == nil {
		c.logMgr = logging.NewLogManager(c.SegMgr())
	}
	return c.logMgr
}

func (c *DefaultDBMSCoreFactory) RecMgr() *recovery.RecoveryManager {
	return recovery.NewRecoveryManager(c.LogMgr())
}

func (c *DefaultDBMSCoreFactory) BtstpMgr() *BootstrapManager {
	// singleton
	if c.btstpMgr == nil {
		c.btstpMgr = NewBootstrapManager(c.cfg, c)
	}
	return c.btstpMgr
}
