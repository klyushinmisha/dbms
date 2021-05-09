package transaction

import (
	"dbms/pkg/atomic"
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/logging"
	"dbms/pkg/core/storage"
	"dbms/pkg/core/storage/buffer"
	"log"
	"sync"
)

const (
	processing = 0
	committed  = 1
	aborted    = 2
)

// Tx is a single-thread ACID transaction
type Tx struct {
	id       int
	lockMode int
	status   int
	// lockedPages is a set of pages positions
	// TODO: use regular map
	lockedPages sync.Map
	txMgr       *TxManager
}

func (t *Tx) Id() int {
	return t.id
}

type TxManager struct {
	idCtr           atomic.AtomicCounter
	bufSlotMgr      *buffer.BufferSlotManager
	logMgr          *logging.LogManager
	sharedLockTable *concurrency.LockTable
}

func NewTxManager(
	lastTxId int64,
	bufSlotMgr *buffer.BufferSlotManager,
	logMgr *logging.LogManager,
	sharedLockTable *concurrency.LockTable,
) *TxManager {
	txMgr := new(TxManager)
	txMgr.bufSlotMgr = bufSlotMgr
	txMgr.logMgr = logMgr
	txMgr.sharedLockTable = sharedLockTable
	return txMgr
}

func (m *TxManager) SetIdCounter(idCounter int) {
	m.idCtr.Init(idCounter)
}

func (m *TxManager) InitTx(lockMode int) *Tx {
	return m.InitTxWithId(m.idCtr.Incr(), lockMode)
}

func (m *TxManager) InitTxWithId(id int, lockMode int) *Tx {
	tx := new(Tx)
	tx.id = id
	tx.lockMode = lockMode
	tx.txMgr = m
	return tx
}

func (tx *Tx) validateTxStatus() {
	if tx.status != processing {
		log.Panic("transaction processing finished")
	}
}

func (tx *Tx) fetchAndLockPage(pos int64) {
	if _, found := tx.lockedPages.Load(pos); found {
		tx.txMgr.sharedLockTable.UpgradeLock(pos, tx.id)
		return
	}
	tx.txMgr.bufSlotMgr.Fetch(pos)
	tx.txMgr.bufSlotMgr.Pin(pos)
	tx.txMgr.sharedLockTable.Lock(pos, tx.lockMode)
	tx.txMgr.sharedLockTable.UpgradeLock(pos, tx.id)
	tx.lockedPages.Store(pos, struct{}{})
}

func (tx *Tx) DowngradeLocks() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.txMgr.sharedLockTable.DowngradeLock(pos.(int64))
		return true
	})
}

func (tx *Tx) ReadPageAtPos(pos int64) *storage.HeapPage {
	tx.validateTxStatus()
	tx.fetchAndLockPage(pos)
	return tx.txMgr.bufSlotMgr.ReadPageAtPos(pos)
}

func (tx *Tx) WritePageAtPos(page *storage.HeapPage, pos int64) {
	tx.validateTxStatus()
	tx.fetchAndLockPage(pos)
	tx.txMgr.bufSlotMgr.WritePageAtPos(page, pos)
}

func (tx *Tx) WritePage(page *storage.HeapPage) int64 {
	tx.validateTxStatus()
	pos := tx.txMgr.bufSlotMgr.StorageManager().Extend()
	tx.WritePageAtPos(page, pos)
	return pos
}

func (tx *Tx) CommitNoLog() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.txMgr.bufSlotMgr.Flush(pos.(int64))
		tx.txMgr.bufSlotMgr.Unpin(pos.(int64))
		tx.txMgr.sharedLockTable.Unlock(pos.(int64))
		return true
	})
	tx.txMgr.bufSlotMgr.StorageManager().Flush()
}

func (tx *Tx) AbortNoLog() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.txMgr.bufSlotMgr.Flush(pos.(int64))
		tx.txMgr.bufSlotMgr.Deallocate(pos.(int64))
		tx.txMgr.sharedLockTable.Unlock(pos.(int64))
		return true
	})
}

func (tx *Tx) Commit() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		if page := tx.txMgr.bufSlotMgr.ReadPageIfDirty(pos.(int64)); page != nil {
			snapshot, err := page.MarshalBinary()
			if err != nil {
				log.Panic(err)
			}
			tx.txMgr.logMgr.LogSnapshot(tx.id, pos.(int64), snapshot)
		}
		return true
	})
	tx.txMgr.logMgr.LogCommit(tx.id)
	tx.txMgr.logMgr.Flush()
	tx.CommitNoLog()
	tx.status = committed
}

func (tx *Tx) Abort() {
	tx.txMgr.logMgr.LogAbort(tx.id)
	tx.txMgr.logMgr.Flush()
	tx.AbortNoLog()
	tx.status = aborted
}

func (tx *Tx) StorageManager() *storage.StorageManager {
	return tx.txMgr.bufSlotMgr.StorageManager()
}
