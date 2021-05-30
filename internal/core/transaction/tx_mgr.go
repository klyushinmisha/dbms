package transaction

import (
	"dbms/internal/atomic"
	"dbms/internal/core/concurrency"
	"dbms/internal/core/logging"
	"dbms/internal/core/storage"
	"dbms/internal/core/storage/buffer"
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
	*TxManager
	id       int
	lockMode int
	status   int
	// lockedPages is a set of pages positions
	// TODO: use regular map
	lockedPages sync.Map
}

func (t *Tx) Id() int {
	return t.id
}

type TxManager struct {
	idCtr           atomic.AtomicCounter
	strgMgr         *storage.StorageManager
	bufSlotMgr      *buffer.BufferSlotManager
	logMgr          *logging.LogManager
	sharedLockTable *concurrency.LockTable
	a               *storage.HeapPageAllocator
}

func NewTxManager(
	strgMgr *storage.StorageManager,
	bufSlotMgr *buffer.BufferSlotManager,
	logMgr *logging.LogManager,
	sharedLockTable *concurrency.LockTable,
	a *storage.HeapPageAllocator,
) *TxManager {
	txMgr := new(TxManager)
	txMgr.strgMgr = strgMgr
	txMgr.bufSlotMgr = bufSlotMgr
	txMgr.logMgr = logMgr
	txMgr.sharedLockTable = sharedLockTable
	txMgr.a = a
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
	tx.TxManager = m
	return tx
}

func (tx *Tx) validateTxStatus() {
	if tx.status != processing {
		log.Panic("transaction processing finished")
	}
}

func (tx *Tx) fetchAndLockPage(pos int64) {
	if _, found := tx.lockedPages.Load(pos); found {
		tx.sharedLockTable.UpgradeLock(pos, tx.id)
		return
	}
	tx.bufSlotMgr.Fetch(pos)
	tx.bufSlotMgr.Pin(pos)
	tx.sharedLockTable.Lock(pos, tx.lockMode)
	tx.lockedPages.Store(pos, struct{}{})
}

func (tx *Tx) DowngradeLocks() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.sharedLockTable.DowngradeLock(pos.(int64))
		return true
	})
}

func (tx *Tx) AllocatePage() *storage.HeapPage {
	return tx.a.AllocatePage()
}

func (tx *Tx) ReadPageAtPos(pos int64) *storage.HeapPage {
	tx.validateTxStatus()
	tx.fetchAndLockPage(pos)
	return tx.bufSlotMgr.ReadPageAtPos(pos)
}

func (tx *Tx) WritePageAtPos(page *storage.HeapPage, pos int64) {
	tx.validateTxStatus()
	tx.fetchAndLockPage(pos)
	tx.sharedLockTable.UpgradeLock(pos, tx.id)
	tx.bufSlotMgr.WritePageAtPos(page, pos)
}

func (tx *Tx) WritePage(page *storage.HeapPage) int64 {
	tx.validateTxStatus()
	pos := tx.strgMgr.Extend()
	tx.WritePageAtPos(page, pos)
	return pos
}

func (tx *Tx) CommitNoLog() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.bufSlotMgr.Flush(pos.(int64))
		tx.bufSlotMgr.Unpin(pos.(int64))
		tx.sharedLockTable.Unlock(pos.(int64))
		return true
	})
	tx.strgMgr.Flush()
	tx.logMgr.Release(tx.Id())
}

func (tx *Tx) Commit() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		if page := tx.bufSlotMgr.ReadPageIfDirty(pos.(int64)); page != nil {
			snapshot, err := page.MarshalBinary()
			if err != nil {
				log.Panic(err)
			}
			tx.logMgr.LogSnapshot(tx.id, pos.(int64), snapshot)
		}
		return true
	})
	tx.logMgr.LogCommit(tx.id)
	tx.logMgr.Flush()
	tx.CommitNoLog()
	tx.status = committed
}

func (tx *Tx) Abort() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.bufSlotMgr.Unpin(pos.(int64))
		tx.bufSlotMgr.Deallocate(pos.(int64))
		tx.sharedLockTable.Unlock(pos.(int64))
		return true
	})
	tx.logMgr.Release(tx.Id())
	tx.status = aborted
}

func (tx *Tx) NoDataFound() bool {
	return tx.strgMgr.Empty()
}
