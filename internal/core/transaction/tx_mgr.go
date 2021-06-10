package transaction

import (
	"dbms/internal/atomic"
	"dbms/internal/core/concurrency"
	"dbms/internal/core/logging"
	"dbms/internal/core/storage"
	"log"
	"sync"
)

type DataCommands interface {
	// props
	NoDataFound() bool
	// methods
	AllocatePage() *storage.HeapPage
	ReadPageAtPos(pos int64) *storage.HeapPage
	WritePageAtPos(page *storage.HeapPage, pos int64)
	WritePage(page *storage.HeapPage) int64
}

type ConcurrencyControlCommands interface {
	DowngradeLocks()
}

type TxCommands interface {
	CommitNoLog()
	Commit()
	Abort()
}

type Tx interface {
	Id() int
	DataCommands
	ConcurrencyControlCommands
	TxCommands
}

type TxManager struct {
	idCtr           atomic.AtomicCounter
	strgMgr         *storage.StorageManager
	bufSlotMgr      *storage.BufferSlotManager
	logMgr          *logging.LogManager
	sharedLockTable *concurrency.LockTable
	a               *storage.HeapPageAllocator
}

func NewTxManager(
	strgMgr *storage.StorageManager,
	bufSlotMgr *storage.BufferSlotManager,
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

func (m *TxManager) InitTx(lockMode int) Tx {
	return m.InitTxWithId(m.idCtr.Incr(), lockMode)
}

func (m *TxManager) InitTxWithId(id int, lockMode int) Tx {
	tx := new(concreteTx)
	tx.id = id
	tx.lockMode = lockMode
	tx.TxManager = m
	return tx
}

const (
	processing = 0
	committed  = 1
	aborted    = 2
)

type concreteTx struct {
	*TxManager
	id       int
	lockMode int
	status   int
	// lockedPages is a set of pages positions
	// TODO: use regular map
	lockedPages sync.Map
}

func (t *concreteTx) Id() int {
	return t.id
}

func (tx *concreteTx) validateTxStatus() {
	if tx.status != processing {
		log.Panic("transaction processing finished")
	}
}

func (tx *concreteTx) fetchAndLockPage(pos int64) {
	if _, found := tx.lockedPages.Load(pos); found {
		tx.sharedLockTable.UpgradeLock(pos, tx.id)
		return
	}
	tx.bufSlotMgr.Fetch(pos)
	tx.bufSlotMgr.Pin(pos)
	tx.sharedLockTable.Lock(pos, tx.lockMode)
	tx.lockedPages.Store(pos, struct{}{})
}

func (tx *concreteTx) DowngradeLocks() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.sharedLockTable.DowngradeLock(pos.(int64))
		return true
	})
}

func (tx *concreteTx) AllocatePage() *storage.HeapPage {
	return tx.a.AllocatePage()
}

func (tx *concreteTx) ReadPageAtPos(pos int64) *storage.HeapPage {
	tx.validateTxStatus()
	tx.fetchAndLockPage(pos)
	return tx.bufSlotMgr.ReadPageAtPos(pos)
}

func (tx *concreteTx) WritePageAtPos(page *storage.HeapPage, pos int64) {
	tx.validateTxStatus()
	tx.fetchAndLockPage(pos)
	tx.sharedLockTable.UpgradeLock(pos, tx.id)
	tx.bufSlotMgr.WritePageAtPos(page, pos)
}

func (tx *concreteTx) WritePage(page *storage.HeapPage) int64 {
	tx.validateTxStatus()
	pos := tx.strgMgr.Extend()
	tx.WritePageAtPos(page, pos)
	return pos
}

func (tx *concreteTx) CommitNoLog() {
	tx.lockedPages.Range(func(ipos, _ interface{}) bool {
		pos := ipos.(int64)
		tx.bufSlotMgr.Flush(pos)
		tx.bufSlotMgr.Unpin(pos)
		tx.sharedLockTable.Unlock(pos)
		return true
	})
	tx.strgMgr.Flush()
	tx.logMgr.Release(tx.Id())
}

func (tx *concreteTx) Commit() {
	tx.lockedPages.Range(func(ipos, _ interface{}) bool {
		pos := ipos.(int64)
		if page := tx.bufSlotMgr.ReadPageIfDirty(pos); page != nil {
			snapshot, err := page.MarshalBinary()
			if err != nil {
				log.Panic(err)
			}
			tx.logMgr.LogSnapshot(tx.id, pos, snapshot)
		}
		return true
	})
	tx.logMgr.LogCommit(tx.id)
	tx.logMgr.Flush()
	tx.CommitNoLog()
	tx.status = committed
}

func (tx *concreteTx) Abort() {
	tx.lockedPages.Range(func(ipos, _ interface{}) bool {
		pos := ipos.(int64)
		tx.bufSlotMgr.Unpin(pos)
		tx.bufSlotMgr.Deallocate(pos)
		tx.sharedLockTable.Unlock(pos)
		return true
	})
	tx.logMgr.Release(tx.Id())
	tx.status = aborted
}

func (tx *concreteTx) NoDataFound() bool {
	return tx.strgMgr.Empty()
}
