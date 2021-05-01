package transaction

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/storage"
	"dbms/pkg/storage/buffer"
	"log"
	"sync"
	"sync/atomic"
)

type Transaction struct {
	id       int
	lockMode int
	// lockedPages is a set of pages positions
	lockedPages sync.Map
	txMgr       *TransactionManager
}

type TransactionManager struct {
	idCounter       int64
	bufSlotMgr      *buffer.BufferSlotManager
	logMgr          *logging.LogManager
	sharedLockTable *concurrency.LockTable
}

func NewTransactionManager(
	lastTxId int64,
	bufSlotMgr *buffer.BufferSlotManager,
	logMgr *logging.LogManager,
	sharedLockTable *concurrency.LockTable,
) *TransactionManager {
	txMgr := new(TransactionManager)
	txMgr.bufSlotMgr = bufSlotMgr
	txMgr.logMgr = logMgr
	txMgr.sharedLockTable = sharedLockTable
	return txMgr
}

func (m *TransactionManager) SetIdCounter(idCounter int64) {
	m.idCounter = idCounter
}

func (m *TransactionManager) InitTx(lockMode int) *Transaction {
	return m.InitTxWithId(int(atomic.AddInt64(&m.idCounter, 1)), lockMode)
}

func (m *TransactionManager) InitTxWithId(id int, lockMode int) *Transaction {
	tx := new(Transaction)
	tx.id = id
	tx.lockMode = lockMode
	tx.txMgr = m
	return tx
}

func (tx *Transaction) fetchAndLockPage(pos int64) {
	if _, found := tx.lockedPages.Load(pos); found {
		tx.txMgr.sharedLockTable.UpgradeLock(pos, tx.id)
		return
	}
	tx.txMgr.bufSlotMgr.Fetch(pos)
	tx.txMgr.bufSlotMgr.Pin(pos)
	tx.txMgr.sharedLockTable.YieldLock(pos, tx.lockMode)
	tx.txMgr.sharedLockTable.UpgradeLock(pos, tx.id)
	tx.lockedPages.Store(pos, struct{}{})
}

func (tx *Transaction) DowngradeLocks() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.txMgr.sharedLockTable.DowngradeLock(pos.(int64))
		return true
	})
}

func (tx *Transaction) ReadPageAtPos(pos int64) *storage.HeapPage {
	tx.fetchAndLockPage(pos)
	return tx.txMgr.bufSlotMgr.ReadPageAtPos(pos)
}

/*

Exclusive (takes locks until commit/abort):
BEGIN
...lock...
GET key
SET key value
...unlock...
COMMIT/ABORT

Shared (takes locks on operations but excludes Exclusive transactions):
BEGIN
...lock...
GET key
...unlock...
...lock...
SET key value
...unlock...
COMMIT/ABORT

В расшаренном режиме апгрейдить блокировки на время выполнения операции.
Когда операция завершена, понижать блокировку

*/

func (tx *Transaction) WritePageAtPos(page *storage.HeapPage, pos int64) {
	tx.fetchAndLockPage(pos)
	tx.txMgr.bufSlotMgr.WritePageAtPos(page, pos)
}

func (tx *Transaction) WritePage(page *storage.HeapPage) int64 {
	pos := tx.txMgr.bufSlotMgr.StorageManager().Extend()
	tx.WritePageAtPos(page, pos)
	return pos
}

func (tx *Transaction) CommitNoLog() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.txMgr.bufSlotMgr.Flush(pos.(int64))
		tx.txMgr.bufSlotMgr.Unpin(pos.(int64))
		tx.txMgr.sharedLockTable.Unlock(pos.(int64))
		return true
	})
	tx.txMgr.bufSlotMgr.StorageManager().Flush()
}

func (tx *Transaction) AbortNoLog() {
	tx.lockedPages.Range(func(pos, _ interface{}) bool {
		tx.txMgr.bufSlotMgr.Flush(pos.(int64))
		tx.txMgr.bufSlotMgr.Deallocate(pos.(int64))
		tx.txMgr.sharedLockTable.Unlock(pos.(int64))
		return true
	})
}

func (tx *Transaction) Commit() {
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
}

func (tx *Transaction) Abort() {
	tx.txMgr.logMgr.LogAbort(tx.id)
	tx.txMgr.logMgr.Flush()
	tx.AbortNoLog()
}

func (tx *Transaction) StorageManager() *storage.StorageManager {
	return tx.txMgr.bufSlotMgr.StorageManager()
}