package recovery

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/storage"
	"dbms/pkg/transaction"
	"log"
)

type RecoveryManager struct {
	logMgr *logging.LogManager
}

func NewRecoveryManager(logMgr *logging.LogManager) *RecoveryManager {
	m := new(RecoveryManager)
	m.logMgr = logMgr
	return m
}

func (m *RecoveryManager) RollForward(txMgr *transaction.TransactionManager) {
	var idCounter int64
	txs := make(map[int]*transaction.Transaction)
	logsIter := m.logMgr.Iterator()
	for {
		r := logsIter()
		if r == nil {
			break
		}
		if r.TxId() > int(idCounter) {
			idCounter = int64(r.TxId())
		}
		tx, found := txs[r.TxId()]
		if !found {
			tx = txMgr.InitTxWithId(r.TxId(), concurrency.ExclusiveMode)
			txs[r.TxId()] = tx
		}
		switch r.Type() {
		case logging.UpdateRecord:
			page := storage.AllocatePage(len(r.Snapshot))
			if err := page.UnmarshalBinary(r.Snapshot); err != nil {
				log.Panic(err)
			}
			tx.WritePageAtPos(page, r.Pos)
			break
		case logging.CommitRecord:
			tx.CommitNoLog()
			delete(txs, tx.Id())
			break
		case logging.AbortRecord:
			tx.AbortNoLog()
			delete(txs, tx.Id())
			break
		}
	}
	// abort trailing transactions
	for _, tx := range txs {
		tx.Abort()
	}
	txMgr.SetIdCounter(idCounter)
}
