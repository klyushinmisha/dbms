package recovery

import (
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/logging"
	"dbms/pkg/core/transaction"
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

func (m *RecoveryManager) RollForward(txMgr *transaction.TxManager) {
	var maxTxId int
	txs := make(map[int]*transaction.Tx)
	logsIter := m.logMgr.Iterator()
	for {
		r := logsIter()
		if r == nil {
			break
		}
		if r.TxId() > maxTxId {
			maxTxId = r.TxId()
		}
		tx, found := txs[r.TxId()]
		if !found {
			tx = txMgr.InitTxWithId(r.TxId(), concurrency.ExclusiveMode)
			txs[r.TxId()] = tx
		}
		switch r.Type() {
		case logging.UpdateRecord:
			page := tx.AllocatePage()
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
	txMgr.SetIdCounter(maxTxId)
}
