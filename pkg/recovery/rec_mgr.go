package recovery

import (
	"bytes"
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/storage/buffer"
	"dbms/pkg/transaction"
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"
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
	txs := make(map[int]*Transaction)
	logsIter := m.logMgr.Iterator()
	for r := logsIter(); r != nil; r = logsIter() {
		if r.txId > idCounter {
			idCounter = r.txId
		}
		var tx *Transaction
		if tx, found := txs[r.txId]; !found {
			// use ExclusiveMode here, because transactions not modifing storage
			// won't lock any page;
			// so no deadlocks will appear during RollForward() call
			tx := txMgr.InitTxWithId(r.txId, concurrency.ExclusiveMode)
		}
		switch r.recType {
		case snapshot:
			tx.WritePageAtPos(r.snapshot, r.pos)
			break
		case commit:
			tx.CommitNoLog()
			break
		case abort:
			tx.AbortNoLog()
			break
		}
	}
	// abort trailing transactions
	for _, tx := range txs {
		tx.Abort()
	}
	txMgr.SetIdCounter(idCounter)
}

/*

recMgr := NewRecoveryManager()
recMgr.RollForward(txMgr)
...
tx := txMgr.InitTx(...)
tx.ReadPageAtPos(pos)
tx.WritePageAtPos(page, pos)
tx.Commit()

*/
