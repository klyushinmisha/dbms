package recovery

import (
	"dbms/internal/core/concurrency"
	"dbms/internal/core/logging"
	"dbms/internal/core/storage"
	"dbms/internal/core/transaction"
	"dbms/internal/utils"
	"log"
	"os"
	"testing"
)

func TestRecoveryManager_LogRecovery(t *testing.T) {
	execErr := utils.FileScopedExec("data.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			pageSize := 128
			logMgr := logging.NewLogManager(logFile, pageSize)
			keys := 32
			page := storage.AllocatePage(pageSize)
			recMgr := NewRecoveryManager(logMgr)
			buf := storage.NewBufferSlotManager(
				storage.NewStorageManager(dataFile, storage.NewHeapPageAllocator(pageSize)),
				128,
				pageSize,
			)
			txMgr := transaction.NewTxManager(
				0,
				buf,
				logMgr,
				concurrency.NewLockTable(),
			)
			for i := 0; i < keys; i++ {
				tx := txMgr.InitTx(concurrency.ExclusiveMode)
				tx.WritePage(page)
				tx.Commit()
			}
			recMgr.RollForward(txMgr)
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
