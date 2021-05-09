package recovery

import (
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/logging"
	"dbms/pkg/core/storage"
	"dbms/pkg/core/storage/buffer"
	"dbms/pkg/core/transaction"
	"dbms/pkg/utils"
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
			buf := buffer.NewBufferSlotManager(
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
