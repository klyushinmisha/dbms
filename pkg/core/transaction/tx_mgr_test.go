package transaction

import (
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/logging"
	"dbms/pkg/core/storage"
	"dbms/pkg/core/storage/buffer"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"sync"
	"testing"
)

const Page8K = 8192

func TestTxManager_InitUpdateCommit(t *testing.T) {
	execErr := utils.FileScopedExec("database.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txCount := 32
			threads := 16
			bufferCap := 32
			buf := buffer.NewBufferSlotManager(
				storage.NewStorageManager(dataFile, storage.NewHeapPageAllocator(Page8K)),
				bufferCap,
				Page8K,
			)
			txMgr := NewTxManager(
				0,
				buf,
				logging.NewLogManager(logFile, Page8K),
				concurrency.NewLockTable(),
			)
			var wg sync.WaitGroup
			wg.Add(txCount)
			for i := 0; i < txCount; i++ {
				func(pos int64) {
					tx := txMgr.InitTx(concurrency.ExclusiveMode)
					page := storage.AllocatePage(Page8K)
					page.AppendData([]byte{byte(pos + 1)})
					tx.WritePage(page)
					tx.Commit()
					wg.Done()
				}(int64(i * Page8K))
			}
			wg.Wait()
			wg.Add(txCount * threads)
			for i := 0; i < txCount; i++ {
				for j := 0; j < threads; j++ {
					go func(pos int64) {
						tx := txMgr.InitTx(concurrency.ExclusiveMode)
						page := tx.ReadPageAtPos(pos)
						page.DeleteData(0)
						page.AppendData([]byte{byte(pos + 1)})
						tx.WritePageAtPos(page, pos)
						tx.Commit()
						wg.Done()
					}(int64(i * Page8K))
				}
			}
			wg.Wait()
			wg.Add(txCount * threads)
			for i := 0; i < txCount; i++ {
				for j := 0; j < threads; j++ {
					go func(pos int64) {
						tx := txMgr.InitTx(concurrency.SharedMode)
						page := tx.ReadPageAtPos(pos)
						tx.Commit()
						assert.Equal(t, page.ReadData(0)[0], byte(pos+1))
						wg.Done()
					}(int64(i * Page8K))
				}
			}
			wg.Wait()
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestTxManager_InitUpdateAbort(t *testing.T) {
	execErr := utils.FileScopedExec("database.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txCount := 32
			threads := 16
			bufferCap := 32
			buf := buffer.NewBufferSlotManager(
				storage.NewStorageManager(dataFile),
				bufferCap,
				Page8K,
			)
			txMgr := NewTxManager(
				0,
				buf,
				logging.NewLogManager(logFile, Page8K),
				concurrency.NewLockTable(),
			)
			var wg sync.WaitGroup
			wg.Add(txCount)
			for i := 0; i < txCount; i++ {
				func(pos int64) {
					tx := txMgr.InitTx(concurrency.ExclusiveMode)
					page := storage.AllocatePage(Page8K)
					page.AppendData([]byte{byte(pos)})
					tx.WritePage(page)
					tx.Commit()
					wg.Done()
				}(int64(i * Page8K))
			}
			wg.Wait()
			wg.Add(txCount * threads)
			for i := 0; i < txCount; i++ {
				for j := 0; j < threads; j++ {
					go func(pos int64) {
						tx := txMgr.InitTx(concurrency.ExclusiveMode)
						page := tx.ReadPageAtPos(pos)
						page.DeleteData(0)
						page.AppendData([]byte{byte(pos + 1)})
						tx.WritePageAtPos(page, pos)
						tx.Abort()
						wg.Done()
					}(int64(i * Page8K))
				}
			}
			wg.Wait()
			wg.Add(txCount * threads)
			for i := 0; i < txCount; i++ {
				for j := 0; j < threads; j++ {
					go func(pos int64) {
						tx := txMgr.InitTx(concurrency.SharedMode)
						page := tx.ReadPageAtPos(pos)
						tx.Commit()
						assert.Equal(t, page.ReadData(0)[0], byte(pos))
						wg.Done()
					}(int64(i * Page8K))
				}
			}
			wg.Wait()
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
