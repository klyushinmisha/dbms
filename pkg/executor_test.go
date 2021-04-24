package pkg

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/storage"
	"dbms/pkg/storage/buffer"
	"dbms/pkg/transaction"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

const Page8K = 8192

func createDefaultTxMgr(dataFile *os.File, logFile *os.File) *transaction.TransactionManager {
	bufferCap := 1024
	buf := buffer.NewBufferSlotManager(
		storage.NewStorageManager(dataFile, Page8K),
		bufferCap,
		Page8K,
	)
	return transaction.NewTransactionManager(
		0,
		buf,
		logging.NewLogManager(logFile, Page8K),
		concurrency.NewLockTable(),
	)
}

func TestExecutor_GetSet(t *testing.T) {
	execErr := utils.FileScopedExec("data.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txMgr := createDefaultTxMgr(dataFile, logFile)
			func() {
				tx := txMgr.InitTx(concurrency.ExclusiveMode)
				defer tx.Commit()
				e := NewExecutor(tx)
				e.Init()
				e.Set("HELLO", []byte("WORLD"))
				data, ok := e.Get("HELLO")
				if !ok {
					log.Panic("value must present in database")
				}
				assert.Equal(t, string(data), "WORLD")
				e.Set("ANOTHER ONE", []byte("ANOTHER WORLD"))
				data, ok = e.Get("HELLO")
				if !ok {
					log.Panic("value must present in database")
				}
				assert.Equal(t, string(data), "WORLD")
				data, ok = e.Get("ANOTHER ONE")
				if !ok {
					log.Panic("value must present in database")
				}
				assert.Equal(t, string(data), "ANOTHER WORLD")
			}()
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestExecutor_SetDelete(t *testing.T) {
	execErr := utils.FileScopedExec("data.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txMgr := createDefaultTxMgr(dataFile, logFile)
			func() {
				tx := txMgr.InitTx(concurrency.ExclusiveMode)
				defer tx.Commit()
				e := NewExecutor(tx)
				e.Init()
				e.Set("HELLO", []byte("WORLD"))
				e.Set("ANOTHER ONE", []byte("ANOTHER WORLD"))
				e.Delete("ANOTHER ONE")
				data, found := e.Get("HELLO")
				if !found {
					log.Panic("value must present in database")
				}
				assert.Equal(t, string(data), "WORLD")
				data, found = e.Get("ANOTHER ONE")
				if found {
					log.Panic("value must be deleted")
				}
			}()
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestExecutor_ConcurrentSetGet(t *testing.T) {
	execErr := utils.FileScopedExec("data.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txMgr := createDefaultTxMgr(dataFile, logFile)
			func() {
				tx := txMgr.InitTx(concurrency.ExclusiveMode)
				defer tx.Commit()
				e := NewExecutor(tx)
				e.Init()
			}()
			keys := 16
			threads := 16
			var wg sync.WaitGroup
			wg.Add(keys * threads)
			for i := 0; i < keys; i++ {
				for j := 0; j < threads; j++ {
					go func(randK string) {
						tx := txMgr.InitTx(concurrency.SharedMode)
						defer func() {
							if err := recover(); err == concurrency.ErrTxLockTimeout {
								tx.Abort()
							}
							defer wg.Done()
						}()
						e := NewExecutor(tx)
						e.Set(randK, []byte(randK))
						_, ok := e.Get(randK)
						if !ok {
							log.Panic("value must present in database")
						}
						tx.Commit()
					}(strconv.Itoa(i))
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

func TestExecutor_ConcurrentSetDelete(t *testing.T) {
	execErr := utils.FileScopedExec("data.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txMgr := createDefaultTxMgr(dataFile, logFile)
			func() {
				tx := txMgr.InitTx(concurrency.ExclusiveMode)
				defer tx.Commit()
				e := NewExecutor(tx)
				e.Init()
			}()
			keys := 16
			threads := 16
			totalTxs := keys * threads
			deadTxs := int32(0)
			var wg sync.WaitGroup
			wg.Add(keys * threads)
			for i := 0; i < keys; i++ {
				for j := 0; j < threads; j++ {
					go func(randK string) {
						tx := txMgr.InitTx(concurrency.SharedMode)
						defer func() {
							if err := recover(); err == concurrency.ErrTxLockTimeout {
								tx.Abort()
								atomic.AddInt32(&deadTxs, 1)
							}
							defer wg.Done()
						}()
						e := NewExecutor(tx)
						e.Set(randK, []byte(randK))
						tx.Commit()
					}(strconv.Itoa(i))
				}
			}
			wg.Wait()
			log.Printf("Transactions:\n\ttotal: %v\n\tdead:  %v", totalTxs, deadTxs)
			wg.Add(keys * threads)
			deadTxs = 0
			for i := 0; i < keys; i++ {
				for j := 0; j < threads; j++ {
					go func(randK string) {
						tx := txMgr.InitTx(concurrency.SharedMode)
						defer func() {
							if err := recover(); err == concurrency.ErrTxLockTimeout {
								tx.Abort()
								atomic.AddInt32(&deadTxs, 1)
							}
							defer wg.Done()
						}()
						e := NewExecutor(tx)
						e.Delete(randK)
						_, found := e.Get(randK)
						if found {
							log.Panic("value must be deleted")
						}
						tx.Commit()
					}(strconv.Itoa(i))
				}
			}
			wg.Wait()
			log.Printf("Transactions:\n\ttotal: %v\n\tdead:  %v", totalTxs, deadTxs)
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
