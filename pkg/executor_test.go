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
	bufferCap := 8192
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
			keys := 1024
			threads := 4
			var wg sync.WaitGroup
			wg.Add(threads)
			for j := 0; j < threads; j++ {
				go func() {
					tx := txMgr.InitTx(concurrency.SharedMode)
					defer func() {
						defer wg.Done()
						if err := recover(); err == concurrency.ErrTxLockTimeout {
							tx.Abort()
						}
					}()
					e := NewExecutor(tx)
					for i := 0; i < keys; i++ {
						k := strconv.Itoa(i)
						e.Set(k, []byte(k))
						_, ok := e.Get(k)
						if !ok {
							log.Panic("value must present in database")
						}
					}
					tx.Commit()
				}()
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
			keys := 1024
			threads := 4
			totalTxs := threads
			deadTxs := int32(0)
			var wg sync.WaitGroup
			wg.Add(threads)
			for j := 0; j < threads; j++ {
				go func() {
					tx := txMgr.InitTx(concurrency.SharedMode)
					defer func() {
						defer wg.Done()
						if err := recover(); err == concurrency.ErrTxLockTimeout {
							tx.Abort()
							atomic.AddInt32(&deadTxs, 1)
						}
					}()
					e := NewExecutor(tx)
					for i := 0; i < keys; i++ {
						e.Set(strconv.Itoa(i), []byte(strconv.Itoa(i)))
					}
					tx.Commit()
				}()
			}
			wg.Wait()
			log.Printf("Transactions:\n\ttotal: %v\n\tdead:  %v", totalTxs, deadTxs)
			wg.Add(threads)
			deadTxs = 0
			for j := 0; j < threads; j++ {
				go func() {
					tx := txMgr.InitTx(concurrency.SharedMode)
					defer func() {
						defer wg.Done()
						if err := recover(); err == concurrency.ErrTxLockTimeout {
							tx.Abort()
							atomic.AddInt32(&deadTxs, 1)
						}
					}()
					e := NewExecutor(tx)
					for i := 0; i < keys; i++ {
						k := strconv.Itoa(i)
						e.Delete(k)
						_, ok := e.Get(k)
						if ok {
							log.Panic("value must not present in database")
						}
					}
					tx.Commit()
				}()
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

func TestExecutor_ConcurrentSetTxServer(t *testing.T) {
	execErr := utils.FileScopedExec("data.bin", func(dataFile *os.File) error {
		return utils.FileScopedExec("log.bin", func(logFile *os.File) error {
			txMgr := createDefaultTxMgr(dataFile, logFile)
			func() {
				tx := txMgr.InitTx(concurrency.ExclusiveMode)
				defer tx.Commit()
				e := NewExecutor(tx)
				e.Init()
			}()
			keys := 1024
			threads := 4
			txSrv := NewTxServer(txMgr)
			var wg sync.WaitGroup
			wg.Add(threads)
			for j := 0; j < threads; j++ {
				go func() {
					defer wg.Done()
					desc := txSrv.Init()
					defer txSrv.Terminate(desc)
					cmd := Cmd{
						BegShCmd,
						"",
						nil,
					}
					txSrv.ExecuteCmd(desc, cmd)
					for i := 0; i < keys; i++ {
						cmd = Cmd{
							SetCmd,
							strconv.Itoa(i),
							[]byte(strconv.Itoa(i)),
						}
						txSrv.ExecuteCmd(desc, cmd)
						cmd = Cmd{
							GetCmd,
							strconv.Itoa(i),
							nil,
						}
						txSrv.ExecuteCmd(desc, cmd)
					}
				}()
			}
			wg.Wait()
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
