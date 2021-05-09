package server

import (
	"dbms/pkg/atomic"
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/storage"
	"dbms/pkg/storage/buffer"
	"dbms/pkg/transaction"
	"dbms/pkg/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
)

const Page8K = 8192

func createDefaultTxMgr(dataFile *os.File, logFile *os.File) *transaction.TxManager {
	bufferCap := 8192
	buf := buffer.NewBufferSlotManager(
		storage.NewStorageManager(dataFile, Page8K),
		bufferCap,
		Page8K,
	)
	return transaction.NewTxManager(
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
			var deadTxsCtr atomic.AtomicCounter
			var wg sync.WaitGroup
			wg.Add(threads)
			for j := 0; j < threads; j++ {
				go func() {
					tx := txMgr.InitTx(concurrency.SharedMode)
					defer func() {
						defer wg.Done()
						if err := recover(); err == concurrency.ErrTxLockTimeout {
							tx.Abort()
							deadTxsCtr.Incr()
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
			log.Printf("Transactions:\n\ttotal: %v\n\tdead:  %v", totalTxs, deadTxsCtr.Value())
			wg.Add(threads)
			deadTxsCtr.Init(0)
			for j := 0; j < threads; j++ {
				go func() {
					tx := txMgr.InitTx(concurrency.SharedMode)
					defer func() {
						defer wg.Done()
						if err := recover(); err == concurrency.ErrTxLockTimeout {
							tx.Abort()
							deadTxsCtr.Incr()
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
			log.Printf("Transactions:\n\ttotal: %v\n\tdead:  %v", totalTxs, deadTxsCtr.Value())
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
			p := NewDumbSingleLineParser()
			var wg sync.WaitGroup
			wg.Add(threads)
			for j := 0; j < threads; j++ {
				go func() {
					defer wg.Done()
					desc := txSrv.Init()
					defer txSrv.Terminate(desc)
					cmd, _ := p.Parse("BEGIN EXCLUSIVE")
					txSrv.ExecuteCmd(desc, cmd)
					for i := 0; i < keys; i++ {
						cmd, _ = p.Parse(fmt.Sprintf("SET %d %d", i, i))
						txSrv.ExecuteCmd(desc, cmd)
						cmd, _ = p.Parse(fmt.Sprintf("GET %d", i))
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

func TestExecutor_Parser(t *testing.T) {
	p := NewDumbSingleLineParser()
	log.Print(p.Parse("GET hello"))
	log.Print(p.Parse("SET hello world"))
	log.Print(p.Parse("DEL hello"))
	log.Print(p.Parse("BEGIN SHARED"))
	log.Print(p.Parse("BEGIN EXCLUSIVE"))
	log.Print(p.Parse("COMMIT"))
	log.Print(p.Parse("ABORT"))
}
