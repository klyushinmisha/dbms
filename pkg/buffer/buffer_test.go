package buffer

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"sync"
	"testing"
)

func TestBuffer_Fetch(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		dataStorage := storage.NewHeapPageStorageBuilder(dataFile, 8192).Build()
		defer dataStorage.Finalize()
		tab := concurrency.NewLockTable()
		bufferCap := 32
		buf := NewBuffer(dataStorage, tab, bufferCap)
		keys := bufferCap
		threads := 16
		for i := 0; i < keys; i++ {
			pos := int64(i * 8192)
			page := storage.AllocatePage(8192)
			page.AppendData([]byte{byte(i)})
			dataStorage.WritePageAtPos(page, pos)
		}
		var wg sync.WaitGroup
		wg.Add(threads * keys)
		for i := 0; i < keys; i++ {
			for j := 0; j < threads; j++ {
				go func(k int) {
					pos := int64(k * 8192)
					func() {
						tab.YieldLock(pos, concurrency.ExclusiveMode)
						defer tab.Unlock(pos)
						buf.Fetch(pos)
					}()
					tab.YieldLock(pos, concurrency.SharedMode)
					defer tab.Unlock(pos)
					buf.Pin(pos)
					defer buf.Unpin(pos)
					page := buf.ReadPage(pos)
					assert.Equal(t, page.ReadData(0)[0], byte(k))
					wg.Done()
				}(i)
			}
		}
		wg.Wait()
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestBuffer_FetchFlush(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		dataStorage := storage.NewHeapPageStorageBuilder(dataFile, 8192).Build()
		defer dataStorage.Finalize()
		tab := concurrency.NewLockTable()
		bufferCap := 32
		buf := NewBuffer(dataStorage, tab, bufferCap)
		keys := bufferCap
		threads := 16
		for i := 0; i < keys; i++ {
			pos := int64(i * 8192)
			page := storage.AllocatePage(8192)
			for j := 0; j < threads; j++ {
				page.AppendData([]byte{byte(i)})
			}
			dataStorage.WritePageAtPos(page, pos)
		}
		var wg sync.WaitGroup
		wg.Add(threads * keys)
		for i := 0; i < keys; i++ {
			for j := 0; j < threads; j++ {
				go func(k int) {
					pos := int64(k * 8192)
					tab.YieldLock(pos, concurrency.ExclusiveMode)
					defer tab.Unlock(pos)
					buf.Fetch(pos)
					buf.Pin(pos)
					defer func() {
						buf.Unpin(pos)
						buf.Flush(pos)
						wg.Done()
					}()
					page := buf.ReadPage(pos)
					page.DeleteData(0)
					page.AppendData([]byte{byte(k + 1)})
					buf.WritePage(page, pos)
				}(i)
			}
		}
		wg.Wait()
		wg.Add(threads * keys)
		for i := 0; i < keys; i++ {
			for j := 0; j < threads; j++ {
				go func(k int) {
					pos := int64(k * 8192)
					func() {
						tab.YieldLock(pos, concurrency.ExclusiveMode)
						defer tab.Unlock(pos)
						buf.Fetch(pos)
					}()
					tab.YieldLock(pos, concurrency.SharedMode)
					defer tab.Unlock(pos)
					buf.Pin(pos)
					defer buf.Unpin(pos)
					page := buf.ReadPage(pos)
					assert.Equal(t, page.ReadData(0)[0], byte(k+1))
					wg.Done()
				}(i)
			}
		}
		wg.Wait()
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
