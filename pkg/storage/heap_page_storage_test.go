package storage

import (
	"dbms/pkg/cache/lru_cache"
	"dbms/pkg/concurrency"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"sync"
	"testing"
)

const pageSize = 8192

func TestHeapPageStorage_WriteReadPage(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		sharedDataLockTable := concurrency.NewLockTable()
		dataCache := lru_cache.NewLRUCache(64, sharedDataLockTable)
		dataStorage := NewHeapPageStorageBuilder(dataFile, pageSize).
			UseLockTable(sharedDataLockTable).
			UseCache(dataCache).
			Build()
		defer dataStorage.Finalize()
		page := AllocatePage(pageSize)
		pos := dataStorage.WritePage(page)
		assert.Equal(t, page, dataStorage.ReadPageAtPos(pos))
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestHeapPageStorage_ConcurrentWriteReadPage(t *testing.T) {
	executors := 32
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		sharedDataLockTable := concurrency.NewLockTable()
		dataCache := lru_cache.NewLRUCache(64, sharedDataLockTable)
		dataStorage := NewHeapPageStorageBuilder(dataFile, pageSize).
			UseLockTable(sharedDataLockTable).
			UseCache(dataCache).
			Build()
		defer dataStorage.Finalize()
		var wg sync.WaitGroup
		wg.Add(executors)
		for i := 0; i < executors; i++ {
			go func(randomStuff int) {
				page := AllocatePage(pageSize)
				page.Data[0] = byte(randomStuff)
				page.freeSpace = 0
				pos := dataStorage.WritePage(page)
				assert.Equal(t, page, dataStorage.ReadPageAtPos(pos))
				wg.Done()
			}(i)
		}
		wg.Wait()
		wg.Add(2 * executors)
		for i := 0; i < executors; i++ {
			go func(randomStuff int) {
				pos := int64(randomStuff * pageSize)
				page := dataStorage.ReadPageAtPos(pos)
				page.Data[0] = byte(randomStuff)
				page.freeSpace = 0
				dataStorage.WritePageAtPos(page, pos)
				assert.Equal(t, page, dataStorage.ReadPageAtPos(pos))
				wg.Done()
			}(i)
			go func(randomStuff int) {
				pos := int64(randomStuff * pageSize)
				page := dataStorage.ReadPageAtPos(pos)
				page.Data[0] = byte(randomStuff)
				page.freeSpace = 0
				dataStorage.WritePageAtPos(page, pos)
				assert.Equal(t, page, dataStorage.ReadPageAtPos(pos))
				wg.Done()
			}(i)
		}
		wg.Wait()
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
