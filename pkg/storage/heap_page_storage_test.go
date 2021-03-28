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

func TestHeapPageStorage_IO(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		dataStorage := NewHeapPageStorageBuilder(dataFile, pageSize).Build()
		defer dataStorage.Finalize()
		runTestsForStorage(t, dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestHeapPageStorage_CachingIO(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		dataCache := lru_cache.NewLRUCache(64, nil)
		dataStorage := NewHeapPageStorageBuilder(dataFile, pageSize).
			UseCache(dataCache).
			Build()
		defer dataStorage.Finalize()
		runTestsForStorage(t, dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestHeapPageStorage_ConcurrentIO(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		sharedDataLockTable := concurrency.NewLockTable()
		dataStorage := NewHeapPageStorageBuilder(dataFile, pageSize).
			UseLockTable(sharedDataLockTable).
			Build()
		defer dataStorage.Finalize()
		runConcurrentTestsForStorage(t, dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestHeapPageStorage_CachingConcurrentIO(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		sharedDataLockTable := concurrency.NewLockTable()
		dataCache := lru_cache.NewLRUCache(64, sharedDataLockTable)
		dataStorage := NewHeapPageStorageBuilder(dataFile, pageSize).
			UseLockTable(sharedDataLockTable).
			UseCache(dataCache).
			Build()
		defer dataStorage.Finalize()
		runConcurrentTestsForStorage(t, dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func runTestsForStorage(t *testing.T, storage *HeapPageStorage) {
	page := AllocatePage(pageSize)
	pos := storage.WritePage(page)
	assert.Equal(t, page, storage.ReadPageAtPos(pos))
}

func runConcurrentTestsForStorage(t *testing.T, storage *HeapPageStorage) {
	executors := 32
	var wg sync.WaitGroup
	wg.Add(executors)
	for i := 0; i < executors; i++ {
		go func(randomStuff int) {
			page := AllocatePage(pageSize)
			page.Data[0] = byte(randomStuff)
			page.freeSpace = 0
			pos := storage.WritePage(page)
			assert.Equal(t, page, storage.ReadPageAtPos(pos))
			wg.Done()
		}(i)
	}
	wg.Wait()
	wg.Add(2 * executors)
	for i := 0; i < executors; i++ {
		go func(randomStuff int) {
			pos := int64(randomStuff * pageSize)
			page := storage.ReadPageAtPos(pos)
			page.Data[0] = byte(randomStuff)
			page.freeSpace = 0
			storage.WritePageAtPos(page, pos)
			assert.Equal(t, page, storage.ReadPageAtPos(pos))
			wg.Done()
		}(i)
		go func(randomStuff int) {
			pos := int64(randomStuff * pageSize)
			page := storage.ReadPageAtPos(pos)
			page.Data[0] = byte(randomStuff)
			page.freeSpace = 0
			storage.WritePageAtPos(page, pos)
			assert.Equal(t, page, storage.ReadPageAtPos(pos))
			wg.Done()
		}(i)
	}
	wg.Wait()
}
