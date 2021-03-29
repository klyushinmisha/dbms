package data

import (
	"dbms/pkg/cache/lru_cache"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"dbms/pkg/utils"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
)

func TestDataAdapter_SetGet(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		dataStorage := storage.NewHeapPageStorageBuilder(dataFile, 8192).
			UseLockTable(concurrency.NewLockTable()).
			Build()
		defer dataStorage.Finalize()
		runTests(dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestDataAdapter_CachingSetGet(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		sharedLockTable := concurrency.NewLockTable()
		dataStorage := storage.NewHeapPageStorageBuilder(dataFile, 8192).
			UseLockTable(sharedLockTable).
			UseCache(lru_cache.NewLRUCache(1024, sharedLockTable)).
			Build()
		defer dataStorage.Finalize()
		runTests(dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestDataAdapter_ConcurrentSetGet(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		dataStorage := storage.NewHeapPageStorageBuilder(dataFile, 8192).
			UseLockTable(concurrency.NewLockTable()).
			Build()
		defer dataStorage.Finalize()
		runConcurrentTests(dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestDataAdapter_ConcurrentCachingSetGet(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		sharedLockTable := concurrency.NewLockTable()
		dataStorage := storage.NewHeapPageStorageBuilder(dataFile, 8192).
			UseLockTable(sharedLockTable).
			UseCache(lru_cache.NewLRUCache(1024, sharedLockTable)).
			Build()
		defer dataStorage.Finalize()
		runConcurrentTests(dataStorage)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func runTests(dataStorage *storage.HeapPageStorage) {
	da := NewDataAdapter(dataStorage)
	keys := 128
	for i := 0; i < keys; i++ {
		randK := strconv.Itoa(i)
		pos, err := da.Write(randK, []byte(randK))
		if err != nil {
			log.Panic(err)
		}
		_, err = da.FindAtPos(randK, pos)
		if err != nil {
			log.Panic(err)
		}
	}
}

func runConcurrentTests(dataStorage *storage.HeapPageStorage) {
	executors := 64
	da := NewDataAdapter(dataStorage)
	var wg sync.WaitGroup
	wg.Add(2 * executors)
	for i := 0; i < executors; i++ {
		go func(randK string) {
			pos, err := da.Write(randK, []byte(randK))
			if err != nil {
				log.Panic(err)
			}
			_, err = da.FindAtPos(randK, pos)
			if err != nil {
				log.Panic(err)
			}
			wg.Done()
		}(strconv.Itoa(i))
		go func(randK string) {
			pos, err := da.Write(randK, []byte(randK))
			if err != nil {
				log.Panic(err)
			}
			_, err = da.FindAtPos(randK, pos)
			if err != nil {
				log.Panic(err)
			}
			wg.Done()
		}(strconv.Itoa(i))
	}
	wg.Wait()
}
