package pkg

import (
	"dbms/pkg/cache/lru_cache"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestExecutor_GetSet(t *testing.T) {
	config := LoadConfig([]byte(`
	{
		"filesPath": ".",
		"pageSize": 8192,
		"cacheSize": 16
	}
	`))
	execErr := utils.FileScopedExec(config.DataPath(), func(dataFile *os.File) error {
		return utils.FileScopedExec(config.IndexPath(), func(indexFile *os.File) error {
			sharedIndexLockTable := concurrency.NewLockTable()
			sharedDataLockTable := concurrency.NewLockTable()
			indexCache := lru_cache.NewLRUCache(config.CacheSize, sharedIndexLockTable)
			indexStorage := storage.NewHeapPageStorage(indexFile, config.PageSize, indexCache, sharedIndexLockTable, nil)
			defer indexStorage.Finalize()
			dataCache := lru_cache.NewLRUCache(config.CacheSize, sharedDataLockTable)
			dataStorage := storage.NewHeapPageStorage(dataFile, config.PageSize, dataCache, sharedDataLockTable, nil)
			defer dataStorage.Finalize()
			e := InitExecutor(indexStorage, dataStorage)
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
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}

func TestExecutor_SetDelete(t *testing.T) {
	config := LoadConfig([]byte(`
	{
		"filesPath": ".",
		"pageSize": 8192,
		"cacheSize": 16
	}
	`))
	execErr := utils.FileScopedExec(config.DataPath(), func(dataFile *os.File) error {
		return utils.FileScopedExec(config.IndexPath(), func(indexFile *os.File) error {
			sharedIndexLockTable := concurrency.NewLockTable()
			sharedDataLockTable := concurrency.NewLockTable()
			indexCache := lru_cache.NewLRUCache(config.CacheSize, sharedIndexLockTable)
			indexStorage := storage.NewHeapPageStorage(indexFile, config.PageSize, indexCache, sharedIndexLockTable, nil)
			defer indexStorage.Finalize()
			dataCache := lru_cache.NewLRUCache(config.CacheSize, sharedDataLockTable)
			dataStorage := storage.NewHeapPageStorage(dataFile, config.PageSize, dataCache, sharedDataLockTable, nil)
			defer dataStorage.Finalize()
			e := InitExecutor(indexStorage, dataStorage)
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
			return nil
		})
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
