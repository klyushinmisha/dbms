package pkg

import (
	"dbms/pkg/cache/lru_cache"
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
)

func createDefaultStorage(file *os.File, c *Config) *storage.HeapPageStorage {
	sharedLockTable := concurrency.NewLockTable()
	return storage.NewHeapPageStorageBuilder(file, c.PageSize).
		UseLockTable(sharedLockTable).
		UseCache(lru_cache.NewLRUCache(c.CacheSize, sharedLockTable)).
		Build()
}

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
			indexStorage := createDefaultStorage(indexFile, config)
			defer indexStorage.Finalize()
			dataStorage := createDefaultStorage(dataFile, config)
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
			indexStorage := createDefaultStorage(indexFile, config)
			defer indexStorage.Finalize()
			dataStorage := createDefaultStorage(dataFile, config)
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

func TestExecutor_ConcurrentSetGet(t *testing.T) {
	config := LoadConfig([]byte(`
	{
		"filesPath": ".",
		"pageSize": 8192,
		"cacheSize": 16
	}
	`))
	threads := 16
	keys := 1024
	execErr := utils.FileScopedExec(config.DataPath(), func(dataFile *os.File) error {
		return utils.FileScopedExec(config.IndexPath(), func(indexFile *os.File) error {
			indexStorage := createDefaultStorage(indexFile, config)
			defer indexStorage.Finalize()
			dataStorage := createDefaultStorage(dataFile, config)
			defer dataStorage.Finalize()
			e := InitExecutor(indexStorage, dataStorage)
			var wg sync.WaitGroup
			wg.Add(keys * threads)
			for i := 0; i < keys; i++ {
				for j := 0; j < threads; j++ {
					go func(randK string) {
						e.Set(randK, []byte(randK))
						_, ok := e.Get(randK)
						if !ok {
							log.Panic("value must present in database")
						}
						wg.Done()
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

// TODO: specify and fix rare deadlock
func TestExecutor_ConcurrentSetDelete(t *testing.T) {
	config := LoadConfig([]byte(`
	{
		"filesPath": ".",
		"pageSize": 8192,
		"cacheSize": 16
	}
	`))
	threads := 16
	keys := 1024
	execErr := utils.FileScopedExec(config.DataPath(), func(dataFile *os.File) error {
		return utils.FileScopedExec(config.IndexPath(), func(indexFile *os.File) error {
			indexStorage := createDefaultStorage(indexFile, config)
			defer indexStorage.Finalize()
			dataStorage := createDefaultStorage(dataFile, config)
			defer dataStorage.Finalize()
			e := InitExecutor(indexStorage, dataStorage)
			for i := 0; i < keys; i++ {
				key := strconv.Itoa(i)
				e.Set(key, []byte(key))
			}
			var wg sync.WaitGroup
			wg.Add(keys * threads)
			for i := 0; i < keys; i++ {
				for j := 0; j < threads; j++ {
					go func(randK string) {
						e.Delete(randK)
						_, found := e.Get(randK)
						if found {
							log.Panic("value must be deleted")
						}
						wg.Done()
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
