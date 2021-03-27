package lru_cache

import (
	"dbms/pkg/concurrency"
	"log"
	"sync"
	"testing"
)

func TestLRUCache_PutPrune(t *testing.T) {
	capacity := 2
	cache := NewLRUCache(capacity, nil)
	for i := 0; i < capacity; i++ {
		item := new(int)
		*item = i
		prunedKey, _ := cache.Put(int64(i), item)
		if prunedKey != -1 {
			log.Panic("No prune expected")
		}
	}
	item := new(int)
	*item = -1
	prunedKey, _ := cache.Put(3, item)
	if prunedKey != 0 {
		log.Panic("Invalid key was pruned")
	}
}

func TestLRUCache_PutGet(t *testing.T) {
	capacity := 2
	cache := NewLRUCache(capacity, nil)
	for i := 0; i < capacity; i++ {
		item := new(int)
		*item = i
		prunedKey, _ := cache.Put(int64(i), item)
		if prunedKey != -1 {
			log.Panic("No prune expected")
		}
	}
	for i := 0; i < capacity; i++ {
		value, found := cache.Get(int64(i))
		if !found {
			log.Panic("key not found")
		}
		if *value.(*int) != i {
			log.Panic("invalid")
		}
	}
	_, found := cache.Get(int64(capacity))
	if found {
		log.Panic("found value for not inserted key")
	}
}

func TestLRUCache_PutPruneAll(t *testing.T) {
	capacity := 2
	cache := NewLRUCache(capacity, nil)
	for i := 0; i < capacity; i++ {
		item := new(int)
		*item = i
		prunedKey, _ := cache.Put(int64(i), item)
		if prunedKey != -1 {
			log.Panic("No prune expected")
		}
	}
	i := 0
	cache.PruneAll(func(key int64, item interface{}) {
		if key == -1 {
			return
		}
		if int(key) != i {
			log.Panic("Invalid key")
		}
		if *item.(*int) != i {
			log.Panic("Invalid item")
		}
		i++
	})
}

func TestLRUCache_ConcurrentPutGet(t *testing.T) {
	lockTable := concurrency.NewLockTable()
	capacity := 1024
	cache := NewLRUCache(capacity, nil)
	var wg sync.WaitGroup
	wg.Add(2 * capacity)
	for i := 0; i < capacity; i++ {
		// run multiple goroutines to emulate real cache use case in with concurrent access
		go func(key int64) {
			lockTable.YieldLock(key)
			defer lockTable.Unlock(key)
			item := new(int)
			*item = int(key)
			prunedKey, _ := cache.Put(key, item)
			if prunedKey != -1 {
				log.Panic("No prune expected")
			}
			value, found := cache.Get(key)
			if !found {
				log.Panic("key not found")
			}
			if *value.(*int) != int(key) {
				log.Panic("invalid")
			}
			wg.Done()
		}(int64(i))
		go func(key int64) {
			lockTable.YieldLock(key)
			defer lockTable.Unlock(key)
			item := new(int)
			*item = int(key)
			prunedKey, _ := cache.Put(key, item)
			if prunedKey != -1 {
				log.Panic("No prune expected")
			}
			value, found := cache.Get(key)
			if !found {
				log.Panic("key not found")
			}
			if *value.(*int) != int(key) {
				log.Panic("invalid")
			}
			wg.Done()
		}(int64(i))
	}
	wg.Wait()
	_, found := cache.Get(int64(capacity))
	if found {
		log.Panic("found value for not inserted key")
	}
}
