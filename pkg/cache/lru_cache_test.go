package cache

import (
	"log"
	"testing"
)

func TestLRUCache_PutPrune(t *testing.T) {
	capacity := 2
	cache := NewLRUCache(capacity)
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
	cache := NewLRUCache(capacity)
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
	cache := NewLRUCache(capacity)
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
		if int(key) != i {
			log.Panic("Invalid key")
		}
		if *item.(*int) != i {
			log.Panic("Invalid item")
		}
		i++
	})
}
