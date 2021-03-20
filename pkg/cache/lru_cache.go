package cache

import (
	"container/list"
	"sync"
)

type lruCacheItem struct {
	key  int64
	item interface{}
}

type LRUCache struct {
	mux   sync.RWMutex
	cap   int
	index map[int64]*list.Element
	cache *list.List
}

func NewLRUCache(cap int) *LRUCache {
	var c LRUCache
	c.cap = cap
	c.index = make(map[int64]*list.Element, cap)
	c.cache = list.New()
	return &c
}

func (c *LRUCache) Put(key int64, item interface{}) (int64, interface{}) {
	var pruneKey = int64(-1)
	var pruneItem interface{}
	c.mux.Lock()
	defer c.mux.Unlock()
	listItem, found := c.index[key]
	if found {
		c.cache.Remove(listItem)
	} else {
		if c.cache.Len() == c.cap {
			pruneKey, pruneItem = c.prune()
		}
	}
	cacheItem := new(lruCacheItem)
	cacheItem.key = key
	cacheItem.item = item
	c.index[key] = c.cache.PushFront(cacheItem)
	return pruneKey, pruneItem
}

func (c *LRUCache) Get(key int64) (interface{}, bool) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	listItem, found := c.index[key]
	if !found {
		return nil, false
	}
	return listItem.Value.(*lruCacheItem).item, true
}

func (c *LRUCache) PruneAll(exec func(int64, interface{})) {
	c.mux.Lock()
	defer c.mux.Unlock()
	for {
		pruneKey, pruneItem := c.prune()
		if pruneKey == -1 {
			return
		}
		exec(pruneKey, pruneItem)
	}
}

func (c *LRUCache) prune() (int64, interface{}) {
	backItem := c.cache.Back()
	if backItem == nil {
		return -1, nil
	}
	cacheItem := backItem.Value.(*lruCacheItem)
	delete(c.index, cacheItem.key)
	c.cache.Remove(backItem)
	return cacheItem.key, cacheItem.item
}
