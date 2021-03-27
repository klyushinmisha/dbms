package lru_cache

import (
	"container/list"
	"dbms/pkg/concurrency"
	"sync"
)

type lruCacheItem struct {
	key  int64
	item interface{}
}

// TODO: verify if locks required
// seems cache will be used only with some storage driver with locking in it
// so no need to use extra locks for cache itself, only lock pruning pages
type LRUCache struct {
	cap             int
	sharedLockTable *concurrency.LockTable
	index           sync.Map
	cacheMux        sync.RWMutex
	cache           *list.List
}

func NewLRUCache(cap int, sharedLockTable *concurrency.LockTable) *LRUCache {
	var c LRUCache
	c.cap = cap
	c.sharedLockTable = sharedLockTable
	c.cache = list.New()
	return &c
}

// key is expected to be already locked
func (c *LRUCache) Put(key int64, item interface{}) (int64, interface{}) {
	cacheItem := new(lruCacheItem)
	cacheItem.key = key
	cacheItem.item = item
	listItem, found := c.index.Load(key)
	if found {
		// cache item elevation case
		func() {
			c.cacheMux.Lock()
			defer c.cacheMux.Unlock()
			c.cache.Remove(listItem.(*list.Element))
			c.index.Store(key, c.cache.PushFront(cacheItem))
		}()
		return -1, nil
	} else {
		// cache item prune and insert case
		pruneKey := int64(-1)
		pruneItem := interface{}(nil)
		for {
			mustContinue := func() bool {
				// deadlock: client locks pos, calls Put, but locked here and underlying code waits for page unlock
				c.cacheMux.Lock()
				defer c.cacheMux.Unlock()
				if c.cache.Len() == c.cap {
					pruneKey = c.pruneCandidate()
					if pruneKey == -1 {
						return false
					}
					// this condition checks self locking to escape deadlock
					if pruneKey != key && c.sharedLockTable != nil {
						// lock key before prune to prevent from race conditions;
						// must be unlock by cache's client
						// TODO: may be unsafe and lead to infinite page locks
						if !c.sharedLockTable.TryLock(pruneKey) {
							return true
						}
					}
					pruneKey, pruneItem = c.prune()
				}
				c.index.Store(key, c.cache.PushFront(cacheItem))
				return false
			}()
			if !mustContinue {
				break
			}
		}
		return pruneKey, pruneItem
	}
}

// key is expected to be already locked
func (c *LRUCache) Get(key int64) (interface{}, bool) {
	listItem, found := c.index.Load(key)
	if !found {
		return nil, false
	}
	return listItem.(*list.Element).Value.(*lruCacheItem).item, true
}

// PruneAll is utility method for pages pruning
// NOTE: cache methods calls in exec method will lead to deadlock (cache locks and calls exec; exec calls cache)!!!
// TODO: remove the method further
func (c *LRUCache) PruneAll(exec func(int64, interface{})) {
	for {
		pruneKey := int64(-1)
		pruneItem := interface{}(nil)
		func() {
			c.cacheMux.Lock()
			defer c.cacheMux.Unlock()
			pruneKey = c.pruneCandidate()
			if pruneKey == -1 {
				return
			}
			if c.sharedLockTable != nil {
				// lock key before prune to prevent from race conditions;
				// must be unlock by cache's client
				// TODO: may be unsafe and lead to infinite page locks
				c.sharedLockTable.YieldLock(pruneKey)
			}
			pruneKey, pruneItem = c.prune()
		}()
		if pruneKey == -1 {
			return
		}
		exec(pruneKey, pruneItem)
	}
}

func (c *LRUCache) pruneCandidate() int64 {
	backItem := c.cache.Back()
	if backItem == nil {
		return -1
	}
	cacheItem := backItem.Value.(*lruCacheItem)
	return cacheItem.key
}

func (c *LRUCache) prune() (int64, interface{}) {
	backItem := c.cache.Back()
	if backItem == nil {
		return -1, nil
	}
	cacheItem := backItem.Value.(*lruCacheItem)
	c.index.Delete(cacheItem.key)
	c.cache.Remove(backItem)
	return cacheItem.key, cacheItem.item
}
