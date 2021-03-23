package lru_cache

import (
	"container/list"
	"runtime"
	"sync"
)

type lruCacheLockTable struct {
	mux   sync.Mutex
	table map[int64]bool
}

func newLRUCacheLockTable(cap int) *lruCacheLockTable {
	var table lruCacheLockTable
	table.table = make(map[int64]bool, cap)
	return &table
}

func (t *lruCacheLockTable) TryLock(key int64) bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	locked, found := t.table[key]
	if found && locked {
		return false
	}
	t.table[key] = true
	return true
}

func (t *lruCacheLockTable) Unlock(key int64) {
	t.mux.Lock()
	defer t.mux.Unlock()
	t.table[key] = false
}

type lruCacheItem struct {
	key  int64
	item interface{}
}

// TODO: make thread-safe access to index map and cache list to prevent ABA problem and etc.
type LRUCache struct {
	cap       int
	lockTable *lruCacheLockTable
	index     sync.Map
	cacheMux  sync.RWMutex
	cache     *list.List
}

func NewLRUCache(cap int) *LRUCache {
	var c LRUCache
	c.cap = cap
	c.lockTable = newLRUCacheLockTable(cap)
	c.cache = list.New()
	return &c
}

func (c *LRUCache) yieldLock(key int64) {
	for !c.lockTable.TryLock(key) {
		// allow other goroutines to work if can't lock page
		runtime.Gosched()
	}
}

func (c *LRUCache) replaceCacheItem(insertItem *lruCacheItem, removeE *list.Element) *list.Element {
	c.cacheMux.Lock()
	defer c.cacheMux.Unlock()
	c.cache.Remove(removeE)
	return c.cache.PushFront(insertItem)
}

func (c *LRUCache) putCacheItemWithPrune(item *lruCacheItem) (*list.Element, int64, interface{}) {
	c.cacheMux.Lock()
	defer c.cacheMux.Unlock()
	pruneKey := int64(-1)
	pruneItem := interface{}(nil)
	if c.cache.Len() == c.cap {
		pruneKey, pruneItem = c.prune()
	}
	return c.cache.PushFront(item), pruneKey, pruneItem
}

func (c *LRUCache) Put(key int64, item interface{}) (int64, interface{}) {
	c.yieldLock(key)
	defer c.lockTable.Unlock(key)
	cacheItem := new(lruCacheItem)
	cacheItem.key = key
	cacheItem.item = item
	listItem, found := c.index.Load(key)
	if found {
		c.index.Store(key, c.replaceCacheItem(cacheItem, listItem.(*list.Element)))
		return -1, nil
	} else {
		e, pruneKey, pruneItem := c.putCacheItemWithPrune(cacheItem)
		c.index.Store(key, e)
		return pruneKey, pruneItem
	}
}

func (c *LRUCache) Get(key int64) (interface{}, bool) {
	// TODO: solve ABA problem and remove locking
	// at the moment ABA problem is related to
	// index read -> index delete -> list delete -> list access case
	c.yieldLock(key)
	defer c.lockTable.Unlock(key)
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
	c.yieldLock(cacheItem.key)
	defer c.lockTable.Unlock(cacheItem.key)
	c.index.Delete(cacheItem.key)
	c.cache.Remove(backItem)
	return cacheItem.key, cacheItem.item
}
