package access

import (
	"container/list"
)

type BPlusTreeCache interface {
	Set(addr AddrType, pNode *node)
	Get(addr AddrType) *node
}

type CacheItem struct {
	addr  AddrType
	pNode *node
}

type LinkedListCache struct {
	*list.List
}

func MakeLinkedListCache() *LinkedListCache {
	var cache LinkedListCache
	cache.List = list.New()
	return &cache
}

func (pCache *LinkedListCache) Set(addr AddrType, pNode *node) {
	for e := pCache.List.Front(); e != nil; e = e.Next() {
		if e.Value.(CacheItem).addr == addr {
			pCache.List.Remove(e)
			break
		}
	}
	var cacheItem CacheItem
	cacheItem.pNode = pNode
	cacheItem.addr = addr
	pCache.List.PushFront(&cacheItem)
}

func (pCache *LinkedListCache) Get(addr AddrType) *node {
	var elementToReturn *list.Element
	for e := pCache.List.Front(); e != nil; e = e.Next() {
		if e.Value.(CacheItem).addr == addr {
			elementToReturn = e
			pCache.List.Remove(e)
			break
		}
	}
	if elementToReturn != nil {
		return elementToReturn.Value.(CacheItem).pNode
	}
	return nil
}

func (pCache *LinkedListCache) Flush() {

}

/*

cache := MakeLinkedListCache()
cache.Set(pNode, addr)
pNode := cache.Get(addr)
if pNode == nil {
	readNodeFromFile
}

*/
