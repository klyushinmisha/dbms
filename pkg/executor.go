package pkg

import (
	"dbms/pkg/access"
	"dbms/pkg/cache"
	"dbms/pkg/storage"
	"log"
	"os"
)

type Executor struct {
	index     *access.BPlusTree
	indexDisk *storage.IndexDiskIO
	disk      *storage.DataDiskIO
}

func InitExecutor(pageSize int, indexFile *os.File, dataFile *os.File, cacheSize int) *Executor {
	var e Executor
	diskCache := cache.NewLRUCache(cacheSize)
	e.disk = storage.NewDataDiskIO(storage.MakeDiskIO(dataFile, nil, nil, diskCache, pageSize))
	indexCache := cache.NewLRUCache(cacheSize)
	e.indexDisk = storage.NewIndexDiskIO(storage.MakeDiskIO(indexFile, nil, nil, indexCache, pageSize))
	e.index = access.MakeBPlusTree(100, e.indexDisk)
	e.index.Init()
	return &e
}

func (e *Executor) Finalize() {
	e.disk.Finalize()
	e.indexDisk.Finalize()
}

func (e *Executor) Get(key string) ([]byte, bool) {
	pointer, findErr := e.index.Find(key)
	if findErr == access.ErrKeyNotFound {
		return nil, false
	}
	page := e.disk.ReadPage(pointer)
	record, _ := page.FindRecordByKey([]byte(key))
	if record == nil {
		log.Panic("index and data pages mismatch")
	}
	return record.Data, true
}

func (e *Executor) Set(key string, data []byte) {
	allocateNewDataPage := true
	pointer, findErr := e.index.Find(key)
	if findErr == nil {
		dataPage := e.disk.ReadPage(pointer)
		writeErr := dataPage.WriteByKey(key, data)
		if writeErr == nil {
			allocateNewDataPage = false
		} else {
			dataPage.DeleteRecordByKey([]byte(key))
		}
		e.disk.WritePage(pointer, dataPage)
	}
	// TODO: instead of allocation use free space map
	if allocateNewDataPage {
		pos := e.disk.GetNextPagePosition()
		dataPage := storage.AllocateDataPage(e.disk.PageSize())
		writeErr := dataPage.WriteByKey(key, data)
		if writeErr != nil {
			log.Panic("can't fit value on page")
		}
		e.disk.WritePage(pos, dataPage)
		e.index.Insert(key, pos)
	}
}

func (e *Executor) Delete(key string) bool {
	pointer, findErr := e.index.Find(key)
	if findErr == access.ErrKeyNotFound {
		return false
	}
	dataPage := e.disk.ReadPage(pointer)
	dataPage.DeleteRecordByKey([]byte(key))
	e.disk.WritePage(pointer, dataPage)
	if deleteErr := e.index.Delete(key); deleteErr == access.ErrKeyNotFound {
		log.Panic("index and data pages mismatch")
	}
	return true
}
