package pkg

import (
	"dbms/pkg/access"
	"dbms/pkg/storage"
	"log"
	"os"
)

const DefaultPageSize = 8192

type Executor struct {
	index     *access.BPlusTree
	indexDisk *storage.DiskIO
	disk      *storage.DiskIO
}

func InitExecutor(indexFile, dataFile *os.File) *Executor {
	var e Executor
	e.disk = storage.MakeDiskIO(dataFile, nil, nil, DefaultPageSize)
	e.indexDisk = storage.MakeDiskIO(indexFile, nil, nil, DefaultPageSize)
	e.index = access.MakeBPlusTree(e.indexDisk)
	e.index.Init()
	return &e
}

func (e *Executor) Finalize() {
	closeErr := e.disk.Finalize()
	if closeErr != nil {
		log.Fatalln(closeErr)
	}
	closeErr = e.indexDisk.Finalize()
	if closeErr != nil {
		log.Fatalln(closeErr)
	}
}

func (e *Executor) Get(key string) ([]byte, bool) {
	pointer, findErr := e.index.Find(key)
	if findErr == access.ErrKeyNotFound {
		return nil, false
	}
	page := e.disk.ReadDataPage(pointer)
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
		dataPage := e.disk.ReadDataPage(pointer)
		writeErr := dataPage.WriteByKey(key, data)
		if writeErr == nil {
			allocateNewDataPage = false
		} else {
			dataPage.DeleteRecordByKey([]byte(key))
		}
		e.disk.WritePage(pointer, dataPage.HeapPage)
	}
	// TODO: instead of allocation use free space map
	if allocateNewDataPage {
		pos := e.disk.GetNextPagePosition()
		dataPage := storage.AllocateDataPage(e.disk.PageSize)
		writeErr := dataPage.WriteByKey(key, data)
		if writeErr != nil {
			log.Panic("can't fit value on page")
		}
		e.disk.WritePage(pos, dataPage.HeapPage)
		e.index.Insert(key, pos)
	}
}

func (e *Executor) Delete(key string) bool {
	pointer, findErr := e.index.Find(key)
	if findErr == access.ErrKeyNotFound {
		return false
	}
	dataPage := e.disk.ReadDataPage(pointer)
	dataPage.DeleteRecordByKey([]byte(key))
	e.disk.WritePage(pointer, dataPage.HeapPage)
	if deleteErr := e.index.Delete(key); deleteErr == access.ErrKeyNotFound {
		log.Panic("index and data pages mismatch")
	}
	return true
}
