package storage

import (
	"dbms/pkg/cache"
	"io"
	"log"
	"os"
)

type DiskIO struct {
	file            *os.File
	freeSpaceMapper *FreeSpaceMapper
	usedBlockMapper *UsedBlockMapper
	cache           cache.Cache
	pageSize        int
	maxPos          int64
}

// MakeDiskIO is constructor for DiskIO. If nil PageCache is passed, the cache will be ignored
func MakeDiskIO(
	file *os.File,
	freeSpaceMapper *FreeSpaceMapper,
	usedBlockMapper *UsedBlockMapper,
	cache cache.Cache,
	pageSize int,
) *DiskIO {
	return &DiskIO{
		file:            file,
		freeSpaceMapper: freeSpaceMapper,
		usedBlockMapper: usedBlockMapper,
		cache:           cache,
		pageSize:        pageSize,
		maxPos:          -int64(pageSize),
	}
}

func (dIo *DiskIO) PageSize() int {
	return dIo.pageSize
}

func (dIo *DiskIO) Finalize() {
	dIo.cache.PruneAll(func(pos int64, page interface{}) {
		dIo.writePageOnDisk(pos, page.(*HeapPage))
	})
}

/*func (dIo *DiskIO) effectiveFragmentSize() int {
	return dIo.pageSize / 4
}*/

func (dIo *DiskIO) readPageFromDisk(pos int64, pageType byte) *HeapPage {
	pPage := AllocatePage(dIo.pageSize, pageType)
	_, seekErr := dIo.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	pageBlob := make([]byte, dIo.pageSize)
	_, readErr := dIo.file.Read(pageBlob)
	if readErr != nil {
		log.Panic(seekErr)
	}
	unmarshalErr := pPage.UnmarshalBinary(pageBlob)
	if unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return pPage
}

func (dIo *DiskIO) writePageOnDisk(pos int64, pPage *HeapPage) {
	_, seekErr := dIo.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	blob, marshalErr := pPage.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	_, writeErr := dIo.file.Write(blob)
	if writeErr != nil {
		log.Panic(writeErr)
	}
}

func (dIo *DiskIO) readPage(pos int64, pageType byte) *HeapPage {
	if dIo.cache != nil {
		if page, found := dIo.cache.Get(pos); found {
			return page.(*HeapPage)
		}
	}
	page := dIo.readPageFromDisk(pos, pageType)
	if dIo.cache != nil {
		prunedPos, prunedPage := dIo.cache.Put(pos, page)
		if prunedPos != -1 {
			dIo.writePageOnDisk(prunedPos, prunedPage.(*HeapPage))
		}
	}
	return page
}

func (dIo *DiskIO) writePage(pos int64, pPage *HeapPage) {
	if pos > dIo.maxPos {
		dIo.maxPos = pos
	}
	// TODO: store max pos value to access it in GetNextPagePosition
	if dIo.cache != nil {
		prunedPos, prunedPage := dIo.cache.Put(pos, pPage)
		if prunedPos != -1 {
			dIo.writePageOnDisk(prunedPos, prunedPage.(*HeapPage))
		}
		return
	}
	dIo.writePageOnDisk(pos, pPage)
}

func (dIo *DiskIO) GetNextPagePosition() int64 {
	// TODO: use FSM index
	if dIo.cache != nil {
		return dIo.maxPos + int64(dIo.pageSize)
	}
	info, statErr := dIo.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size()
}

func (dIo *DiskIO) IsFileEmpty() bool {
	info, statErr := dIo.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size() == 0
}

type DataDiskIO struct {
	*DiskIO
}

func NewDataDiskIO(disk *DiskIO) *DataDiskIO {
	return &DataDiskIO{disk}
}

func (dataIo *DataDiskIO) ReadPage(pos int64) *DataPage {
	return DataPageFromHeapPage(dataIo.readPage(pos, DATA_PAGE))
}

func (dataIo *DataDiskIO) WritePage(pos int64, pPage *DataPage) {
	dataIo.writePage(pos, pPage.HeapPage)
}

type IndexDiskIO struct {
	*DiskIO
}

func NewIndexDiskIO(disk *DiskIO) *IndexDiskIO {
	return &IndexDiskIO{disk}
}

func (indexIo *IndexDiskIO) ReadPage(pos int64) *IndexPage {
	return IndexPageFromHeapPage(indexIo.readPage(pos, INDEX_PAGE))
}

func (indexIo *IndexDiskIO) WritePage(pos int64, pPage *IndexPage) {
	indexIo.writePage(pos, pPage.HeapPage)
}
