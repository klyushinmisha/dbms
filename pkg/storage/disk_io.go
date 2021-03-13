package storage

import (
	"io"
	"log"
	"os"
)

type DiskIO struct {
	file            *os.File
	freeSpaceMapper *FreeSpaceMapper
	usedBlockMapper *UsedBlockMapper
	PageSize        int
}

// MakeDiskIO is constructor for DiskIO. If nil PageCache is passed, the cache will be ignored
func MakeDiskIO(
	file *os.File,
	freeSpaceMapper *FreeSpaceMapper,
	usedBlockMapper *UsedBlockMapper,
	pageSize int,
) *DiskIO {
	return &DiskIO{
		file:            file,
		freeSpaceMapper: freeSpaceMapper,
		usedBlockMapper: usedBlockMapper,
		PageSize:        pageSize,
	}
}

func (dIo *DiskIO) Finalize() error {
	return dIo.file.Close()
}

func (dIo *DiskIO) effectiveFragmentSize() int {
	return dIo.PageSize / 4
}

func (dIo *DiskIO) readPageFromDisk(pos int64, pageType byte) *HeapPage {
	pPage := AllocatePage(dIo.PageSize, pageType)
	_, seekErr := dIo.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	pageBlob := make([]byte, dIo.PageSize)
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

func (dIo *DiskIO) ReadDataPage(pos int64) *DataPage {
	// TODO: add caching
	return DataPageFromHeapPage(dIo.readPageFromDisk(pos, DATA_PAGE))
}

func (dIo *DiskIO) ReadIndexPage(pos int64) *IndexPage {
	// TODO: add caching
	return IndexPageFromHeapPage(dIo.readPageFromDisk(pos, INDEX_PAGE))
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

func (dIo *DiskIO) WritePage(pos int64, pPage *HeapPage) {
	// TODO: add caching
	dIo.writePageOnDisk(pos, pPage)
}

func (dIo *DiskIO) GetNextPagePosition() int64 {
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
