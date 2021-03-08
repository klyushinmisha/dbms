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
	pageSize        int
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
		pageSize:        pageSize,
	}
}

func (dIo *DiskIO) effectiveFragmentSize() int {
	return dIo.pageSize / 4
}

func (dIo *DiskIO) DumbWritePos(record *Record) int64 {
	return -1
}

func (dIo *DiskIO) DumbWrite(record *Record) int64 {
	data, marshalErr := record.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	pagePos := dIo.freeSpaceMapper.FindFirstFit(len(data))
	pPage := dIo.ReadPage(pagePos)
	pPage.Used()
	return -1
}

func (dIo *DiskIO) readPageFromDisk(pos int64) *Page {
	pPage := AllocatePage(dIo.pageSize, HEAP_PAGE)
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

func (dIo *DiskIO) ReadPage(pos int64) *Page {
	// TODO: add caching
	return dIo.readPageFromDisk(pos)
}

func (dIo *DiskIO) writePageOnDisk(pos int64, pPage *Page) {
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

func (dIo *DiskIO) WritePage(pos int64, pPage *Page) {
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
