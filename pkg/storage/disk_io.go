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

func (dIo *DiskIO) PageSize() int {
	return dIo.pageSize
}

func (dIo *DiskIO) Finalize() error {
	return dIo.file.Close()
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

func (dIo *DiskIO) GetNextPagePosition() int64 {
	// TODO: use FSM index
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
	return DataPageFromHeapPage(dataIo.readPageFromDisk(pos, DATA_PAGE))
}

func (dataIo *DataDiskIO) WritePage(pos int64, pPage *DataPage) {
	dataIo.writePageOnDisk(pos, pPage.HeapPage)
}

type IndexDiskIO struct {
	*DiskIO
}

func NewIndexDiskIO(disk *DiskIO) *IndexDiskIO {
	return &IndexDiskIO{disk}
}

func (indexIo *IndexDiskIO) ReadPage(pos int64) *IndexPage {
	return IndexPageFromHeapPage(indexIo.readPageFromDisk(pos, INDEX_PAGE))
}

func (indexIo *IndexDiskIO) WritePage(pos int64, pPage *IndexPage) {
	indexIo.writePageOnDisk(pos, pPage.HeapPage)
}
