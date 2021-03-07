package pkg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
	"unsafe"
)

type bitArray uint8

func (arr *bitArray) Set(value bool, pos int) {
	if value {
		*arr |= 1 << pos
	} else {
		bitMask := bitArray(^(1 << pos))
		*arr &= bitMask
	}
}

func (arr bitArray) Get(pos int) bool {
	return (arr>>pos)&1 == 1
}

type pageHeader struct {
	Flags   bitArray
	PayloadSize int64
}

// uint8 + int64
var pageHeaderSize = 9

type Page struct {
	header   pageHeader
	payload  []byte
	checksum uint32
}

// uint32
var pageChecksumSize = 4

func (pP *Page) Used() bool {
	return pP.header.Flags.Get(0)
}

func (pP *Page) SetUsed(value bool) {
	pP.header.Flags.Set(value, 0)
}

func (pP *Page) Dirty() bool {
	return pP.header.Flags.Get(1)
}

func (pP *Page) SetDirty(value bool) {
	pP.header.Flags.Set(value, 1)
}

func (pP *Page) MarshalBinary() ([]byte, error) {
	var pBuffer = new(bytes.Buffer)
	var writeErr error

	writeErr = binary.Write(pBuffer, binary.LittleEndian, pP.header)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	_, writeErr = pBuffer.Write(pP.payload)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	pP.checksum = crc32.ChecksumIEEE(pBuffer.Bytes())
	writeErr = binary.Write(pBuffer, binary.LittleEndian, pP.checksum)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	return pBuffer.Bytes(), nil
}

var ErrChecksum = errors.New("corrupted data: page checksum mismatch")

func (pP *Page) UnmarshalBinary(blob []byte) error {
	var readErr error
	pageNoChecksumSize := len(blob) - int(unsafe.Sizeof(pP.checksum))
	var pBuffer = bytes.NewBuffer(blob[pageNoChecksumSize:])
	readErr = binary.Read(pBuffer, binary.LittleEndian, &pP.checksum)
	if readErr != nil {
		log.Panic(readErr)
	}
	pBuffer = bytes.NewBuffer(blob[:pageNoChecksumSize])
	if pP.checksum != crc32.ChecksumIEEE(pBuffer.Bytes()) {
		return ErrChecksum
	}
	readErr = binary.Read(pBuffer, binary.LittleEndian, &pP.header)
	if readErr != nil {
		log.Panic(readErr)
	}
	_, readErr = pBuffer.Read(pP.payload)
	if readErr != nil {
		log.Panic(readErr)
	}
	return nil
}

func (pP *Page) PayloadSize() int {
	return len(pP.payload)
}

func AllocatePage(pageSize int) *Page {
	pPage := new(Page)
	payloadSize := pageSize - pageHeaderSize - pageChecksumSize
	pPage.payload = make([]byte, payloadSize)
	return pPage
}

type PageCacheItem struct {
	pos   int64
	pPage *Page
}

type PageCache interface {
	Get(pos int64) *Page
	Set(pos int64, pPage *Page)
	DirtyPages() []*PageCacheItem
}

type DiskIO struct {
	file     *os.File
	cache    PageCache
	pageSize int
}

// MakeDiskIO is constructor for DiskIO. If nil PageCache is passed, the cache will be ignored
func MakeDiskIO(file *os.File, cache PageCache, pageSize int) *DiskIO {
	return &DiskIO{
		file:     file,
		cache:    cache,
		pageSize: pageSize,
	}
}

func (dIo *DiskIO) readPageFromDisk(pos int64) *Page {
	pPage := AllocatePage(dIo.pageSize)
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
	var pPage *Page
	if dIo.cache != nil {
		pPage = dIo.cache.Get(pos)
	}
	if pPage == nil {
		pPage = dIo.readPageFromDisk(pos)
	}
	// TODO: mark only on updates
	// NOTE: mark here to simplify implementation
	pPage.SetDirty(true)
	return pPage
}

func (dIo *DiskIO) writePageOnDisk(pos int64, pPage *Page) {
	_, seekErr := dIo.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	pPage.SetDirty(false)
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
	if dIo.cache != nil {
		dIo.cache.Set(pos, pPage)
	} else {
		dIo.writePageOnDisk(pos, pPage)
	}
}

func (dIo *DiskIO) GetNextPagePosition() int64 {
	info, statErr := dIo.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size()
}

func (dIo *DiskIO) Flush() {
	if dIo.cache != nil {
		for _, pItem := range dIo.cache.DirtyPages() {
			dIo.writePageOnDisk(pItem.pos, pItem.pPage)
		}
	}
}


/*
diskIo.ReadPage(pos)
node.readFromPage(pPage) -> pNode
rd := NewPageReader(diskIo, startPos)
rd.Read()
...
node.writeOnPage(pPage)
diskIo.WritePage(pos, pPage)
*/