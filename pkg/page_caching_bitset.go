package pkg

import (
	"errors"
	"io"
	"log"
	"os"
)

// TODO: simplify page writer and reader
// return
type PageCachingBitset struct {
	// NOTE: this readWriteFile must be opened in r+ mode
	readWriteFile *os.File

	writer *PageWriter
	reader *PageReader

	curPage     *Page
	pageSize    int64
	pageToWrite int64
}

// bitset operations
const (
	bitsetCheck = iota
	bitsetSet
	bitsetReset
)

func NewPageCachingBitset(ioFile *os.File, pageSize int64) *PageCachingBitset {
	return &PageCachingBitset{
		readWriteFile: ioFile,
		pageSize:      pageSize,
		pageToWrite:   -1,
	}
}

func (c *PageCachingBitset) getPageFromAddr(addr int64) int64 {
	return addr / c.pageSize
}

func (c *PageCachingBitset) readPage(offset int64) error {
	// move to the page
	_, seekErr := c.readWriteFile.Seek(offset, io.SeekStart)
	log.Printf("Seeked at position %d (for read)\n", offset)
	if seekErr != nil {
		return seekErr
	}
	// read the page
	c.reader = NewPageReader(c.readWriteFile, c.pageSize)
	c.curPage = NewPage(c.pageSize)
	fi, fiErr := c.readWriteFile.Stat()
	if fiErr != nil {
		return fiErr
	}
	if fi.Size() >= offset+c.pageSize {
		_, readErr := c.reader.Read(c.curPage.Data)
		log.Printf("Read page (%d bytes) at position %d\n", c.pageSize, offset)
		return readErr
	}
	return nil
}

func (c *PageCachingBitset) initWriter(offset int64) error {
	// prepare to write the page
	c.writer = NewPageWriter(c.readWriteFile, c.pageSize)
	_, seekErr := c.readWriteFile.Seek(offset, io.SeekStart)
	log.Printf("Seeked at position %d (for flush)\n", offset)
	if seekErr != nil {
		return seekErr
	}
	return nil
}

func (c *PageCachingBitset) Flush() error {
	_, writeErr := c.writer.Write(c.curPage.Data)
	if writeErr != nil {
		return writeErr
	}
	flushErr := c.writer.Flush()
	if flushErr != nil {
		return flushErr
	}
	c.writer = nil
	return nil
}

var ErrInvalidBlockAddress = errors.New("address modulo divided by page size is not eq to 0")

// bitValueOperation works with pages and allows to mark blocks.
// some page caching technics implemented here
func (c *PageCachingBitset) bitValueOperation(blockAddr int64, operation int) (bool, error) {
	// NOTE: in case on operation != bitsetCheck first return value is redundant
	if blockAddr%c.pageSize != 0 {
		return false, ErrInvalidBlockAddress
	}
	var offset = blockAddr >> 3
	var page = offset / c.pageSize
	var byteOnPage = offset % c.pageSize
	var bitOnPage = blockAddr & 0b111

	// TODO: make seeking+writing more transparent
	if page != c.pageToWrite {
		// dump page on disk
		// ISSUE: can't flush whenever want
		if c.writer != nil {
			flushErr := c.Flush()
			if flushErr != nil {
				return false, flushErr
			}
		}
		initErr := c.initWriter(offset)
		if initErr != nil {
			return false, initErr
		}
		if operation == bitsetCheck {
			readErr := c.readPage(offset)
			if readErr != nil {
				return false, readErr
			}
		}
		c.pageToWrite = page
	}
	switch operation {
	case bitsetCheck:
		return ((c.curPage.Data[byteOnPage] >> bitOnPage) & 1) == 1, nil
	case bitsetSet:
		c.curPage.Data[byteOnPage] |= 1 << bitOnPage
		return false, nil
	case bitsetReset:
		c.curPage.Data[byteOnPage] &= ^(1 << bitOnPage)
		return false, nil
	default:
		panic("Invalid operation")
	}
}

// IDEA: set bits on pages and flush pages only when
// page number differs
func (c *PageCachingBitset) Set(blockAddr int64) error {
	_, setError := c.bitValueOperation(blockAddr, bitsetSet)
	log.Printf("Marked block at position %d as used\n", blockAddr)
	return setError
}

func (c *PageCachingBitset) Reset(blockAddr int64) error {
	_, resetError := c.bitValueOperation(blockAddr, bitsetReset)
	log.Printf("Marked block at position %d as free\n", blockAddr)
	return resetError
}

func (c *PageCachingBitset) Check(blockAddr int64) (bool, error) {
	isSet, checkErr := c.bitValueOperation(blockAddr, bitsetCheck)
	var blockStatus = "free"
	if isSet {
		blockStatus = "used"
	}
	log.Printf("Block at position %d is %s\n", blockAddr, blockStatus)
	return isSet, checkErr
}
