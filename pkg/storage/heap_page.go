package storage

import (
	"bytes"
	"dbms/pkg/utils"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log"
	"unsafe"
)

var heapPageHeaderSize = 10

type heapPageHeader struct {
	Flags      utils.BitArray
	Type       uint8
	RecordsNum int32
	FreeSpace  int32
}

func (ph *heapPageHeader) Used() bool {
	return ph.Flags.Get(0)
}

func (ph *heapPageHeader) SetUsed(value bool) {
	ph.Flags.Set(value, 0)
}

var pageChecksumSize = 4

type HeapPage struct {
	heapPageHeader
	data     []byte
	checksum uint32
}

func AllocatePage(pageSize int, pageType byte) *HeapPage {
	pPage := new(HeapPage)
	dataSize := pageSize - heapPageHeaderSize - pageChecksumSize
	pPage.RecordsNum = 0
	pPage.Type = pageType
	pPage.FreeSpace = int32(len(pPage.data))
	pPage.data = make([]byte, dataSize)
	return pPage
}

func (p *HeapPage) MarshalBinary() ([]byte, error) {
	var pBuffer = new(bytes.Buffer)
	var writeErr error

	writeErr = binary.Write(pBuffer, binary.LittleEndian, p.heapPageHeader)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	_, writeErr = pBuffer.Write(p.data)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	p.checksum = crc32.ChecksumIEEE(pBuffer.Bytes())
	writeErr = binary.Write(pBuffer, binary.LittleEndian, p.checksum)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	return pBuffer.Bytes(), nil
}

var ErrChecksum = errors.New("corrupted payload: page checksum mismatch")

func (p *HeapPage) UnmarshalBinary(data []byte) error {
	var readErr error
	pageNoChecksumSize := len(data) - int(unsafe.Sizeof(p.checksum))
	var pBuffer = bytes.NewBuffer(data[pageNoChecksumSize:])
	readErr = binary.Read(pBuffer, binary.LittleEndian, &p.checksum)
	if readErr != nil {
		log.Panic(readErr)
	}
	pBuffer = bytes.NewBuffer(data[:pageNoChecksumSize])
	if p.checksum != crc32.ChecksumIEEE(pBuffer.Bytes()) {
		return ErrChecksum
	}
	pBuffer = bytes.NewBuffer(data)
	readErr = binary.Read(pBuffer, binary.LittleEndian, &p.heapPageHeader)
	if readErr != nil {
		log.Panic(readErr)
	}
	p.data = pBuffer.Bytes()
	return nil
}

func (p *HeapPage) readPointer(pointerNum int) int32 {
	var pointer int32
	// 4 is int32 size
	readBias := pointerNum * 4
	reader := bytes.NewReader(p.data[readBias : readBias+4])
	readErr := binary.Read(reader, binary.LittleEndian, &pointer)
	if readErr != nil {
		log.Panic(readErr)
	}
	return pointer
}

func (p *HeapPage) writePointer(pointerNum int, pointer int32) {
	// 4 is int32 size
	writeBias := pointerNum * 4
	buffer := bytes.NewBuffer(p.data[writeBias:writeBias])
	writeErr := binary.Write(buffer, binary.LittleEndian, pointer)
	if writeErr != nil {
		log.Panic(writeErr)
	}
}
