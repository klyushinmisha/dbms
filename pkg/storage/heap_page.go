package storage

import (
	"bytes"
	"dbms/pkg/utils"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log"
)

var (
	ErrChecksum = errors.New("corrupted payload: page checksum mismatch")
)

type heapPageHeader struct {
	Flags     utils.BitArray
	records   int32
	freeSpace int32
}

func (ph *heapPageHeader) Records() int {
	return int(ph.records)
}

func (ph *heapPageHeader) FreeSpace() int {
	return int(ph.freeSpace)
}

func (ph *heapPageHeader) Used() bool {
	return ph.Flags.Get(0)
}

func (ph *heapPageHeader) SetUsed(value bool) {
	ph.Flags.Set(value, 0)
}

const (
	// uint8 + int32 + int32
	heapPageHeaderSize = 9
	// int32
	heapPageChecksumSize = 4
	// int32
	HeapRecordPointerSize = 4
)

// TODO: move data page records and pointers manipulations to HeapPage
type HeapPage struct {
	heapPageHeader
	Data     []byte
	checksum uint32
}

func AllocatePage(pageSize int) *HeapPage {
	var page HeapPage
	page.records = 0
	page.Data = make([]byte, pageSize-heapPageHeaderSize-heapPageChecksumSize)
	page.freeSpace = int32(len(page.Data))
	return &page
}

func (p *HeapPage) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if writeErr := binary.Write(buf, binary.LittleEndian, p.heapPageHeader); writeErr != nil {
		log.Panic(writeErr)
	}
	if _, writeErr := buf.Write(p.Data); writeErr != nil {
		log.Panic(writeErr)
	}
	p.checksum = crc32.ChecksumIEEE(buf.Bytes())
	if writeErr := binary.Write(buf, binary.LittleEndian, p.checksum); writeErr != nil {
		log.Panic(writeErr)
	}
	return buf.Bytes(), nil
}

func (p *HeapPage) UnmarshalBinary(data []byte) error {
	pageNoChecksumSize := len(data) - heapPageChecksumSize
	crcBuf := bytes.NewBuffer(data[pageNoChecksumSize:])
	if readErr := binary.Read(crcBuf, binary.LittleEndian, &p.checksum); readErr != nil {
		log.Panic(readErr)
	}
	buf := bytes.NewBuffer(data[:pageNoChecksumSize])
	if p.checksum != crc32.ChecksumIEEE(buf.Bytes()) {
		return ErrChecksum
	}
	if readErr := binary.Read(buf, binary.LittleEndian, &p.heapPageHeader); readErr != nil {
		log.Panic(readErr)
	}
	p.Data = buf.Bytes()
	return nil
}

func (p *HeapPage) AppendData(data []byte) {
	recEnd := len(p.Data)
	if p.records != 0 {
		recEnd = int(p.readPointer(int(p.records) - 1))
	}
	recStart := recEnd - len(data)
	copy(p.Data[recStart:], data)
	p.writePointer(int(p.records), int32(recStart))
	p.records++
	p.freeSpace -= int32(HeapRecordPointerSize + len(data))
}

func (p *HeapPage) ReadData(n int) []byte {
	var recData []byte
	if n < 0 || n >= int(p.records) {
		return nil
	}
	recStart := p.readPointer(n)
	if n == 0 {
		// last rec
		recData = p.Data[recStart:]
	} else {
		recEnd := p.readPointer(n - 1)
		recData = p.Data[recStart:recEnd]
	}
	return recData
}

func (p *HeapPage) DeleteData(n int) {
	recStart := int(p.readPointer(n))
	recEnd := len(p.Data)
	if n != 0 {
		recEnd = int(p.readPointer(n - 1))
	}
	recLen := recEnd - recStart
	blkStart := int(p.readPointer(int(p.records - 1)))
	blkEnd := recStart
	// remove record
	copy(p.Data[blkStart+recLen:blkEnd+recLen], p.Data[blkStart:blkEnd])
	// remove pointer from payload
	ptrSize := HeapRecordPointerSize
	blkStart = n * ptrSize
	blkEnd = int(p.records-1) * ptrSize
	copy(p.Data[blkStart:blkEnd], p.Data[blkStart+ptrSize:blkEnd+ptrSize])
	p.records--
	p.freeSpace += int32(ptrSize + recLen)
	// rebind pointers
	for n := n; n < int(p.records); n++ {
		p.writePointer(n, p.readPointer(n)+int32(recLen))
	}
}

func (p *HeapPage) readPointer(ptrN int) int32 {
	var ptr int32
	readBias := ptrN * HeapRecordPointerSize
	reader := bytes.NewReader(p.Data[readBias : readBias+HeapRecordPointerSize])
	if readErr := binary.Read(reader, binary.LittleEndian, &ptr); readErr != nil {
		log.Panic(readErr)
	}
	return ptr
}

func (p *HeapPage) writePointer(ptrN int, ptr int32) {
	writeBias := ptrN * HeapRecordPointerSize
	writer := bytes.NewBuffer(p.Data[writeBias:writeBias])
	if writeErr := binary.Write(writer, binary.LittleEndian, ptr); writeErr != nil {
		log.Panic(writeErr)
	}
}
