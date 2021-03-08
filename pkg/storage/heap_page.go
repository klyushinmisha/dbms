package storage

import (
	"bytes"
	"dbms/pkg/utils"
	"encoding/binary"
	"errors"
	"log"
)

var heapPageHeaderSize = 8

type heapPageHeader struct {
	RecordsNum int32
	FreeSpace  int32
}

type HeapPage struct {
	heapPageHeader
	data []byte
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
	return pBuffer.Bytes(), nil
}

func (p *HeapPage) UnmarshalBinary(data []byte) error {
	var readErr error
	var pBuffer = bytes.NewBuffer(data)
	readErr = binary.Read(pBuffer, binary.LittleEndian, &p.heapPageHeader)
	if readErr != nil {
		log.Panic(readErr)
	}
	p.data = pBuffer.Bytes()
	return nil
}

func (p *HeapPage) Init() {
	p.RecordsNum = 0
	p.FreeSpace = int32(len(p.data))
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

func (p *HeapPage) deleteRecord(recordN int) {
	recordStart := int(p.readPointer(recordN))
	recordEnd := len(p.data)
	if recordN != 0 {
		recordEnd = int(p.readPointer(recordN - 1))
	}
	recordLen := recordEnd - recordStart
	blockHead := int(p.readPointer(int(p.RecordsNum - 1)))
	blockTail := recordStart
	// remove record
	copy(p.data[blockHead+recordLen:blockTail+recordLen], p.data[blockHead:blockTail])
	p.RecordsNum--
	// 4 is int32 size
	// remove pointer from payload
	copy(p.data[recordN*4:p.RecordsNum*4], p.data[(recordN+1)*4:(p.RecordsNum+1)*4])
	p.FreeSpace += int32(4 + recordLen)
	// rebind pointers
	for n := recordN; n < int(p.RecordsNum); n++ {
		p.writePointer(n, p.readPointer(n)+int32(recordLen))
	}
}

func (p *HeapPage) ReadRecord(recordN int) *Record {
	if recordN < 0 || recordN >= int(p.RecordsNum) {
		return nil
	}
	var record Record
	var recordData []byte
	recordStart := p.readPointer(recordN)
	if recordN == 0 {
		// last record
		recordData = p.data[recordStart:]
	} else {
		recordEnd := p.readPointer(recordN - 1)
		recordData = p.data[recordStart:recordEnd]
	}
	unmarshalErr := record.UnmarshalBinary(recordData)
	if unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return &record
}

func (p *HeapPage) FindRecordByKey(key []byte) (*Record, int) {
	for n := 0; n < int(p.RecordsNum); n++ {
		foundRecord := p.ReadRecord(n)
		if utils.Memcmp(key, foundRecord.key) == 0 {
			return foundRecord, n
		}
	}
	return nil, -1
}

func (p *HeapPage) DeleteRecordByKey(key []byte) bool {
	foundRecord, n := p.FindRecordByKey(key)
	if foundRecord != nil {
		p.deleteRecord(n)
		return true
	}
	return false
}

var ErrPageIsFull = errors.New("page is full")

// Expected workflow is:
//     - if record size is larger than heap block data size then panic (TODO: add spanning records)
//     - find item in index
//     - if found
//         - delete
//         - if can fit record in current block, put record
//         - else find new block
//     - else find new block and put record
//     - if block not found, then create new one
//     - update index leaf by putting new block pos
func (p *HeapPage) WriteRecord(record *Record) error {
	var recordData []byte
	var marshalErr error
	// get free space with potentially removed record
	freeSpace := int(p.FreeSpace)
	foundRecord, n := p.FindRecordByKey(record.key)
	if foundRecord != nil {
		freeSpace += foundRecord.Size()
	}
	if freeSpace < record.Size() {
		return ErrPageIsFull
	}
	if foundRecord != nil {
		p.deleteRecord(n)
	}
	recordData, marshalErr = record.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	recordEnd := len(p.data)
	if p.RecordsNum != 0 {
		recordEnd = int(p.readPointer(int(p.RecordsNum) - 1))
	}
	recordStart := recordEnd - len(recordData)
	copy(p.data[recordStart:], recordData)
	p.writePointer(int(p.RecordsNum), int32(recordStart))
	p.RecordsNum++
	// 4 is int32 size
	p.FreeSpace -= int32(4 + len(recordData))
	return nil
}
