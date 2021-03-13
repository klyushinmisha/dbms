package storage

import (
	"dbms/pkg/utils"
	"errors"
	"log"
)

const DATA_PAGE byte = 0

type DataPage struct {
	*HeapPage
}

func AllocateDataPage(pageSize int) *DataPage {
	var p DataPage
	p.HeapPage = AllocatePage(pageSize, DATA_PAGE)
	return &p
}

func DataPageFromHeapPage(p *HeapPage) *DataPage {
	var convPage DataPage
	convPage.HeapPage = p
	return &convPage
}

func (p *DataPage) deleteRecord(recordN int) {
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

func (p *DataPage) ReadRecord(recordN int) *Record {
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

func (p *DataPage) FindRecordByKey(key []byte) (*Record, int) {
	for n := 0; n < int(p.RecordsNum); n++ {
		foundRecord := p.ReadRecord(n)
		if utils.Memcmp(key, foundRecord.Key) == 0 {
			return foundRecord, n
		}
	}
	return nil, -1
}

func (p *DataPage) DeleteRecordByKey(key []byte) bool {
	foundRecord, n := p.FindRecordByKey(key)
	if foundRecord != nil {
		p.deleteRecord(n)
		return true
	}
	return false
}

var ErrPageIsFull = errors.New("page is full")

// TODO: spanning records
func (p *DataPage) WriteRecord(record *Record) error {
	var recordData []byte
	var marshalErr error
	// get free space with potentially removed record
	freeSpace := int(p.FreeSpace)
	foundRecord, n := p.FindRecordByKey(record.Key)
	if foundRecord != nil {
		freeSpace += foundRecord.Size()
	}
	// 4 is int32 size
	if freeSpace < record.Size()+4 {
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

func (p *DataPage) WriteByKey(key string, data []byte) error {
	var record Record
	record.Key = []byte(key)
	record.Data = data
	return p.WriteRecord(&record)
}
