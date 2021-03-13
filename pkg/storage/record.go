package storage

import (
	"bytes"
	"encoding/binary"
	"log"
)

type Record struct {
	Key  []byte
	Data []byte
}

func NewRecord(key []byte, data []byte) *Record {
	var record Record
	record.Key = key
	record.Data = data
	return &record
}

func (r *Record) Size() int {
	// 4 is int32 size
	return len(r.Key) + len(r.Data) + 4 + 4
}

func (r *Record) MarshalBinary() ([]byte, error) {
	var pBuffer = new(bytes.Buffer)
	var writeErr error
	var size int32

	size = int32(len(r.Key))
	writeErr = binary.Write(pBuffer, binary.LittleEndian, size)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	_, writeErr = pBuffer.Write(r.Key)
	if writeErr != nil {
		log.Panic(writeErr)
	}

	size = int32(len(r.Data))
	writeErr = binary.Write(pBuffer, binary.LittleEndian, size)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	_, writeErr = pBuffer.Write(r.Data)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	return pBuffer.Bytes(), nil
}

func (r *Record) UnmarshalBinary(data []byte) error {
	var readErr error
	var pBuffer = bytes.NewBuffer(data)
	var size int32

	readErr = binary.Read(pBuffer, binary.LittleEndian, &size)
	if readErr != nil {
		log.Panic(readErr)
	}
	r.Key = make([]byte, size)
	_, readErr = pBuffer.Read(r.Key)
	if readErr != nil {
		log.Panic(readErr)
	}

	readErr = binary.Read(pBuffer, binary.LittleEndian, &size)
	if readErr != nil {
		log.Panic(readErr)
	}
	r.Data = make([]byte, size)
	_, readErr = pBuffer.Read(r.Data)
	if readErr != nil {
		log.Panic(readErr)
	}
	return nil
}
