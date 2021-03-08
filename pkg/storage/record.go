package storage

import (
	"bytes"
	"encoding/binary"
	"log"
)

type Record struct {
	key  []byte
	data []byte
}

func NewRecord(key []byte, data []byte) *Record {
	var record Record
	record.key = key
	record.data = data
	return &record
}

func (r *Record) Size() int {
	// 4 is int32 size
	return len(r.key) + len(r.data) + 4 + 4
}

func (r *Record) MarshalBinary() ([]byte, error) {
	var pBuffer = new(bytes.Buffer)
	var writeErr error
	var size int32

	size = int32(len(r.key))
	writeErr = binary.Write(pBuffer, binary.LittleEndian, size)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	_, writeErr = pBuffer.Write(r.key)
	if writeErr != nil {
		log.Panic(writeErr)
	}

	size = int32(len(r.data))
	writeErr = binary.Write(pBuffer, binary.LittleEndian, size)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	_, writeErr = pBuffer.Write(r.data)
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
	r.key = make([]byte, size)
	_, readErr = pBuffer.Read(r.key)
	if readErr != nil {
		log.Panic(readErr)
	}

	readErr = binary.Read(pBuffer, binary.LittleEndian, &size)
	if readErr != nil {
		log.Panic(readErr)
	}
	r.data = make([]byte, size)
	_, readErr = pBuffer.Read(r.data)
	if readErr != nil {
		log.Panic(readErr)
	}
	return nil
}
