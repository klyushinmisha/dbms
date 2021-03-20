package data

import (
	"bytes"
	"encoding/binary"
	"log"
)

const (
	// int32
	keyLenSize = 4
	// int32
	dataLenSize = 4
)

type record struct {
	Key  []byte
	Data []byte
}

func NewRecord(key []byte, data []byte) *record {
	var rec record
	rec.Key = key
	rec.Data = data
	return &rec
}

func (r *record) Size() int {
	return len(r.Key) + len(r.Data) + keyLenSize + dataLenSize
}

func (r *record) MarshalBinary() ([]byte, error) {
	recBuf := new(bytes.Buffer)
	keySize := int32(len(r.Key))
	if writeErr := binary.Write(recBuf, binary.LittleEndian, keySize); writeErr != nil {
		log.Panic(writeErr)
	}
	if _, writeErr := recBuf.Write(r.Key); writeErr != nil {
		log.Panic(writeErr)
	}
	dataSize := int32(len(r.Data))
	if writeErr := binary.Write(recBuf, binary.LittleEndian, dataSize); writeErr != nil {
		log.Panic(writeErr)
	}
	if _, writeErr := recBuf.Write(r.Data); writeErr != nil {
		log.Panic(writeErr)
	}
	return recBuf.Bytes(), nil
}

func (r *record) UnmarshalBinary(data []byte) error {
	recBuf := bytes.NewBuffer(data)
	keySize := new(int32)
	if readErr := binary.Read(recBuf, binary.LittleEndian, keySize); readErr != nil {
		log.Panic(readErr)
	}
	r.Key = make([]byte, *keySize)
	if _, readErr := recBuf.Read(r.Key); readErr != nil {
		log.Panic(readErr)
	}
	dataSize := new(int32)
	if readErr := binary.Read(recBuf, binary.LittleEndian, dataSize); readErr != nil {
		log.Panic(readErr)
	}
	r.Data = make([]byte, *dataSize)
	if _, readErr := recBuf.Read(r.Data); readErr != nil {
		log.Panic(readErr)
	}
	return nil
}
