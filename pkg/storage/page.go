package storage

import (
	"bytes"
	"dbms/pkg/utils"
	"encoding"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"log"
	"unsafe"
)

// page types (pageHeader.Type)
const (
	HEAP_PAGE  uint8 = 0
	INDEX_PAGE uint8 = 1
)

// uint8 + uint8
var pageHeaderSize = 2

type pageHeader struct {
	Flags utils.BitArray
	Type  uint8
}

func (ph *pageHeader) Used() bool {
	return ph.Flags.Get(0)
}

func (ph *pageHeader) SetUsed(value bool) {
	ph.Flags.Set(value, 0)
}

type PagePayload interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	Init()
}

// uint32
var pageChecksumSize = 4

type Page struct {
	pageHeader
	payload  PagePayload
	checksum uint32
}

func AllocatePage(pageSize int, pageType byte) *Page {
	pPage := new(Page)
	pPage.Type = pageType
	payloadSize := pageSize - pageHeaderSize - pageChecksumSize
	data := make([]byte, payloadSize)
	switch pageType {
	case HEAP_PAGE:
		pPage.payload = new(HeapPage)
	case INDEX_PAGE:
		log.Panic("not implemented")
	}
	unmarshalErr := pPage.payload.UnmarshalBinary(data)
	if unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	pPage.payload.Init()
	return pPage
}

func (pP *Page) MarshalBinary() ([]byte, error) {
	var pBuffer = new(bytes.Buffer)
	var writeErr error

	writeErr = binary.Write(pBuffer, binary.LittleEndian, pP.pageHeader)
	if writeErr != nil {
		log.Panic(writeErr)
	}
	data, marshalErr := pP.payload.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	_, writeErr = pBuffer.Write(data)
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

var ErrChecksum = errors.New("corrupted payload: page checksum mismatch")

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
	readErr = binary.Read(pBuffer, binary.LittleEndian, &pP.pageHeader)
	if readErr != nil {
		log.Panic(readErr)
	}
	switch pP.Type {
	case HEAP_PAGE:
		pP.payload = new(HeapPage)
	case INDEX_PAGE:
		log.Panic("not implemented")
	}
	unmarshalErr := pP.payload.UnmarshalBinary(pBuffer.Bytes())
	if unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return nil
}
