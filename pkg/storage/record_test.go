package storage

import (
	"log"
	"testing"
)

func TestRecord_ReadWrite(t *testing.T) {
	key := "HELLO"
	data := "WORLD"
	rec := NewRecord([]byte(key), []byte(data))
	blob, err := rec.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	var recCopy Record
	err = recCopy.UnmarshalBinary(blob)
	if err != nil {
		log.Panic(err)
	}
	if key != string(recCopy.key) {
		log.Panic("keys not equal")
	}
	if data != string(recCopy.data) {
		log.Panic("datas not equal")
	}
}
