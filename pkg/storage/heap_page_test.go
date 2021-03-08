package storage

import (
	"log"
	"testing"
)

func TestHeapPage_ReadWrite(t *testing.T) {
	data := make([]byte, 1024, 1024)
	var p HeapPage
	err := p.UnmarshalBinary(data)
	if err != nil {
		log.Panic(err)
	}
	p.Init()

	// fill page
	key := "HELLO"
	recData := "__INITIAL_DATA__"
	for {
		rec := NewRecord([]byte(key), []byte(recData))
		err = p.WriteRecord(rec)
		if err == ErrPageIsFull {
			break
		}
		key += "_"
	}

	// check records
	key = "HELLO"
	for i := 0; ; i++ {
		rec := p.ReadRecord(i)
		if rec == nil {
			break
		}
		if key != string(rec.key) {
			log.Panic("keys not equal")
		}
		if recData != string(rec.data) {
			log.Panic("datas not equal")
		}
		key += "_"
	}

	// override records
	key = "HELLO"
	recData = "OVERRIDE_DATA"
	for {
		rec := NewRecord([]byte(key), []byte(recData))
		err = p.WriteRecord(rec)
		if err == ErrPageIsFull {
			break
		}
		key += "_"
	}

	// check overridden records
	key = "HELLO"
	for i := 0; ; i++ {
		rec := p.ReadRecord(i)
		if rec == nil {
			break
		}
		if key != string(rec.key) {
			log.Panic("keys not equal")
		}
		if recData != string(rec.data) {
			log.Panic("datas not equal")
		}
		key += "_"
	}
}
