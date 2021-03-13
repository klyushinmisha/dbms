package storage

import (
	"log"
	"testing"
)

func TestDataPage_ReadWrite(t *testing.T) {
	var err error
	p := AllocateDataPage(1024)

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
