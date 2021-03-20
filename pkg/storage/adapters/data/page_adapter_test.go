package data

import (
	"dbms/pkg/storage"
	"log"
	"testing"
)

func TestDataPageAdapter_ReadWrite(t *testing.T) {
	var err error
	p := storage.AllocatePage(1024)
	dpa := newDataPageAdapter(p)

	// fill page
	key := "HELLO"
	recData := "__INITIAL_DATA__"
	for {
		rec := NewRecord([]byte(key), []byte(recData))
		err = dpa.WriteRecord(rec)
		if err == ErrPageIsFull {
			break
		}
		key += "_"
	}

	// check records
	key = "HELLO"
	for i := 0; ; i++ {
		rec := dpa.ReadRecord(i)
		if rec == nil {
			break
		}
		if key != string(rec.Key) {
			log.Panic("keys not equal")
		}
		if recData != string(rec.Data) {
			log.Panic("datas not equal")
		}
		key += "_"
	}

	// override records
	key = "HELLO"
	recData = "OVERRIDE_DATA"
	for {
		rec := NewRecord([]byte(key), []byte(recData))
		err = dpa.WriteRecord(rec)
		if err == ErrPageIsFull {
			break
		}
		key += "_"
	}

	// check overridden records
	key = "HELLO"
	for i := 0; ; i++ {
		rec := dpa.ReadRecord(i)
		if rec == nil {
			break
		}
		if key != string(rec.Key) {
			log.Panic("keys not equal")
		}
		if recData != string(rec.Data) {
			log.Panic("datas not equal")
		}
		key += "_"
	}
}
