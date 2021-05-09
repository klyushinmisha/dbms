package data

import (
	"dbms/pkg/core/storage"
	"log"
)

type dataPageAdapter struct {
	page *storage.HeapPage
}

func newDataPageAdapter(page *storage.HeapPage) *dataPageAdapter {
	var dpa dataPageAdapter
	dpa.page = page
	return &dpa
}

func (dpa *dataPageAdapter) ReadRecord(n int) *record {
	if n < 0 || n >= dpa.page.Records() {
		return nil
	}
	recData := dpa.page.ReadData(n)
	rec := new(record)
	if unmarshalErr := rec.UnmarshalBinary(recData); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return rec
}

func (dpa *dataPageAdapter) FindRecordByKey(key []byte) (*record, int) {
	for n := 0; n < dpa.page.Records(); n++ {
		foundRecord := dpa.ReadRecord(n)
		if memcmp(key, foundRecord.Key) == 0 {
			return foundRecord, n
		}
	}
	return nil, -1
}

func (dpa *dataPageAdapter) DeleteRecordByKey(key []byte) bool {
	foundRecord, n := dpa.FindRecordByKey(key)
	if foundRecord != nil {
		dpa.page.DeleteData(n)
		return true
	}
	return false
}

// TODO: spanning records
func (dpa *dataPageAdapter) WriteRecord(record *record) error {
	// get free space with potentially removed record
	expSpace := dpa.page.FreeSpace()
	foundRecord, n := dpa.FindRecordByKey(record.Key)
	if foundRecord != nil {
		expSpace += foundRecord.Size()
	}
	reqSpace := record.Size() + storage.HeapRecordPointerSize
	if expSpace < reqSpace {
		return ErrPageIsFull
	}
	if foundRecord != nil {
		dpa.page.DeleteData(n)
	}
	recData, marshalErr := record.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	dpa.page.AppendData(recData)
	return nil
}

func (dpa *dataPageAdapter) WriteRecordByKey(key []byte, data []byte) error {
	var rec record
	rec.Key = key
	rec.Data = data
	return dpa.WriteRecord(&rec)
}

func memcmp(a []byte, b []byte) int {
	for pos := 0; pos < len(a) && pos < len(b); pos++ {
		if a[pos] > b[pos] {
			return 1
		}
		if a[pos] < b[pos] {
			return -1
		}
	}
	if len(a) > len(b) {
		return 1
	}
	if len(b) > len(a) {
		return -1
	}
	return 0
}
