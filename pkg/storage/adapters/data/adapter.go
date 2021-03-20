package data

import (
	"dbms/pkg/storage"
	"errors"
)

var (
	ErrRecordNotFound = errors.New("record not found")
	ErrPageIsFull     = errors.New("page is full")
)

type DataAdapter struct {
	storage *storage.HeapPageStorage
}

func NewDataAdapter(storage *storage.HeapPageStorage) *DataAdapter {
	var da DataAdapter
	da.storage = storage
	return &da
}

func (da *DataAdapter) FindAtPos(key string, pos int64) ([]byte, error) {
	page := da.storage.ReadPage(pos)
	dpa := newDataPageAdapter(page)
	rec, _ := dpa.FindRecordByKey([]byte(key))
	if rec == nil {
		return nil, ErrRecordNotFound
	}
	return rec.Data, nil
}

func (da *DataAdapter) WriteAtPos(key string, data []byte, pos int64) error {
	page := da.storage.ReadPage(pos)
	dpa := newDataPageAdapter(page)
	if writeErr := dpa.WriteRecordByKey([]byte(key), data); writeErr != nil {
		return writeErr
	}
	da.storage.WritePage(page, pos)
	return nil
}

func (da *DataAdapter) Write(key string, data []byte) (int64, error) {
	pos := da.storage.GetFreePagePosition()
	page := storage.AllocatePage(da.storage.PageSize())
	dpa := newDataPageAdapter(page)
	if writeErr := dpa.WriteRecordByKey([]byte(key), data); writeErr != nil {
		return -1, writeErr
	}
	da.storage.WritePage(page, pos)
	return pos, nil
}

func (da *DataAdapter) DeleteAtPos(key string, pos int64) error {
	page := da.storage.ReadPage(pos)
	dpa := newDataPageAdapter(page)
	if found := dpa.DeleteRecordByKey([]byte(key)); !found {
		return ErrRecordNotFound
	}
	da.storage.WritePage(page, pos)
	return nil
}
