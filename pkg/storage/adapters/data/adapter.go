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
	if da.storage.LockTable() != nil {
		da.storage.LockTable().YieldLock(pos)
		defer da.storage.LockTable().Unlock(pos)
	}
	page := da.storage.ReadPageAtPos(pos)
	dpa := newDataPageAdapter(page)
	rec, _ := dpa.FindRecordByKey([]byte(key))
	if rec == nil {
		return nil, ErrRecordNotFound
	}
	return rec.Data, nil
}

func (da *DataAdapter) WriteAtPos(key string, data []byte, pos int64, lockPage bool) error {
	if da.storage.LockTable() != nil {
		if lockPage {
			da.storage.LockTable().YieldLock(pos)
			defer da.storage.LockTable().Unlock(pos)
		}
	}
	page := da.storage.ReadPageAtPos(pos)
	dpa := newDataPageAdapter(page)
	if writeErr := dpa.WriteRecordByKey([]byte(key), data); writeErr != nil {
		return writeErr
	}
	da.storage.WritePageAtPos(page, pos)
	return nil
}

func (da *DataAdapter) Write(key string, data []byte) (int64, error) {
	var rec record
	rec.Key = []byte(key)
	rec.Data = data
	pos := da.storage.FindFirstFit(2 * rec.Size())
	if pos == -1 {
		page := storage.AllocatePage(da.storage.PageSize())
		dpa := newDataPageAdapter(page)
		if writeErr := dpa.WriteRecordByKey([]byte(key), data); writeErr != nil {
			return -1, writeErr
		}
		pos = da.storage.WritePage(page)
		if da.storage.LockTable() != nil {
			defer da.storage.LockTable().Unlock(pos)
		}
		return pos, nil
	}
	if da.storage.LockTable() != nil {
		defer func() {
			da.storage.LockTable().Unlock(pos)
		}()
	}
	// TODO: lock here to prevent dirty writes
	return pos, da.WriteAtPos(key, data, pos, false)
}

func (da *DataAdapter) DeleteAtPos(key string, pos int64) error {
	if da.storage.LockTable() != nil {
		da.storage.LockTable().YieldLock(pos)
		defer da.storage.LockTable().Unlock(pos)
	}
	page := da.storage.ReadPageAtPos(pos)
	dpa := newDataPageAdapter(page)
	if found := dpa.DeleteRecordByKey([]byte(key)); !found {
		return ErrRecordNotFound
	}
	da.storage.WritePageAtPos(page, pos)
	return nil
}
