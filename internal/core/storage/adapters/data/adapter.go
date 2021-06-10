package data

import (
	"dbms/internal/core/transaction"
	"errors"
)

var (
	ErrRecordNotFound = errors.New("record not found")
	ErrPageIsFull     = errors.New("page is full")
)

type DataAdapter struct {
	tx transaction.Tx
}

func NewDataAdapter(tx transaction.Tx) *DataAdapter {
	var da DataAdapter
	da.tx = tx
	return &da
}

func (da *DataAdapter) FindAtPos(key string, pos int64) ([]byte, error) {
	page := da.tx.ReadPageAtPos(pos)
	dpa := newDataPageAdapter(page)
	rec, _ := dpa.FindRecordByKey([]byte(key))
	if rec == nil {
		return nil, ErrRecordNotFound
	}
	return rec.Data, nil
}

func (da *DataAdapter) WriteAtPos(key string, data []byte, pos int64) error {
	page := da.tx.ReadPageAtPos(pos)
	dpa := newDataPageAdapter(page)
	if writeErr := dpa.WriteRecordByKey([]byte(key), data); writeErr != nil {
		return writeErr
	}
	da.tx.WritePageAtPos(page, pos)
	return nil
}

func (da *DataAdapter) Write(key string, data []byte) (int64, error) {
	var rec record
	rec.Key = []byte(key)
	rec.Data = data
	// TODO: write at free page
	page := da.tx.AllocatePage()
	dpa := newDataPageAdapter(page)
	if writeErr := dpa.WriteRecordByKey([]byte(key), data); writeErr != nil {
		return -1, writeErr
	}
	return da.tx.WritePage(page), nil
}

func (da *DataAdapter) DeleteAtPos(key string, pos int64) error {
	page := da.tx.ReadPageAtPos(pos)
	dpa := newDataPageAdapter(page)
	if found := dpa.DeleteRecordByKey([]byte(key)); !found {
		return ErrRecordNotFound
	}
	da.tx.WritePageAtPos(page, pos)
	return nil
}
