package pkg

import (
	"bufio"
	"errors"
	"io"
	"os"
)

type Store interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

type DiskStore struct {
	reader    *DataBlockReader
	writer    *DataBlockWriter
	bufWriter *bufio.Writer
}

func NewDiskStore(inp *os.File, out *os.File) *DiskStore {
	bufWriter := bufio.NewWriter(out)
	writer := NewDataBlockWriter(bufWriter)

	bufReader := bufio.NewReader(inp)
	reader := NewDataBlockReader(bufReader)

	return &DiskStore{reader: reader, writer: writer, bufWriter: bufWriter}
}

var ErrKeyNotFound = errors.New("key not found")

func (store *DiskStore) Get(key string) ([]byte, error) {
	keyBuf := make([]byte, 1024)
	valueBuf := make([]byte, 1024)
	for {
		keySize, err := store.reader.Read(keyBuf)
		if err != nil {
			panic(err)
		}
		valueSize, err := store.reader.Read(valueBuf)
		if err == io.EOF {
			return nil, ErrKeyNotFound
		}
		if string(keyBuf[:keySize]) == key {
			return valueBuf[:valueSize], nil
		}

	}
}

func (store *DiskStore) Set(key string, value []byte) error {
	defer store.bufWriter.Flush()
	_, err := store.writer.Write([]byte(key))
	if err != nil {
		return err
	}
	_, err = store.writer.Write(value)
	return err
}
