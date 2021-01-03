package pkg

import (
	"encoding/binary"
	"errors"
	"io"
)

type DataBlockReader struct {
	reader io.Reader
}

func NewDataBlockReader(reader io.Reader) *DataBlockReader {
	return &DataBlockReader{reader: reader}
}

var endianness = binary.LittleEndian

var ErrBlockTooLarge = errors.New("can't allocate block in the buffer")

func (dbReader *DataBlockReader) Read(p []byte) (int, error) {
	var blockSize int64
	err := binary.Read(dbReader.reader, endianness, &blockSize)
	if err != nil {
		return 0, err
	}
	if int64(len(p)) < blockSize {
		return 0, ErrBlockTooLarge
	}
	buf := make([]byte, blockSize)
	n, err := dbReader.reader.Read(buf)
	copy(p, buf)
	return n, err
}

type DataBlockWriter struct {
	writer io.Writer
}

func NewDataBlockWriter(writer io.Writer) *DataBlockWriter {
	return &DataBlockWriter{writer: writer}
}

func (dbWriter *DataBlockWriter) Write(p []byte) (int, error) {
	err := binary.Write(dbWriter.writer, endianness, int64(len(p)))
	if err != nil {
		return 0, err
	}
	return dbWriter.writer.Write(p)
}
