package pkg

import (
	"bufio"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	testFileName := "somefile.bin"
	blockToWrite := []byte{34, 45, 23, 1}

	out, err := os.Create(testFileName)
	defer out.Close()
	if err != nil {
		panic(err)
	}
	bufWriter := bufio.NewWriter(out)
	writer := NewDataBlockWriter(bufWriter)
	writer.Write(blockToWrite)
	bufWriter.Flush()

	out.Close()

	inp, err := os.Open(testFileName)
	defer inp.Close()
	bufReader := bufio.NewReader(inp)
	reader := NewDataBlockReader(bufReader)
	blockBuf := make([]byte, 1024)
	n, err := reader.Read(blockBuf)
	if err != nil {
		panic(err)
	}
	block := blockBuf[:n]
	assert.True(t, reflect.DeepEqual(block, blockToWrite))
	os.Remove(testFileName)
}

func TestWriteMultiple(t *testing.T) {
	testFileName := "somefile.bin"
	blockToWrite1 := []byte{34, 45, 23, 1}
	blockToWrite2 := []byte{1, 0, 2, 39, 15}

	out, err := os.Create(testFileName)
	defer out.Close()
	if err != nil {
		panic(err)
	}
	bufWriter := bufio.NewWriter(out)
	writer := NewDataBlockWriter(bufWriter)
	writer.Write(blockToWrite1)
	writer.Write(blockToWrite2)
	bufWriter.Flush()

	out.Close()

	inp, err := os.Open(testFileName)
	defer inp.Close()
	bufReader := bufio.NewReader(inp)
	reader := NewDataBlockReader(bufReader)
	blockBuf := make([]byte, 1024)
	n, err := reader.Read(blockBuf)
	if err != nil {
		panic(err)
	}
	block := blockBuf[:n]
	assert.True(t, reflect.DeepEqual(block, blockToWrite1))
	n, err = reader.Read(blockBuf)
	if err != nil {
		panic(err)
	}
	block = blockBuf[:n]
	assert.True(t, reflect.DeepEqual(block, blockToWrite2))
	os.Remove(testFileName)
}
