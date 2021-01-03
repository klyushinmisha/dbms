package pkg

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func fillWithBytes(writer *PageWriter, b byte, n int) {
	blockToWrite := make([]byte, n)
	for i := 0; i < n; i++ {
		blockToWrite[i] = b
	}
	bytesWritten, err := writer.Write(blockToWrite)
	if err != nil {
		log.Fatalln(err)
	}
	if n != bytesWritten {
		log.Fatalf("Failed to write %d bytes", n)
	}
}

func TestPageWriter(t *testing.T) {
	testFileName := "somefile.bin"
	out, err := os.Create(testFileName)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		closeErr := out.Close()
		if closeErr != nil {
			log.Fatalln(closeErr)
		}
		removeErr := os.Remove(testFileName)
		if removeErr != nil {
			log.Fatalln(removeErr)
		}
	}()

	pageSize := 16
	writer := NewPageWriter(out, pageSize)
	defer func() {
		flushErr := writer.Flush()
		if flushErr != nil {
			log.Fatalln(flushErr)
		}
	}()
	fillWithBytes(writer, 2, pageSize - 1)
	fillWithBytes(writer, 1, pageSize - 2)
	fillWithBytes(writer, 0xFF, 3)
}

func TestPageReader(t *testing.T) {
	// main purpose of this test is writing and reading some pages
	// to validate PageReader behaviour
	// this test relies on TestPageWriter

	testFileName := "somefile.bin"
	out, err := os.Create(testFileName)
	if err != nil {
		log.Fatalln(err)
	}

	defer func() {
		closeErr := out.Close()
		if closeErr != nil {
			log.Fatalln(err)
		}
		removeErr := os.Remove(testFileName)
		if removeErr != nil {
			log.Fatalln(removeErr)
		}
	}()

	pageSize := 16
	writer := NewPageWriter(out, pageSize)
	dataToWrite := [][2]int{
		{2, pageSize - 1},
		{1, pageSize - 2},
		{0xFF, 3},
	}
	for _, pair := range dataToWrite {
		fillWithBytes(writer, byte(pair[0]), pair[1])
	}
	flushErr := writer.Flush()
	if flushErr != nil {
		log.Fatalln(flushErr)
	}

	in, openErr := os.Open(testFileName)
	if openErr != nil {
		log.Fatalln(openErr)
	}
	defer func() {
		closeErr := in.Close()
		if closeErr != nil {
			log.Fatalln(err)
		}
	}()

	reader := NewPageReader(in, pageSize)
	for _, pair := range dataToWrite {
		bytesToReadCnt := pair[1]
		readData := make([]byte, bytesToReadCnt, bytesToReadCnt)
		_, err = reader.Read(readData)
		if err != nil {
			log.Fatalln(err)
		}
		for _, v := range readData {
			assert.Equal(t, int(v), pair[0])
		}
	}
}