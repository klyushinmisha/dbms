package pkg

import (
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	fileContext("somefile1.bin", func(bitsetFile *os.File) error {
		fileContext("somefile2.bin", func(file *os.File) error {
			var pageSize = int64(os.Getpagesize())
			var bitset = NewPageCachingBitset(bitsetFile, pageSize)
			var segWriter = NewSegmentWriter(file, bitset, pageSize)
			var writeErr error
			for i := 0; i < 100; i++ {
				_, _, writeErr = segWriter.Write([]byte("HELLO WORLD"))
			}
			return writeErr
		})
		return nil
	})
}
