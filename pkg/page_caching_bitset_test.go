package pkg

import (
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func fileContext(filename string, testBody func(*os.File) error) {
	f, createErr := os.Create(filename)
	if createErr != nil {
		log.Fatalln(createErr)
	}
	defer func() {
		closeErr := f.Close()
		if closeErr != nil {
			log.Fatalln(closeErr)
		}
		return
		removeErr := os.Remove(filename)
		if removeErr != nil {
			log.Fatalln(removeErr)
		}
	}()
	bodyErr := testBody(f)
	if bodyErr != nil {
		log.Fatalln(bodyErr)
	}
}

func TestNewPageCachingBitset_MultipleChanges(t *testing.T) {
	fileContext("somefile.bin", func(file *os.File) error {
		var pageSize = int64(os.Getpagesize())
		bitset := NewPageCachingBitset(file, pageSize)

		var positions = []int64{
			0,
			pageSize,
			pageSize * pageSize,
			pageSize * (pageSize + 1),
			2 * pageSize,
			pageSize * (pageSize + 2),
		}
		var val bool
		var checkErr, setErr error
		for _, pos := range positions {
			val, checkErr = bitset.Check(pos)
			if checkErr != nil {
				return checkErr
			}
			assert.False(t, val)
		}
		for _, pos := range positions {
			setErr = bitset.Set(pos)
			if setErr != nil {
				return setErr
			}
		}
		for _, pos := range positions {
			val, checkErr = bitset.Check(pos)
			if checkErr != nil {
				return checkErr
			}
			assert.True(t, val)
		}
		for _, pos := range positions {
			setErr = bitset.Reset(pos)
			if setErr != nil {
				return setErr
			}
		}
		for _, pos := range positions {
			val, checkErr = bitset.Check(pos)
			if checkErr != nil {
				return checkErr
			}
			assert.False(t, val)
		}
		return nil
	})
}
