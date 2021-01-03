package pkg

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	testFileName := "somefile.bin"

	out, err := os.Create(testFileName)
	defer out.Close()
	if err != nil {
		panic(err)
	}
	inp, err := os.Open(testFileName)
	defer inp.Close()
	store := NewDiskStore(inp, out)
	store.Set("HELLO", []byte("WORLD"))
	data, err := store.Get("HELLO")
	assert.Equal(t, "WORLD", string(data))
	os.Remove(testFileName)
}
