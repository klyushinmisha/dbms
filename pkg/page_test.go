package pkg

import (
	"log"
	"os"
	"testing"
)

func TestPage_Write(t *testing.T) {
	execErr := fileScopedExec("somefile.bin", func(file *os.File) error {
		diskIo := MakeDiskIO(file, nil, os.Getpagesize())
		page := AllocatePage(os.Getpagesize())
		diskIo.WritePage(0, page)
		page = diskIo.ReadPage(0)
		log.Print(page)
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
