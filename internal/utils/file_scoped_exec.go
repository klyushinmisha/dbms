package utils

import (
	"log"
	"os"
)

func FileScopedExec(name string, exec func(*os.File) error) error {
	file, err := os.Create(name)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			log.Panic(closeErr)
		}
		removeErr := os.Remove(name)
		if removeErr != nil {
			log.Panic(removeErr)
		}
	}()
	return exec(file)
}
