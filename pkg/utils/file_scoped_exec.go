package utils

import (
	"log"
	"os"
)

func FileScopedExec(name string, exec func(*os.File) error) error {
	file, err := os.Create(name)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			log.Fatalln(closeErr)
		}
		removeErr := os.Remove(name)
		if removeErr != nil {
			log.Fatalln(removeErr)
		}
	}()
	return exec(file)
}
