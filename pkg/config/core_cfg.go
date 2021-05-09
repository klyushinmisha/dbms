package config

import (
	"log"
	"path/filepath"
)

type CoreConfig struct {
	PageSize  int    `json:"pageSize"`
	BufCap    int    `json:"bufferCapacity"`
	FilesPath string `json:"filesPath"`
}

func (c *CoreConfig) absFilesPath() string {
	p, err := filepath.Abs(c.FilesPath)
	if err != nil {
		log.Panic(err)
	}
	return p
}

func (c *CoreConfig) DataPath() string {
	return filepath.Join(c.absFilesPath(), "data.bin")
}

func (c *CoreConfig) LogPath() string {
	return filepath.Join(c.absFilesPath(), "log.bin")
}
