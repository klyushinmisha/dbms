package pkg

import (
	"encoding/json"
	"log"
	"path/filepath"
)

type Config struct {
	FilesPath string `json:"filesPath"`
	PageSize  int    `json:"pageSize"`
	CacheSize int    `json:"cacheSize"`
}

func LoadConfig(data []byte) *Config {
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		log.Panic(err)
	}
	return &c
}

func (c *Config) IndexPath() string {
	return filepath.Join(c.FilesPath, "index.bin")
}

func (c *Config) DataPath() string {
	return filepath.Join(c.FilesPath, "data.bin")
}
