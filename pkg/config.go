package pkg

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
)

type CoreConfig struct {
	PageSize  int    `json:"pageSize"`
	BufCap    int    `json:"bufferCapacity"`
	FilesPath string `json:"filesPath"`
}

func (c *CoreConfig) DataPath() string {
	return filepath.Join(c.FilesPath, "data.bin")
}

func (c *CoreConfig) LogPath() string {
	return filepath.Join(c.FilesPath, "log.bin")
}

type ServerConfig struct {
	TransportProtocol string `json:"transportProtocol"`
	Port              int    `json:"port"`
	MaxConnections    int    `json:"maxConnections"`
}

type config struct {
	CoreConfig
	ServerConfig
}

type ConfigLoader interface {
	CoreCfg() *CoreConfig
	SrvCfg() *ServerConfig
	Load()
}

type DefaultConfigLoader struct {
	cfg *config
}

func (l *DefaultConfigLoader) CoreCfg() *CoreConfig {
	return &l.cfg.CoreConfig
}

func (l *DefaultConfigLoader) SrvCfg() *ServerConfig {
	return &l.cfg.ServerConfig
}

func (l *DefaultConfigLoader) Load() {
	l.cfg = &config{
		CoreConfig{
			PageSize:  8192,
			BufCap:    4096,
			FilesPath: ".",
		},
		ServerConfig{
			TransportProtocol: "tcp",
			Port:              8080,
			MaxConnections:    100,
		},
	}
}

type JSONConfigLoader struct {
	cfgFilePath string
	cfg         *config
}

func NewJSONConfigLoader(cfgFilePath string) *JSONConfigLoader {
	l := new(JSONConfigLoader)
	l.cfgFilePath = cfgFilePath
	return l
}

func (l *JSONConfigLoader) CoreCfg() *CoreConfig {
	return &l.cfg.CoreConfig
}

func (l *JSONConfigLoader) SrvCfg() *ServerConfig {
	return &l.cfg.ServerConfig
}

func (l *JSONConfigLoader) Load() {
	data, err := ioutil.ReadFile(l.cfgFilePath)
	if err != nil {
		log.Panic(err)
	}
	l.cfg = new(config)
	if err := json.Unmarshal(data, l.cfg); err != nil {
		log.Panic(err)
	}
}
