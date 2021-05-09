package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

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
