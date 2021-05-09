package config

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
