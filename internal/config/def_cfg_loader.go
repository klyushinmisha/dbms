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

const (
	KB = 1024
	MB = KB * KB
)

func (l *DefaultConfigLoader) Load() {
	l.cfg = &config{
		CoreConfig{
			PageSize:  8 * KB,
			BufCap:    4 * KB,
			FilesPath: ".",
			LogSegCap: 1 * MB,
		},
		ServerConfig{
			TransportProtocol: "tcp",
			Port:              8080,
			MaxConnections:    100,
		},
	}
}
