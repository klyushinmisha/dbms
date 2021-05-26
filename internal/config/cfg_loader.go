package config

type ConfigLoader interface {
	CoreCfg() *CoreConfig
	SrvCfg() *ServerConfig
	Load()
}
