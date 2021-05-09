package config

type ServerConfig struct {
	TransportProtocol string `json:"transportProtocol"`
	Port              int    `json:"port"`
	MaxConnections    int    `json:"maxConnections"`
}
