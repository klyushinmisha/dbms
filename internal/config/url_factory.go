package config

type ServerUrlFactory interface {
	BuildUrl() string
}
