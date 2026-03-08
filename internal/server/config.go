package server

import (
	commonconfig "github.com/openjobspec/ojs-go-backend-common/config"
)

// Config holds server configuration from environment variables.
type Config struct {
	commonconfig.BaseConfig

	// AMQP connection
	AMQPURL string
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	base := commonconfig.LoadBaseConfig()

	return Config{
		BaseConfig: base,
		AMQPURL:    commonconfig.GetEnv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
	}
}
