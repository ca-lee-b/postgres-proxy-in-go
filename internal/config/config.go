// Package config implements the config structure
package config

import (
	"encoding/json"
	"os"
)

// BackendConfig is an individual postgres server
type BackendConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type Config struct {
	ListenAddr     string          `json:"listen_addr"`
	MetricsAddr    string          `json:"metrics_addr"`
	PoolSize       int             `json:"pool_size"`
	Primary        BackendConfig   `json:"primary"`
	Replicas       []BackendConfig `json:"replicas"`
	HealthInterval int             `json:"health_interval_seconds"`
}

// Config is a json file
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	config := Default()

	if err := json.NewDecoder(f).Decode(config); err != nil {
		return nil, err
	}

	return config, nil
}

func Default() *Config {
	return &Config{
		ListenAddr:  "0.0.0.0:5433",
		MetricsAddr: ":9090",
		PoolSize:    10,
		Primary: BackendConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		},
		HealthInterval: 5,
	}
}
