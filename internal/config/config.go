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
	ListenAddr  string `json:"listen_addr"`
	MetricsAddr string `json:"metrics_addr"`
	PoolSize    int    `json:"pool_size"`
	PoolMinSize int    `json:"pool_min_size"`
	// PoolMaxIdleSeconds — idle connections older than this may be closed by the reaper (if total > pool_min_size).
	PoolMaxIdleSeconds int `json:"pool_max_idle_seconds"`
	// PoolMaxLifeSeconds — connections older than this are recycled on checkout or by the reaper.
	PoolMaxLifeSeconds int `json:"pool_max_life_seconds"`
	// PoolAcquireTimeoutSeconds — max time to wait for a free connection (0 = wait until context only).
	PoolAcquireTimeoutSeconds int `json:"pool_acquire_timeout_seconds"`
	// PoolIdleCheckSeconds — how often the reaper runs (0 disables periodic reaping).
	PoolIdleCheckSeconds int             `json:"pool_idle_check_seconds"`
	Primary              BackendConfig   `json:"primary"`
	Replicas             []BackendConfig `json:"replicas"`
	HealthInterval       int             `json:"health_interval_seconds"`
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
		PoolMinSize: 0,
		// Defaults aligned with common pool settings; tune via JSON.
		PoolMaxIdleSeconds:        300,
		PoolMaxLifeSeconds:        3600,
		PoolAcquireTimeoutSeconds: 3,
		PoolIdleCheckSeconds:      30,
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
