// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package csi

import (
	"encoding/json"
	"fmt"
	"os"
)


// Config stores CSI driver configuration
type Config struct {
	// CurvineCliPath Curvine CLI tool path
	CurvineCliPath string `json:"curvineCliPath"`
	// FuseBinaryPath Curvine FUSE binary file path
	FuseBinaryPath string `json:"fuseBinaryPath"`
	// RetryCount number of operation retries
	RetryCount int `json:"retryCount"`
	// RetryInterval retry interval time (seconds)
	RetryInterval int `json:"retryInterval"`
	// CommandTimeout command execution timeout (seconds)
	CommandTimeout int `json:"commandTimeout"`

	// ConfigFile configuration file path
	ConfigFile string `json:"configFile"`
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		CurvineCliPath: "/opt/curvine/bin/cv",
		FuseBinaryPath: "/opt/curvine/bin/curvine-fuse",
		RetryCount:     3,
		RetryInterval:  2,
		CommandTimeout: 30,
		ConfigFile:     "/opt/curvine/conf/curvine-cluster.toml",
	}
}

// LoadConfig loads configuration from file
func LoadConfig(configPath string) (*Config, error) {
	config := DefaultConfig()

	// If config file path is empty, use default configuration
	if configPath == "" {
		return loadFromEnv(config), nil
	}

	// Try to load configuration from file
	file, err := os.Open(configPath)
	if err != nil {
		// If file doesn't exist, use default configuration
		if os.IsNotExist(err) {
			return loadFromEnv(config), nil
		}
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %v", err)
	}

	// Load configuration from environment variables (env vars have higher priority than file config)
	return loadFromEnv(config), nil
}

// loadFromEnv loads configuration from environment variables
func loadFromEnv(config *Config) *Config {
	if path := os.Getenv("CURVINE_CLI_PATH"); path != "" {
		config.CurvineCliPath = path
	}

	if path := os.Getenv("CURVINE_FUSE_PATH"); path != "" {
		config.FuseBinaryPath = path
	}

	if retryCount := os.Getenv("CURVINE_RETRY_COUNT"); retryCount != "" {
		var count int
		if _, err := fmt.Sscanf(retryCount, "%d", &count); err == nil && count > 0 {
			config.RetryCount = count
		}
	}

	if retryInterval := os.Getenv("CURVINE_RETRY_INTERVAL"); retryInterval != "" {
		var interval int
		if _, err := fmt.Sscanf(retryInterval, "%d", &interval); err == nil && interval > 0 {
			config.RetryInterval = interval
		}
	}

	if commandTimeout := os.Getenv("CURVINE_COMMAND_TIMEOUT"); commandTimeout != "" {
		var timeout int
		if _, err := fmt.Sscanf(commandTimeout, "%d", &timeout); err == nil && timeout > 0 {
			config.CommandTimeout = timeout
		}
	}

	return config
}
