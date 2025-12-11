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

	// KubernetesNamespace namespace for storing mount state Secret
	KubernetesNamespace string `json:"kubernetesNamespace"`
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		CurvineCliPath:      "/opt/curvine/curvine-cli",
		FuseBinaryPath:      "/opt/curvine/curvine-fuse",
		RetryCount:          3,
		RetryInterval:       2,
		CommandTimeout:      30,
		KubernetesNamespace: "curvine-system",
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
	if namespace := os.Getenv("KUBERNETES_NAMESPACE"); namespace != "" {
		config.KubernetesNamespace = namespace
	}

	return config
}
