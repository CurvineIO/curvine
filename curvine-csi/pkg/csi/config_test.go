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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func wantDefaultConfig() Config {
	return Config{
		CurvineCliPath:      "/opt/curvine/curvine-cli",
		FuseBinaryPath:      "/opt/curvine/curvine-fuse",
		RetryCount:          3,
		RetryInterval:       2,
		CommandTimeout:      30,
		KubernetesNamespace: "curvine",
	}
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func TestDefaultConfig(t *testing.T) {
	if got, want := *DefaultConfig(), wantDefaultConfig(); got != want {
		t.Fatalf("DefaultConfig() = %+v, want %+v", got, want)
	}
}

func TestDefaultConfigReturnsIndependentValues(t *testing.T) {
	first := DefaultConfig()
	first.RetryCount = 99
	if got := DefaultConfig().RetryCount; got != 3 {
		t.Fatalf("DefaultConfig() shares state between calls: RetryCount = %d, want 3", got)
	}
}

func TestLoadConfigFallsBackToDefaults(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "empty path", path: ""},
		{name: "missing file", path: filepath.Join(t.TempDir(), "missing.json")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KUBERNETES_NAMESPACE", "")
			cfg, err := LoadConfig(tt.path)
			if err != nil {
				t.Fatalf("LoadConfig(%q) error = %v", tt.path, err)
			}
			if got, want := *cfg, wantDefaultConfig(); got != want {
				t.Fatalf("LoadConfig(%q) = %+v, want %+v", tt.path, got, want)
			}
		})
	}
}

func TestLoadConfigMergesFileOverDefaults(t *testing.T) {
	t.Setenv("KUBERNETES_NAMESPACE", "")
	path := writeConfigFile(t, `{"curvineCliPath": "/usr/local/bin/curvine-cli", "retryCount": 5, "kubernetesNamespace": "storage"}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	want := wantDefaultConfig()
	want.CurvineCliPath = "/usr/local/bin/curvine-cli"
	want.RetryCount = 5
	want.KubernetesNamespace = "storage"
	if *cfg != want {
		t.Fatalf("LoadConfig() = %+v, want %+v", *cfg, want)
	}
}

func TestLoadConfigRejectsMalformedJSON(t *testing.T) {
	t.Setenv("KUBERNETES_NAMESPACE", "")
	tests := []struct {
		name    string
		content string
	}{
		{name: "truncated object", content: `{"retryCount": 5`},
		{name: "wrong field type", content: `{"retryCount": "five"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeConfigFile(t, tt.content))
			if err == nil {
				t.Fatalf("LoadConfig() = %+v, want error", cfg)
			}
			if !strings.Contains(err.Error(), "failed to decode config file") {
				t.Fatalf("LoadConfig() error = %q, want it to mention decoding", err)
			}
			if cfg != nil {
				t.Fatalf("LoadConfig() returned config %+v alongside error", cfg)
			}
		})
	}
}

func TestLoadConfigNamespaceFromEnvironment(t *testing.T) {
	tests := []struct {
		name string
		path func(t *testing.T) string
	}{
		{name: "overrides default", path: func(*testing.T) string { return "" }},
		{name: "overrides missing file", path: func(t *testing.T) string { return filepath.Join(t.TempDir(), "missing.json") }},
		{name: "overrides file", path: func(t *testing.T) string {
			return writeConfigFile(t, `{"kubernetesNamespace": "from-file", "retryCount": 7}`)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KUBERNETES_NAMESPACE", "from-env")
			cfg, err := LoadConfig(tt.path(t))
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if cfg.KubernetesNamespace != "from-env" {
				t.Fatalf("KubernetesNamespace = %q, want %q", cfg.KubernetesNamespace, "from-env")
			}
		})
	}

	// The file's other fields must survive the namespace override.
	t.Run("keeps other file fields", func(t *testing.T) {
		t.Setenv("KUBERNETES_NAMESPACE", "from-env")
		cfg, err := LoadConfig(writeConfigFile(t, `{"kubernetesNamespace": "from-file", "retryCount": 7}`))
		if err != nil {
			t.Fatalf("LoadConfig() error = %v", err)
		}
		if cfg.RetryCount != 7 {
			t.Fatalf("RetryCount = %d, want 7", cfg.RetryCount)
		}
	})
}

func TestLoadConfigReturnsNonMissingOpenError(t *testing.T) {
	t.Setenv("KUBERNETES_NAMESPACE", "")
	// A NUL byte makes os.Open fail with EINVAL on every platform, without
	// depending on file permissions or the user running the test.
	cfg, err := LoadConfig("config\x00.json")
	if err == nil {
		t.Fatalf("LoadConfig() = %+v, want error", cfg)
	}
	if !strings.Contains(err.Error(), "failed to open config file") {
		t.Fatalf("LoadConfig() error = %q, want it to mention opening the file", err)
	}
	if cfg != nil {
		t.Fatalf("LoadConfig() returned config %+v alongside error", cfg)
	}
}
