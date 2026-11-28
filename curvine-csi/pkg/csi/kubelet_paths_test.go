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
	"path/filepath"
	"testing"
)

func TestResolveFuseStagingBaseDir(t *testing.T) {
	t.Run("defaults when unset", func(t *testing.T) {
		t.Setenv(EnvFuseStagingBaseDir, "")
		if got := ResolveFuseStagingBaseDir(); got != DefaultFuseStagingBaseDir {
			t.Fatalf("got %q, want %q", got, DefaultFuseStagingBaseDir)
		}
	})

	t.Run("falls back for a relative value", func(t *testing.T) {
		t.Setenv(EnvFuseStagingBaseDir, "plugins/kubernetes.io/csi/curvine")
		if got := ResolveFuseStagingBaseDir(); got != DefaultFuseStagingBaseDir {
			t.Fatalf("got %q, want the default %q", got, DefaultFuseStagingBaseDir)
		}
	})

	t.Run("honours the override and cleans it", func(t *testing.T) {
		t.Setenv(EnvFuseStagingBaseDir, "  /var/lib/k0s/kubelet/plugins/kubernetes.io/csi/curvine/  ")
		want := "/var/lib/k0s/kubelet/plugins/kubernetes.io/csi/curvine"
		if got := ResolveFuseStagingBaseDir(); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

func TestResolveStandaloneHostMountBaseDir(t *testing.T) {
	t.Run("defaults when unset", func(t *testing.T) {
		t.Setenv(EnvStandaloneHostMountBaseDir, " ")
		if got := ResolveStandaloneHostMountBaseDir(); got != DefaultStandaloneHostMountBaseDir {
			t.Fatalf("got %q, want %q", got, DefaultStandaloneHostMountBaseDir)
		}
	})

	t.Run("honours the override", func(t *testing.T) {
		t.Setenv(EnvStandaloneHostMountBaseDir, "/var/lib/k0s/kubelet/plugins/curvine")
		want := "/var/lib/k0s/kubelet/plugins/curvine"
		if got := ResolveStandaloneHostMountBaseDir(); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

func TestComputeFuseMntPathUsesStagingBaseOverride(t *testing.T) {
	t.Setenv(EnvFuseStagingBaseDir, "/var/lib/k0s/kubelet/plugins/kubernetes.io/csi/curvine")
	got := ComputeFuseMntPath("abc123")
	want := "/var/lib/k0s/kubelet/plugins/kubernetes.io/csi/curvine/abc123/fuse-mount"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if !filepath.IsAbs(got) {
		t.Fatalf("want an absolute path, got %q", got)
	}
}

func TestStandaloneGetHostMountPathUsesOverride(t *testing.T) {
	t.Setenv(EnvStandaloneHostMountBaseDir, "/var/lib/k0s/kubelet/plugins/curvine")
	m := &standaloneMountManagerImpl{}
	got := m.GetHostMountPath("abc123")
	want := "/var/lib/k0s/kubelet/plugins/curvine/abc123/fuse-mount"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStandalonePodNamesDifferForSimilarNodeNames(t *testing.T) {
	mountKey := "e67370c784fc022e4894f20ff99209ca"
	seen := map[string]string{}
	for _, node := range []string{"fpt-h200-gpu-17", "fpt-h200-gpu-39", "fpt-h200-gpu-48"} {
		m := &standaloneMountManagerImpl{nodeName: node}
		name := m.getStandaloneName(mountKey)
		if other, ok := seen[name]; ok {
			t.Fatalf("node %s and %s share pod name %s", node, other, name)
		}
		seen[name] = node
	}
}

func TestValidateKubeletPathEnv(t *testing.T) {
	t.Run("accepts unset and absolute values", func(t *testing.T) {
		t.Setenv(EnvFuseStagingBaseDir, "")
		t.Setenv(EnvStandaloneHostMountBaseDir, "/var/lib/k0s/kubelet/plugins/curvine")
		if err := ValidateKubeletPathEnv(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	for _, env := range []string{EnvFuseStagingBaseDir, EnvStandaloneHostMountBaseDir} {
		t.Run("rejects a relative "+env, func(t *testing.T) {
			t.Setenv(EnvFuseStagingBaseDir, "")
			t.Setenv(EnvStandaloneHostMountBaseDir, "")
			t.Setenv(env, "plugins/curvine")
			if err := ValidateKubeletPathEnv(); err == nil {
				t.Fatalf("want an error for a relative %s", env)
			}
		})
	}
}
