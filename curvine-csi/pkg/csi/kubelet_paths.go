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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog"
)

const (
	// EnvFuseStagingBaseDir overrides where the per-mount FUSE staging
	// directories are created (embedded mode).
	//
	// The value is the directory that contains <mount-key>/fuse-mount, as seen
	// from the csi-plugin container. It must be the same directory kubelet calls
	// NodeStageVolume with, which is <kubelet-root>/plugins/kubernetes.io/csi/curvine.
	EnvFuseStagingBaseDir = "FUSE_STAGING_BASE_DIR"

	// DefaultFuseStagingBaseDir is the staging base for a kubelet root of
	// /var/lib/kubelet, the layout used by kubeadm clusters.
	DefaultFuseStagingBaseDir = "/var/lib/kubelet/plugins/kubernetes.io/csi/curvine"

	// EnvStandaloneHostMountBaseDir overrides the host directory that backs the
	// per-mount FUSE pod (standalone mode).
	//
	// The value is the directory that contains <mount-key>/fuse-mount on the
	// host, i.e. <host-kubelet-root>/plugins/curvine, and it must be visible at
	// the same path inside the csi-plugin container, because the driver binds
	// from there into kubelet's pod volume paths.
	EnvStandaloneHostMountBaseDir = "STANDALONE_HOST_MOUNT_BASE_DIR"

	// DefaultStandaloneHostMountBaseDir is the standalone host mount base for a
	// kubelet root of /var/lib/kubelet.
	DefaultStandaloneHostMountBaseDir = "/var/lib/kubelet/plugins/curvine"
)

// resolveEnv returns the trimmed value of env, or def when it is unset or blank.
func resolveEnv(env, def string) string {
	if value := strings.TrimSpace(os.Getenv(env)); value != "" {
		return value
	}
	return def
}

// resolvePathEnv is resolveEnv for filesystem paths. The value must be absolute:
// a relative base would place the FUSE mount under the container's working
// directory, where kubelet never looks, and the node would serve dead mounts
// after the next plugin restart. ValidateKubeletPathEnv rejects it at startup;
// the fallback here keeps a bad value from reaching a mount in any other use.
func resolvePathEnv(env, def string) string {
	value := resolveEnv(env, def)
	if !filepath.IsAbs(value) {
		klog.Errorf("%s must be an absolute path, got %q: using %q", env, value, def)
		return def
	}
	return filepath.Clean(value)
}

// ValidateKubeletPathEnv rejects a kubelet path override that is not absolute.
// Both overrides are read when the node plugin starts, so a bad value fails the
// plugin instead of producing mounts outside the kubelet root.
func ValidateKubeletPathEnv() error {
	for _, env := range []string{EnvFuseStagingBaseDir, EnvStandaloneHostMountBaseDir} {
		value := strings.TrimSpace(os.Getenv(env))
		if value != "" && !filepath.IsAbs(value) {
			return fmt.Errorf("%s must be an absolute path, got %q", env, value)
		}
	}
	return nil
}

// ResolveFuseStagingBaseDir returns the parent directory of the per-mount FUSE
// staging dirs used in embedded mode.
func ResolveFuseStagingBaseDir() string {
	return resolvePathEnv(EnvFuseStagingBaseDir, DefaultFuseStagingBaseDir)
}

// ResolveStandaloneHostMountBaseDir returns the host directory that backs the
// per-mount FUSE pod used in standalone mode.
func ResolveStandaloneHostMountBaseDir() string {
	return resolvePathEnv(EnvStandaloneHostMountBaseDir, DefaultStandaloneHostMountBaseDir)
}
