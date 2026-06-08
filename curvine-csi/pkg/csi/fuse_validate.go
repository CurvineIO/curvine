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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	defaultFuseConfPath = "/opt/curvine/conf/curvine-cluster.toml"
)

// ResolveFuseBinaryPath returns the curvine-fuse binary path from env or driver defaults.
func ResolveFuseBinaryPath() string {
	if path := os.Getenv("FUSE_BINARY_PATH"); path != "" {
		return path
	}
	return DefaultConfig().FuseBinaryPath
}

// ResolveFuseConfPath returns the cluster config path passed to curvine-fuse.
func ResolveFuseConfPath() string {
	if path := os.Getenv("FUSE_CONF_PATH"); path != "" {
		return path
	}
	return defaultFuseConfPath
}

// IsStaticVolumeID reports whether the volume ID does not use the dynamic
// {cluster-id}@{fs-path}@{pv-name} format produced by CreateVolume.
func IsStaticVolumeID(volumeID string) bool {
	_, err := ParseVolumeHandle(volumeID)
	return err != nil
}

// ValidateFuseParameters runs curvine-fuse validate-config with the same argv
// shape used for mount, minus mnt-path (not required for config dry-run).
func ValidateFuseParameters(ctx context.Context, masterAddrs, fsPath string, passthrough map[string]string) error {
	if fsPath == "" {
		fsPath = "/"
	}
	return ExecFuseValidateConfig(ctx, FuseExecArgsInput{
		ConfPath:    ResolveFuseConfPath(),
		MasterAddrs: masterAddrs,
		FSPath:      fsPath,
		Passthrough: passthrough,
	})
}

// ExecFuseValidateConfig executes curvine-fuse validate-config and returns stderr on failure.
func ExecFuseValidateConfig(ctx context.Context, in FuseExecArgsInput) error {
	in.Subcommand = "validate-config"
	if in.ConfPath == "" {
		in.ConfPath = ResolveFuseConfPath()
	}

	args := BuildFuseExecArgs(in)
	cmd := exec.CommandContext(ctx, ResolveFuseBinaryPath(), args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// fuseValidateTimeout returns the timeout for validate-config subprocess calls.
func fuseValidateTimeout() time.Duration {
	seconds := DefaultConfig().CommandTimeout
	if seconds <= 0 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}
