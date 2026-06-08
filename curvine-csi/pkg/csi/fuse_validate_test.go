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
	"reflect"
	"testing"
)

func TestBuildFuseValidateExecArgs(t *testing.T) {
	args := BuildFuseExecArgs(FuseExecArgsInput{
		Subcommand:  "validate-config",
		ConfPath:    "/opt/curvine/conf/curvine-cluster.toml",
		MasterAddrs: "m1:8995",
		FSPath:      "/data",
		Passthrough: map[string]string{
			"io-threads":        "4",
			"client.block-size": "128MB",
		},
	})
	want := []string{
		"validate-config",
		"--conf", "/opt/curvine/conf/curvine-cluster.toml",
		"--master-addrs", "m1:8995",
		"--fs-path", "/data",
		"--client.block-size", "128MB",
		"--io-threads", "4",
	}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("BuildFuseExecArgs() = %#v, want %#v", args, want)
	}
}

func TestResolveFuseBinaryPathDefaults(t *testing.T) {
	t.Setenv("FUSE_BINARY_PATH", "")
	if got := ResolveFuseBinaryPath(); got != DefaultConfig().FuseBinaryPath {
		t.Fatalf("ResolveFuseBinaryPath() = %q, want default %q", got, DefaultConfig().FuseBinaryPath)
	}
}

func TestResolveFuseConfPathEnvOverride(t *testing.T) {
	t.Setenv("FUSE_CONF_PATH", "/custom/conf.toml")
	if got := ResolveFuseConfPath(); got != "/custom/conf.toml" {
		t.Fatalf("ResolveFuseConfPath() = %q, want env override", got)
	}
}
