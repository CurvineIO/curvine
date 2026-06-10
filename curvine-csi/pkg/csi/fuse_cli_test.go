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

func TestComputeFuseMntPath(t *testing.T) {
	mountKey := "abc123"
	want := "/var/lib/kubelet/plugins/kubernetes.io/csi/curvine/abc123/fuse-mount"
	if got := ComputeFuseMntPath(mountKey); got != want {
		t.Fatalf("ComputeFuseMntPath() = %q, want %q", got, want)
	}
}

func TestCollectPassthroughParams(t *testing.T) {
	volumeContext := map[string]string{
		"master-addrs":      "m1:8995",
		"io-threads":        "4",
		"client.block-size": "65536",
		"mnt-path":          "/should-not-appear",
	}
	publishContext := map[string]string{
		"entry-timeout": "1.0",
		"io-threads":      "8",
	}

	got := CollectPassthroughParams(volumeContext, publishContext)
	want := map[string]string{
		"io-threads":        "4",
		"entry-timeout":     "1.0",
		"client.block-size": "65536",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CollectPassthroughParams() = %#v, want %#v", got, want)
	}
}

func TestBuildFuseExecArgs(t *testing.T) {
	in := FuseExecArgsInput{
		Subcommand:  "validate-config",
		ConfPath:    "/etc/curvine/cluster.toml",
		MasterAddrs: "m1:8995",
		FSPath:      "/data",
		MountKey:    "mk1",
		Passthrough: map[string]string{
			"entry-timeout":     "1.0",
			"client.block-size": "65536",
			"io-threads":        "4",
		},
	}

	got := BuildFuseExecArgs(in)
	want := []string{
		"validate-config",
		"--conf", "/etc/curvine/cluster.toml",
		"--master-addrs", "m1:8995",
		"--fs-path", "/data",
		"--mnt-path", ComputeFuseMntPath("mk1"),
		"--client.block-size", "65536",
		"--entry-timeout", "1.0",
		"--io-threads", "4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuildFuseExecArgs() = %#v, want %#v", got, want)
	}
}

func TestBuildFuseExecArgsExplicitMntPath(t *testing.T) {
	explicit := "/custom/mnt/path"
	in := FuseExecArgsInput{
		MountKey: "ignored-key",
		MntPath:  explicit,
		Passthrough: map[string]string{
			"io-threads": "4",
		},
	}

	got := BuildFuseExecArgs(in)
	want := []string{
		"--mnt-path", explicit,
		"--io-threads", "4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BuildFuseExecArgs() = %#v, want %#v", got, want)
	}
}

func TestIsRejectedVolumeParameterKey(t *testing.T) {
	if !IsRejectedVolumeParameterKey("mnt-path") {
		t.Fatal("expected mnt-path to be rejected")
	}
	if IsRejectedVolumeParameterKey("io-threads") {
		t.Fatal("did not expect io-threads to be rejected")
	}
}

func TestIsDeniedVolumeParameterKeyKubernetesInternal(t *testing.T) {
	if !IsDeniedVolumeParameterKey("storage.kubernetes.io/csiProvisionerIdentity") {
		t.Fatal("expected storage.kubernetes.io/csiProvisionerIdentity to be denied")
	}
	if !IsDeniedVolumeParameterKey("csi.storage.k8s.io/pod.name") {
		t.Fatal("expected csi.storage.k8s.io/pod.name to be denied")
	}
	if IsDeniedVolumeParameterKey("io-threads") {
		t.Fatal("did not expect io-threads to be denied")
	}
}

func TestCollectPassthroughParamsFiltersProvisionerIdentity(t *testing.T) {
	volumeContext := map[string]string{
		"io-threads": "4",
		"storage.kubernetes.io/csiProvisionerIdentity": "1781067593753-8639-curvine",
	}
	got := CollectPassthroughParams(volumeContext, nil)
	if len(got) != 1 || got["io-threads"] != "4" {
		t.Fatalf("CollectPassthroughParams() = %#v, want only io-threads=4", got)
	}
}

func TestRejectDisallowedVolumeParameters(t *testing.T) {
	volumeContext := map[string]string{
		"master-addrs": "m1:8995",
		"mnt-path":     "/custom",
	}
	if err := RejectDisallowedVolumeParameters(volumeContext, nil, "req"); err == nil {
		t.Fatal("expected error for mnt-path in volume context")
	}
}
