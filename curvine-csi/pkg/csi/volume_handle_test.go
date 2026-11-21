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
	"regexp"
	"strings"
	"testing"
)

// These tests cover the pure string helpers that build and parse the volume
// handle. The handle format, the segment count and the sorted order of the
// FUSE parameters are part of the on-cluster contract, so the assertions here
// are intentionally exact rather than approximate.

var (
	clusterIDPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
	mountKeyPattern  = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

func TestGenerateClusterIDIsStableHexPrefix(t *testing.T) {
	got := GenerateClusterID("m1:8995")
	if got != GenerateClusterID("m1:8995") {
		t.Fatalf("GenerateClusterID is not stable across calls: %q != %q", got, GenerateClusterID("m1:8995"))
	}
	if !clusterIDPattern.MatchString(got) {
		t.Fatalf("cluster ID %q is not 8 lowercase hex characters", got)
	}
}

func TestGenerateClusterIDDiffersForDifferentMasterAddrs(t *testing.T) {
	cases := []struct {
		name  string
		left  string
		right string
	}{
		{"different hosts", "m1:8995", "m2:8995"},
		{"different ports", "m1:8995", "m1:8996"},
		{"different address lists", "m1:8995", "m1:8995,m2:8995"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if GenerateClusterID(tc.left) == GenerateClusterID(tc.right) {
				t.Fatalf("cluster ID collides for %q and %q", tc.left, tc.right)
			}
		})
	}
}

func TestGenerateMountKeyIsStableHexPrefix(t *testing.T) {
	got := GenerateMountKey("m1:8995", "/data")
	if got != GenerateMountKey("m1:8995", "/data") {
		t.Fatalf("GenerateMountKey is not stable across calls: %q != %q", got, GenerateMountKey("m1:8995", "/data"))
	}
	if !mountKeyPattern.MatchString(got) {
		t.Fatalf("mount key %q is not 32 lowercase hex characters", got)
	}
}

func TestGenerateMountKeyDiffersPerClusterAndFSPath(t *testing.T) {
	base := GenerateMountKey("m1:8995", "/data")
	cases := []struct {
		name        string
		masterAddrs string
		fsPath      string
	}{
		{"different fs-path", "m1:8995", "/other"},
		{"different master-addrs", "m1:8996", "/data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GenerateMountKey(tc.masterAddrs, tc.fsPath)
			if got == base {
				t.Fatalf("mount key %q collides with the base key for %q/%q", got, tc.masterAddrs, tc.fsPath)
			}
			if !mountKeyPattern.MatchString(got) {
				t.Fatalf("mount key %q is not 32 lowercase hex characters", got)
			}
		})
	}
}

func TestGenerateMountKeyWithFuseParamsFallsBackWithoutParams(t *testing.T) {
	base := GenerateMountKey("m1:8995", "/data")
	for _, tc := range []struct {
		name   string
		params map[string]string
	}{
		{"nil map", nil},
		{"empty map", map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := GenerateMountKeyWithFuseParams("m1:8995", "/data", tc.params); got != base {
				t.Fatalf("key with %s = %q, want the legacy key %q", tc.name, got, base)
			}
		})
	}
}

func TestVolumeHandleHelpersMatchGoldenValues(t *testing.T) {
	// Golden values pin the exact hash inputs. Changing the separator, the hash
	// algorithm, the hex prefix length or the order in which FUSE parameters are
	// serialised changes the generated keys, renames standalone mount paths for
	// existing StorageClasses, or makes two different parameter sets collide.
	cases := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "cluster ID for m1:8995",
			got:  GenerateClusterID("m1:8995"),
			want: "72b342c5",
		},
		{
			name: "mount key for m1:8995 and /data",
			got:  GenerateMountKey("m1:8995", "/data"),
			want: "4121a1be87305106d02a1ca9281c76dd",
		},
		{
			name: "mount key with attr-timeout=1 and entry-timeout=1",
			got: GenerateMountKeyWithFuseParams("m1:8995", "/data", map[string]string{
				"entry-timeout": "1",
				"attr-timeout":  "1",
			}),
			want: "158b318984efafd5725888453782cb09",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("%s = %q, want %q", tc.name, tc.got, tc.want)
			}
		})
	}
}

func TestGenerateMountKeyWithFuseParamsSeparatesParamSets(t *testing.T) {
	base := GenerateMountKey("m1:8995", "/data")
	withTimeouts := GenerateMountKeyWithFuseParams("m1:8995", "/data", map[string]string{
		"entry-timeout": "1",
		"attr-timeout":  "1",
	})
	if withTimeouts == base {
		t.Fatalf("key with FUSE params equals the legacy key %q", base)
	}
	if !mountKeyPattern.MatchString(withTimeouts) {
		t.Fatalf("mount key with FUSE params %q is not 32 lowercase hex characters", withTimeouts)
	}

	// A different parameter value must produce a different mount key, otherwise
	// two StorageClasses would share one FUSE pod with different timeouts.
	if got := GenerateMountKeyWithFuseParams("m1:8995", "/data", map[string]string{
		"entry-timeout": "5",
		"attr-timeout":  "1",
	}); got == withTimeouts {
		t.Fatalf("mount key does not change when entry-timeout changes: %q", got)
	}

	// The cluster endpoint and fs-path still participate in the key.
	if got := GenerateMountKeyWithFuseParams("m1:8995", "/other", map[string]string{
		"entry-timeout": "1",
		"attr-timeout":  "1",
	}); got == withTimeouts {
		t.Fatalf("mount key does not change when fs-path changes: %q", got)
	}
}

func TestGenerateMountKeyWithFuseParamsIsInsertionOrderIndependent(t *testing.T) {
	left := map[string]string{
		"entry-timeout": "1",
		"attr-timeout":  "1",
	}
	right := map[string]string{
		"attr-timeout":  "1",
		"entry-timeout": "1",
	}

	leftKey := GenerateMountKeyWithFuseParams("m1:8995", "/data", left)
	rightKey := GenerateMountKeyWithFuseParams("m1:8995", "/data", right)
	if leftKey != rightKey {
		t.Fatalf("mount key depends on map insertion order: %q != %q", leftKey, rightKey)
	}

	// The key must be derived from the sorted parameter representation, not from
	// the raw map contents in iteration order.
	if got := GenerateMountKeyWithFuseParams("m1:8995", "/data", map[string]string{
		"entry-timeout": "2",
		"attr-timeout":  "2",
	}); got == leftKey {
		t.Fatalf("mount key does not change when both parameter values change: %q", got)
	}
}

func TestVolumeHandleRoundTrip(t *testing.T) {
	cases := []struct {
		name        string
		masterAddrs string
		fsPath      string
		pvName      string
	}{
		{"root fs-path", "m1:8995", "/", "pvc-a"},
		{"single segment fs-path", "m1:8995", "/data", "pvc-a"},
		{"nested fs-path", "m1:8995", "/a/b/c", "pvc-b"},
		{"multiple master addresses", "m1:8995,m2:8995", "/data", "pvc-c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle := GenerateVolumeHandle(tc.masterAddrs, tc.fsPath, tc.pvName)
			components, err := ParseVolumeHandle(handle)
			if err != nil {
				t.Fatalf("ParseVolumeHandle(%q) returned error: %v", handle, err)
			}
			if components.FSPath != tc.fsPath {
				t.Fatalf("fs-path = %q, want %q", components.FSPath, tc.fsPath)
			}
			if components.PVName != tc.pvName {
				t.Fatalf("pv-name = %q, want %q", components.PVName, tc.pvName)
			}
			if components.ClusterID != GenerateClusterID(tc.masterAddrs) {
				t.Fatalf("cluster-id = %q, want %q", components.ClusterID, GenerateClusterID(tc.masterAddrs))
			}
		})
	}
}

func TestGenerateVolumeHandleFormat(t *testing.T) {
	masterAddrs, fsPath, pvName := "m1:8995", "/data", "pvc-a"
	handle := GenerateVolumeHandle(masterAddrs, fsPath, pvName)

	parts := strings.Split(handle, VolumeHandleSeparator)
	if len(parts) != 3 {
		t.Fatalf("volume handle %q has %d separator-separated parts, want 3", handle, len(parts))
	}
	if want := GenerateClusterID(masterAddrs); parts[0] != want {
		t.Fatalf("cluster-id segment = %q, want %q", parts[0], want)
	}
	if parts[1] != fsPath {
		t.Fatalf("fs-path segment = %q, want %q", parts[1], fsPath)
	}
	if parts[2] != pvName {
		t.Fatalf("pv-name segment = %q, want %q", parts[2], pvName)
	}

	// Lock the wire format: {cluster-id}@{fs-path}@{pv-name}.
	if want := parts[0] + "@" + fsPath + "@" + pvName; handle != want {
		t.Fatalf("volume handle = %q, want %q", handle, want)
	}
	if VolumeHandleSeparator != "@" {
		t.Fatalf("VolumeHandleSeparator = %q, want %q", VolumeHandleSeparator, "@")
	}
	if got := strings.Count(handle, VolumeHandleSeparator); got != 2 {
		t.Fatalf("volume handle %q contains %d separators, want 2", handle, got)
	}
}

func TestParseVolumeHandleRejectsInvalidHandles(t *testing.T) {
	cases := []struct {
		name     string
		handle   string
		wantHint string
	}{
		{"empty handle", "", "empty"},
		{"two segments", "a@b", "invalid volume handle format"},
		{"four segments", "a@b@c@d", "invalid volume handle format"},
		{"only separators", "@@@", "invalid volume handle format"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			components, err := ParseVolumeHandle(tc.handle)
			if err == nil {
				t.Fatalf("ParseVolumeHandle(%q) = %#v, want an error", tc.handle, components)
			}
			if components != nil {
				t.Fatalf("ParseVolumeHandle(%q) returned components %#v alongside an error", tc.handle, components)
			}
			if !strings.Contains(err.Error(), tc.wantHint) {
				t.Fatalf("error for %q = %q, want it to mention %q", tc.handle, err.Error(), tc.wantHint)
			}
			if tc.wantHint == "invalid volume handle format" &&
				!strings.Contains(err.Error(), "{cluster-id}@{fs-path}@{pv-name}") {
				t.Fatalf("error for %q = %q, want the expected handle format", tc.handle, err.Error())
			}
		})
	}
}

func TestParseVolumeHandleKeepsEmptySegments(t *testing.T) {
	// ParseVolumeHandle only enforces the segment count; empty segments are
	// returned as-is and the fs-path rules are enforced by ValidateFSPath.
	// Locking this down keeps a future tightening of the parser intentional.
	cases := []struct {
		name   string
		handle string
		wantFS string
		wantPV string
	}{
		{"empty fs-path", "abc@" + VolumeHandleSeparator + "pvc-a", "", "pvc-a"},
		{"empty pv-name", "abc@/data" + VolumeHandleSeparator, "/data", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			components, err := ParseVolumeHandle(tc.handle)
			if err != nil {
				t.Fatalf("ParseVolumeHandle(%q) returned error: %v", tc.handle, err)
			}
			if components.ClusterID != "abc" || components.FSPath != tc.wantFS || components.PVName != tc.wantPV {
				t.Fatalf("unexpected components: %#v", components)
			}
		})
	}
}

func TestGetCurvinePath(t *testing.T) {
	cases := []struct {
		name   string
		fsPath string
		pvName string
		want   string
	}{
		{"root fs-path", "/", "pvc-a", "/pvc-a"},
		{"no trailing slash", "/data", "pvc-a", "/data/pvc-a"},
		{"trailing slash", "/data/", "pvc-a", "/data/pvc-a"},
		{"nested fs-path", "/a/b/c", "pvc-a", "/a/b/c/pvc-a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := GetCurvinePath(tc.fsPath, tc.pvName)
			if got != tc.want {
				t.Fatalf("GetCurvinePath(%q, %q) = %q, want %q", tc.fsPath, tc.pvName, got, tc.want)
			}
			if strings.Contains(got, "//") {
				t.Fatalf("GetCurvinePath(%q, %q) = %q, want no doubled slash", tc.fsPath, tc.pvName, got)
			}
		})
	}
}
