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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func TestNormalizeCurvinePath(t *testing.T) {
	valid := map[string]string{
		"/":            "/",
		"/data":        "/data",
		"/data/pvc-1":  "/data/pvc-1",
		"/data//pvc-1": "/data/pvc-1",
	}
	for input, want := range valid {
		got, err := NormalizeCurvinePath(input)
		if err != nil {
			t.Fatalf("NormalizeCurvinePath(%q) returned error: %v", input, err)
		}
		if got != want {
			t.Fatalf("NormalizeCurvinePath(%q) = %q, want %q", input, got, want)
		}
	}

	invalid := []string{
		"",
		"data",
		"/data/..",
		"/data/../etc",
		"/data/../../etc",
		"/data/.",
		"/../etc",
	}
	for _, input := range invalid {
		if _, err := NormalizeCurvinePath(input); err == nil {
			t.Fatalf("NormalizeCurvinePath(%q) accepted an unsafe path", input)
		}
	}
}

func TestIsSubPathUsesComponentBoundaries(t *testing.T) {
	cases := []struct {
		root, target string
		want         bool
	}{
		{"/data", "/data", true},
		{"/data", "/data/pvc-1", true},
		{"/data", "/database/pvc-1", false},
		{"/data", "/dat", false},
		{"/data", "/", false},
		{"/", "/anything", true},
		{"/", "/", true},
	}
	for _, tc := range cases {
		if got := IsSubPath(tc.root, tc.target); got != tc.want {
			t.Fatalf("IsSubPath(%q, %q) = %v, want %v", tc.root, tc.target, got, tc.want)
		}
	}
}

func TestCurvineSubPath(t *testing.T) {
	cases := []struct {
		root, target string
		want         string
		wantErr      bool
	}{
		{"/", "/pvc-1", "pvc-1", false},
		{"/", "/", "", false},
		{"/test-data", "/test-data/pvc-1", "pvc-1", false},
		{"/test-data", "/test-data/a/b", "a/b", false},
		{"/data", "/database/pvc-1", "", true},
		{"/data", "/data/../etc", "", true},
	}
	for _, tc := range cases {
		got, err := CurvineSubPath(tc.root, tc.target)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("CurvineSubPath(%q, %q) = %q, want error", tc.root, tc.target, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("CurvineSubPath(%q, %q) returned error: %v", tc.root, tc.target, err)
		}
		if got != tc.want {
			t.Fatalf("CurvineSubPath(%q, %q) = %q, want %q", tc.root, tc.target, got, tc.want)
		}
	}
}

func TestJoinMountSubPathStaysInsideMountRoot(t *testing.T) {
	got, err := JoinMountSubPath("/var/lib/kubelet/fuse-mount", "pvc-1")
	if err != nil {
		t.Fatalf("JoinMountSubPath returned error: %v", err)
	}
	if got != "/var/lib/kubelet/fuse-mount/pvc-1" {
		t.Fatalf("JoinMountSubPath = %q", got)
	}

	for _, escaped := range []string{"../etc", "../../../etc/passwd", "a/../../etc"} {
		if _, err := JoinMountSubPath("/var/lib/kubelet/fuse-mount", escaped); err == nil {
			t.Fatalf("JoinMountSubPath accepted escaping path %q", escaped)
		}
	}
}

func TestValidatePathRejectsTraversal(t *testing.T) {
	if err := ValidatePath("/data/pvc-1"); err != nil {
		t.Fatalf("ValidatePath rejected a valid path: %v", err)
	}
	for _, p := range []string{"/data/..", "/data/../..", "relative/path"} {
		if err := ValidatePath(p); err == nil {
			t.Fatalf("ValidatePath accepted unsafe path %q", p)
		}
	}
}

func TestIsReadOnlyRequest(t *testing.T) {
	readOnly := &csi.NodePublishVolumeRequest{Readonly: true}
	if !IsReadOnlyRequest(readOnly) {
		t.Fatal("explicit readonly request was not detected")
	}

	readerOnly := &csi.NodePublishVolumeRequest{
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			},
		},
	}
	if !IsReadOnlyRequest(readerOnly) {
		t.Fatal("MULTI_NODE_READER_ONLY access mode was not treated as read-only")
	}

	readWrite := &csi.NodePublishVolumeRequest{
		VolumeCapability: &csi.VolumeCapability{
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
			},
		},
	}
	if IsReadOnlyRequest(readWrite) {
		t.Fatal("read-write request was treated as read-only")
	}
}
