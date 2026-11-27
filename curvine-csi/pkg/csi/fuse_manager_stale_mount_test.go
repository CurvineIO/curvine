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
	"testing"
)

func TestDirHasEntries(t *testing.T) {
	dir := t.TempDir()

	if dirHasEntries(dir) {
		t.Fatalf("empty directory reported as non-empty")
	}
	if dirHasEntries(filepath.Join(dir, "missing")) {
		t.Fatalf("missing directory reported as non-empty")
	}
	if err := os.MkdirAll(filepath.Join(dir, "data", "s3"), 0750); err != nil {
		t.Fatal(err)
	}
	if !dirHasEntries(dir) {
		t.Fatalf("directory with leftovers reported as empty")
	}
}

func TestMountPointUsable(t *testing.T) {
	dir := t.TempDir()
	if !mountPointUsable(dir) {
		t.Fatalf("empty but readable directory reported as unusable")
	}
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if !mountPointUsable(dir) {
		t.Fatalf("directory with entries reported as unusable")
	}
	if mountPointUsable(filepath.Join(dir, "missing")) {
		t.Fatalf("missing directory reported as usable")
	}
}

// A directory left over from a previous FUSE session is not empty; restaging
// must clear it, because curvine-fuse refuses to mount on a non-empty
// directory ("Mnt ... is not empty").
func TestCleanupMountPointClearsLeftovers(t *testing.T) {
	dir := t.TempDir()
	mnt := filepath.Join(dir, "fuse-mount")
	if err := os.MkdirAll(filepath.Join(mnt, "data", "s3", "meshy-datasets"), 0750); err != nil {
		t.Fatal(err)
	}

	fm := &FuseManager{processes: make(map[string]*FuseProcess)}
	if err := fm.cleanupMountPoint(mnt); err != nil {
		t.Fatalf("cleanupMountPoint: %v", err)
	}
	entries, err := os.ReadDir(mnt)
	if err != nil {
		t.Fatalf("mount point not recreated: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("mount point still has %d entries after cleanup", len(entries))
	}
	if !dirHasEntries(filepath.Dir(mnt)) {
		t.Fatalf("cleanup removed the parent directory")
	}
}
