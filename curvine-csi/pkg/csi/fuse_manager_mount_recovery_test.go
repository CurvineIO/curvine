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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// recoveryManager builds a FuseManager whose mount-table answers are scripted,
// so every branch of the restage decision can be exercised without a live FUSE
// mount. Each entry of `mounted` answers one call in order; running past the
// end answers false.
func recoveryManager(t *testing.T, mounted ...bool) (*FuseManager, *[]string) {
	t.Helper()
	fm := &FuseManager{processes: make(map[string]*FuseProcess)}
	calls := []string{}
	next := 0
	fm.mountedInTable = func(path string) (bool, error) {
		calls = append(calls, "mounted:"+path)
		if next >= len(mounted) {
			return false, nil
		}
		answer := mounted[next]
		next++
		return answer, nil
	}
	fm.lazyUnmount = func(path string) error {
		calls = append(calls, "umount:"+path)
		return nil
	}
	return fm, &calls
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0750); err != nil {
		t.Fatal(err)
	}
}

func requireEmptyDir(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("mount point missing: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("mount point still holds %d entries", len(entries))
	}
}

// An owned, running process keeps its mount: staging is a no-op.
func TestPrepareMountPointReusesOwnedProcess(t *testing.T) {
	fm, calls := recoveryManager(t)
	mnt := t.TempDir()
	key := getProcessKey(mnt, "cluster")
	fm.processes[key] = &FuseProcess{PID: os.Getpid()}

	reuse, err := fm.prepareMountPoint(mnt, key)
	if err != nil || !reuse {
		t.Fatalf("reuse=%v err=%v, want reuse", reuse, err)
	}
	if len(*calls) != 0 {
		t.Fatalf("unexpected mount-table work: %v", *calls)
	}
}

// A mount this plugin does not own is detached and restaged, never reused:
// nothing else would watch it or unmount it later.
func TestPrepareMountPointRestagesUnownedMount(t *testing.T) {
	// The first answer is prepare's own look, the second is cleanup's.
	fm, calls := recoveryManager(t, true, true, false)
	mnt := filepath.Join(t.TempDir(), "fuse-mount")
	mustMkdirAll(t, filepath.Join(mnt, "data", "s3", "dataset-a"))

	reuse, err := fm.prepareMountPoint(mnt, getProcessKey(mnt, "cluster"))
	if err != nil || reuse {
		t.Fatalf("reuse=%v err=%v, want restage", reuse, err)
	}
	want := []string{"mounted:" + mnt, "mounted:" + mnt, "umount:" + mnt, "mounted:" + mnt}
	if fmt.Sprint(*calls) != fmt.Sprint(want) {
		t.Fatalf("calls=%v, want %v", *calls, want)
	}
	requireEmptyDir(t, mnt)
}

// Leftovers under an unmounted directory are cleared without an unmount.
func TestPrepareMountPointClearsLeftoversWhenUnmounted(t *testing.T) {
	fm, calls := recoveryManager(t, false)
	mnt := filepath.Join(t.TempDir(), "fuse-mount")
	mustMkdirAll(t, filepath.Join(mnt, "data", "s3"))

	reuse, err := fm.prepareMountPoint(mnt, getProcessKey(mnt, "cluster"))
	if err != nil || reuse {
		t.Fatalf("reuse=%v err=%v, want restage", reuse, err)
	}
	for _, call := range *calls {
		if strings.HasPrefix(call, "umount:") {
			t.Fatalf("unmounted a directory that is not mounted: %v", *calls)
		}
	}
	requireEmptyDir(t, mnt)
}

// A ready directory or a missing one needs no work; curvine-fuse creates the
// mount point itself.
func TestPrepareMountPointReadyDirectoryIsNoop(t *testing.T) {
	for _, mnt := range []string{filepath.Join(t.TempDir(), "missing"), t.TempDir()} {
		fm, calls := recoveryManager(t, false)
		reuse, err := fm.prepareMountPoint(mnt, getProcessKey(mnt, "cluster"))
		if err != nil || reuse {
			t.Fatalf("%s: reuse=%v err=%v, want no-op", mnt, reuse, err)
		}
		if len(*calls) != 1 {
			t.Fatalf("%s: unexpected work: %v", mnt, *calls)
		}
	}
}

func TestPrepareMountPointPropagatesMountTableError(t *testing.T) {
	fm, _ := recoveryManager(t)
	fm.mountedInTable = func(string) (bool, error) { return false, fmt.Errorf("boom") }
	if _, err := fm.prepareMountPoint(t.TempDir(), "k"); err == nil {
		t.Fatal("want error when the mount table cannot be read")
	}
}

// The detach must not be gated on a stat: a dead mount fails stat with
// ENOTCONN, and a missing path with ENOENT. Here the path does not exist but
// the table says mounted, which a stat-first implementation would treat as
// "not mounted" and skip the unmount.
func TestCleanupMountPointUnmountsWithoutStatting(t *testing.T) {
	fm, calls := recoveryManager(t, true, false)
	mnt := filepath.Join(t.TempDir(), "missing")

	if err := fm.cleanupMountPoint(mnt); err != nil {
		t.Fatal(err)
	}
	want := []string{"mounted:" + mnt, "umount:" + mnt, "mounted:" + mnt}
	if fmt.Sprint(*calls) != fmt.Sprint(want) {
		t.Fatalf("calls=%v, want %v", *calls, want)
	}
	requireEmptyDir(t, mnt)
}

// If the mount is still listed after the unmount, the directory must be left
// alone: deleting through an attached mount would recurse into the namespace.
func TestCleanupMountPointRefusesWhenStillMounted(t *testing.T) {
	fm, _ := recoveryManager(t, true, true)
	mnt := t.TempDir()
	kept := filepath.Join(mnt, "keep")
	if err := os.WriteFile(kept, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	err := fm.cleanupMountPoint(mnt)
	if err == nil || !strings.Contains(err.Error(), "still mounted") {
		t.Fatalf("err=%v, want still-mounted error", err)
	}
	if _, statErr := os.Stat(kept); statErr != nil {
		t.Fatalf("cleanup touched a still-mounted directory: %v", statErr)
	}
}

func TestCleanupMountPointPropagatesUnmountError(t *testing.T) {
	fm, _ := recoveryManager(t, true)
	fm.lazyUnmount = func(string) error { return fmt.Errorf("busy") }
	if err := fm.cleanupMountPoint(t.TempDir()); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("err=%v, want the unmount error", err)
	}
}

// A regular file under an unmounted mount point is not leftover noise; refuse
// instead of deleting it.
func TestCleanupMountPointRefusesToDeleteFiles(t *testing.T) {
	fm, _ := recoveryManager(t, false)
	mnt := t.TempDir()
	kept := filepath.Join(mnt, "keep")
	if err := os.WriteFile(kept, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	err := fm.cleanupMountPoint(mnt)
	if err == nil || !strings.Contains(err.Error(), "refusing to delete") {
		t.Fatalf("err=%v, want refusal", err)
	}
	if _, statErr := os.Stat(kept); statErr != nil {
		t.Fatalf("cleanup deleted a file: %v", statErr)
	}
}

// The mount table stores canonical paths. A kubelet root reached through a
// symlink must still match the canonical entry, without stat-ing the mount
// endpoint (a dead FUSE mount returns ENOTCONN there).
func TestMountTableHasResolvesSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	real := filepath.Join(root, "real")
	if err := os.MkdirAll(real, 0750); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(real, alias); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	mnt := filepath.Join(real, "fuse-mount")
	if err := os.MkdirAll(mnt, 0750); err != nil {
		t.Fatal(err)
	}
	data := []byte(fmt.Sprintf("36 1 0:32 / %s rw,nosuid - tmpfs tmpfs rw\n", mnt))
	ok, err := mountTableHasData(data, filepath.Join(alias, "fuse-mount"))
	if err != nil || !ok {
		t.Fatalf("aliased mount not listed: ok=%v err=%v", ok, err)
	}
	if ok, err := mountTableHasData(data, filepath.Join(root, "other")); err != nil || ok {
		t.Fatalf("unrelated path listed: ok=%v err=%v", ok, err)
	}
}

func TestMountTableHasRealMounts(t *testing.T) {
	if ok, err := mountTableHas("/"); err != nil || !ok {
		t.Fatalf("root not listed: ok=%v err=%v", ok, err)
	}
	if ok, err := mountTableHas(t.TempDir()); err != nil || ok {
		t.Fatalf("temp dir listed: ok=%v err=%v", ok, err)
	}
}

// End-to-end over a real mount: with a mount attached, cleanup detaches it and
// leaves an empty directory. tmpfs stands in for FUSE, which needs a daemon;
// the code path under test (mount table, lazy unmount, leftover removal) is the
// same. Skipped where mounting is not permitted.
func TestCleanupMountPointDetachesRealMount(t *testing.T) {
	mnt := filepath.Join(t.TempDir(), "fuse-mount")
	mustMkdirAll(t, mnt)
	if out, err := exec.Command("mount", "-t", "tmpfs", "tmpfs", mnt).CombinedOutput(); err != nil {
		t.Skipf("mounting tmpfs is not permitted: %v: %s", err, out)
	}
	defer exec.Command("umount", "-l", mnt).Run()
	if mounted, err := mountTableHas(mnt); err != nil || !mounted {
		t.Fatalf("tmpfs not listed as mounted: %v %v", mounted, err)
	}
	if err := os.WriteFile(filepath.Join(mnt, "leftover"), []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	fm := &FuseManager{
		processes:      make(map[string]*FuseProcess),
		mountedInTable: mountTableHas,
		lazyUnmount:    umountLazy,
	}
	if err := fm.cleanupMountPoint(mnt); err != nil {
		t.Fatal(err)
	}
	if mounted, err := mountTableHas(mnt); err != nil || mounted {
		t.Fatalf("still mounted after cleanup: %v %v", mounted, err)
	}
	requireEmptyDir(t, mnt)
}

// The syscall reports "not a mount point" and "no such path" as errnos; both
// mean there is nothing to detach.
func TestUmountLazyTreatsNotMountedAsSuccess(t *testing.T) {
	// umount(2) checks CAP_SYS_ADMIN before it looks at the path, so a run
	// without mount privileges sees EPERM instead of EINVAL/ENOENT.
	for _, path := range []string{t.TempDir(), filepath.Join(t.TempDir(), "missing")} {
		err := umountLazy(path)
		if errors.Is(err, syscall.EPERM) {
			t.Skipf("no mount privileges: %v", err)
		}
		if err != nil {
			t.Fatalf("umountLazy(%s) = %v, want nil", path, err)
		}
	}
}
