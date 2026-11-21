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
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

func newTestMountState(t *testing.T) *MountState {
	t.Helper()

	client := &K8sClient{
		clientset:  fake.NewSimpleClientset(),
		namespace:  "curvine",
		nodeName:   "node-1",
		secretName: "curvine-csi-mount-node-1",
	}

	ms, err := NewMountState(client)
	if err != nil {
		t.Fatalf("NewMountState: %v", err)
	}
	return ms
}

func TestRemoveVolumeIgnoresUnknownVolumeAndKeepsRefCount(t *testing.T) {
	ms := newTestMountState(t)

	if err := ms.AddMount(&MountInfo{MntPath: "/mnt/curvine", ClusterID: "cluster-1"}); err != nil {
		t.Fatalf("AddMount: %v", err)
	}
	if err := ms.AddVolume("/mnt/curvine", "cluster-1", "vol-a"); err != nil {
		t.Fatalf("AddVolume(vol-a): %v", err)
	}
	if err := ms.AddVolume("/mnt/curvine", "cluster-1", "vol-b"); err != nil {
		t.Fatalf("AddVolume(vol-b): %v", err)
	}

	mount, ok := ms.GetMount("/mnt/curvine", "cluster-1")
	if !ok {
		t.Fatal("mount state missing after AddVolume")
	}
	if mount.RefCount != 2 {
		t.Fatalf("RefCount = %d, want 2", mount.RefCount)
	}

	// A volume that was never registered must not decrement the shared mount.
	if err := ms.RemoveVolume("/mnt/curvine", "cluster-1", "vol-missing"); err != nil {
		t.Fatalf("RemoveVolume(unknown): %v", err)
	}
	mount, _ = ms.GetMount("/mnt/curvine", "cluster-1")
	if mount.RefCount != 2 {
		t.Fatalf("RefCount after removing unknown volume = %d, want 2", mount.RefCount)
	}
	if len(mount.Volumes) != 2 {
		t.Fatalf("Volumes = %v, want 2 entries", mount.Volumes)
	}

	// Removing a known volume decrements exactly once.
	if err := ms.RemoveVolume("/mnt/curvine", "cluster-1", "vol-a"); err != nil {
		t.Fatalf("RemoveVolume(vol-a): %v", err)
	}
	mount, _ = ms.GetMount("/mnt/curvine", "cluster-1")
	if mount.RefCount != 1 {
		t.Fatalf("RefCount after removing vol-a = %d, want 1", mount.RefCount)
	}

	// A repeated call is a no-op, so NodeUnpublish/NodeUnstage retries cannot
	// drive the counter to zero while vol-b is still published.
	if err := ms.RemoveVolume("/mnt/curvine", "cluster-1", "vol-a"); err != nil {
		t.Fatalf("RemoveVolume(vol-a) retry: %v", err)
	}
	mount, _ = ms.GetMount("/mnt/curvine", "cluster-1")
	if mount.RefCount != 1 {
		t.Fatalf("RefCount after repeated removal = %d, want 1", mount.RefCount)
	}
}

// DecrementRefCount and UpdateFusePID used to call saveToSecret while already
// holding ms.mutex, which self-deadlocks on sync.RWMutex. Guard against a
// regression by requiring both calls to return promptly.
func TestStateMutatorsDoNotDeadlockOnPersistence(t *testing.T) {
	ms := newTestMountState(t)
	if err := ms.AddMount(&MountInfo{MntPath: "/mnt/curvine", ClusterID: "cluster-1"}); err != nil {
		t.Fatalf("AddMount: %v", err)
	}
	if err := ms.AddVolume("/mnt/curvine", "cluster-1", "vol-a"); err != nil {
		t.Fatalf("AddVolume: %v", err)
	}

	done := make(chan error, 2)
	go func() { done <- ms.DecrementRefCount("/mnt/curvine", "cluster-1") }()
	go func() { done <- ms.UpdateFusePID("/mnt/curvine", "cluster-1", 4242) }()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("mutator returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("state mutator deadlocked while persisting state")
		}
	}

	mount, ok := ms.GetMount("/mnt/curvine", "cluster-1")
	if !ok {
		t.Fatal("mount state missing")
	}
	if mount.RefCount != 0 {
		t.Fatalf("RefCount = %d, want 0", mount.RefCount)
	}
	if mount.FusePID != 4242 {
		t.Fatalf("FusePID = %d, want 4242", mount.FusePID)
	}
}
