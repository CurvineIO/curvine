/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func lifecycleOptions(key string) *StandaloneOptions {
	return &StandaloneOptions{MountKey: key, MasterAddrs: "localhost:8995", FSPath: "/", NodeName: "node", Namespace: "test"}
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not reach barrier")
	}
}

func waitLockUsers(t *testing.T, locks *keyedLifecycleLocks, key string, users int) {
	t.Helper()
	if locks.keyFor != nil {
		key = locks.keyFor(key)
	}
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		locks.mu.Lock()
		entry := locks.entries[key]
		ready := entry != nil && entry.users == users
		locks.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-deadline:
			t.Fatal("waiter did not reach lifecycle lock")
		case <-ticker.C:
		}
	}
}

func readyCreatedPods(client *fake.Clientset) {
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		return false, nil, nil
	})
}

func TestStageHoldsLifecycleThroughEnsureAndReference(t *testing.T) {
	m, client := cleanupManager(t, "old")
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	blocked := false
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if !blocked && m.mountEntry(cleanupKey).PendingStages["new"] != "" {
			blocked = true
			close(entered)
			<-proceed
		}
		return false, nil, nil
	})
	staged, released := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new")
		staged <- err
	}()
	waitSignal(t, entered)
	// This association must already be committed before ensure touches the Pod.
	hasVolume(t, m, "new", true)
	go func() { released <- m.ReleaseVolume(context.Background(), cleanupKey, "old") }()
	waitLockUsers(t, &m.lifecycle, cleanupKey, 2)
	once.Do(func() { close(proceed) })
	if err := waitResult(t, staged); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, released); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "new", true)
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("new consumer lost its Pod:", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "pods" {
			t.Fatal("shared Pod deleted")
		}
	}
}

func TestLastCleanupExcludesReplacementAndAllowsOtherMounts(t *testing.T) {
	m, client := cleanupManager(t, "old")
	setTestHostMountBase(t, m, client, t.TempDir())
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	present := true
	m.mountCleanup = &mountCleanup{
		readInfo: func() ([]byte, error) {
			if present {
				return targetMountInfo(m.GetHostMountPath(cleanupKey)), nil
			}
			return []byte(rootMountInfo), nil
		},
		run: func(context.Context, ...string) ([]byte, error) {
			close(entered)
			<-proceed
			present = false
			return nil, nil
		},
	}
	readyCreatedPods(client)
	released := make(chan error, 1)
	go func() { released <- m.ReleaseVolume(context.Background(), cleanupKey, "old") }()
	waitSignal(t, entered)

	// An unrelated mount can commit a reference while this unmount is blocked.
	other := make(chan error, 1)
	go func() { other <- m.AddVolumeRef(context.Background(), "other-mount", "other") }()
	if err := waitResult(t, other); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "old", true)
	hasVolume(t, m, "other", true)

	// Waiting for the same mount must honor cancellation, including the publish ensure path.
	for _, stage := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			var err error
			if stage {
				_, err = m.StageVolume(ctx, lifecycleOptions(cleanupKey), "canceled")
			} else {
				_, err = m.EnsureStandalone(ctx, lifecycleOptions(cleanupKey))
			}
			result <- err
		}()
		waitLockUsers(t, &m.lifecycle, cleanupKey, 2)
		cancel()
		if err := waitResult(t, result); !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	}
	staged := make(chan error, 1)
	go func() {
		_, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new")
		staged <- err
	}()
	waitLockUsers(t, &m.lifecycle, cleanupKey, 2)
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
			// Only cleanupManager's original seed Pod may have been created so far.
			pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
			if pod.UID != "original" {
				t.Fatal("replacement created before host cleanup")
			}
		}
	}
	once.Do(func() { close(proceed) })
	if err := waitResult(t, released); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, staged); err != nil {
		t.Fatal(err)
	}
	restored := recovered(t, client)
	hasVolume(t, restored, "old", false)
	hasVolume(t, restored, "new", true)
	hasVolume(t, restored, "other", true)
	hasVolume(t, restored, "canceled", false)
}

func TestStagePersistsBeforeCreationAndSurvivesCancellation(t *testing.T) {
	client := fake.NewClientset()
	m := NewStandaloneManager(client, "test", "node", "image", "sa").(*standaloneMountManagerImpl)
	setTestHostMountBase(t, m, client, t.TempDir())
	client.ClearActions() // fixture setup is not part of the staging operation
	fail := true
	for _, verb := range []string{"create", "update"} {
		client.PrependReactor(verb, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
			if fail {
				return true, nil, errors.New("persistence unavailable")
			}
			return false, nil, nil
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "first"); err == nil {
		t.Fatal("expected first-reference persistence failure")
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "pods" && action.GetVerb() != "get" {
			t.Fatal("resources mutated before durable association")
		}
	}
	hasVolume(t, m, "first", true)
	fail = false
	client.PrependReactor("create", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		hasVolume(t, m, "first", true)
		cancel()    // Pod creation completes, but readiness cannot complete.
		fail = true // Subsequent persistence is also unavailable.
		return false, nil, nil
	})
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "first"); err == nil {
		t.Fatal("canceled staging succeeded")
	}
	hasVolume(t, m, "first", true)
	m = recovered(t, client)
	hasVolume(t, m, "first", true)
	if err := m.ReleaseVolume(context.Background(), cleanupKey, "first"); err == nil {
		t.Fatal("cleanup persistence failure must propagate")
	}
	hasVolume(t, recovered(t, client), "first", true)
	fail = false
	if err := m.ReleaseVolume(context.Background(), cleanupKey, "first"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "first", false)
}

func TestGarbageCollectionRetriesDeletedReferenceWithLiveConsumer(t *testing.T) {
	m, client := cleanupManager(t, "deleted", "live")
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "live-pv"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: "live"}}}}
	if _, err := client.CoreV1().PersistentVolumes().Create(context.Background(), pv, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	fail := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("persist unavailable")
		}
		return false, nil, nil
	})
	if err := m.ReleaseVolume(context.Background(), cleanupKey, "deleted"); err == nil {
		t.Fatal("expected failure")
	}
	m = recovered(t, client)
	fail = false
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	n.cleanupOrphanedStandalonePods()
	n.cleanupOrphanedStandalonePods()
	hasVolume(t, recovered(t, client), "deleted", false)
	hasVolume(t, m, "live", true)
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("live consumer lost Pod:", err)
	}
}

func TestStateTransactionsAllowReadsAndCancelWaiters(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) { close(entered); <-proceed; return false, nil, nil })
	result := make(chan error, 1)
	go func() { result <- m.AddVolumeRef(context.Background(), "new-key", "new") }()
	waitSignal(t, entered)
	read := make(chan error, 1)
	go func() {
		_, old := m.FindMountKeyByVolumeID("existing")
		_, newRef := m.FindMountKeyByVolumeID("new")
		if !old || !newRef {
			read <- errors.New("missing committed or uncertain cleanup reference")
		} else {
			read <- nil
		}
	}()
	if err := waitResult(t, read); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	waiting := make(chan error, 1)
	go func() { waiting <- m.AddVolumeRef(ctx, "another-key", "waiting") }()
	waitLockUsers(t, &m.persistence, "state", 2)
	cancel()
	if err := waitResult(t, waiting); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	once.Do(func() { close(proceed) })
	if err := waitResult(t, result); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "new", true)
	hasVolume(t, m, "waiting", false)
}

func TestConcurrentReferenceTransactionsPreserveBothMounts(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	results := make(chan error, 2)
	for _, key := range []string{"one", "two"} {
		go func(key string) { results <- m.AddVolumeRef(context.Background(), key, key) }(key)
	}
	for range 2 {
		if err := waitResult(t, results); err != nil {
			t.Fatal(err)
		}
	}
	restored := recovered(t, client)
	for _, v := range []string{"existing", "one", "two"} {
		hasVolume(t, restored, v, true)
	}
}

// An API write may commit even when its response is lost. Before recreating a
// Pod, a repeated stage must reassert its durable association, not just trust
// the conservative reference retained in memory after failed cleanup.
func TestRestageReassertsReferenceAfterAmbiguousCleanupWrite(t *testing.T) {
	m, client := cleanupManager(t, "same")
	setTestHostMountBase(t, m, client, t.TempDir())
	fail := true
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !fail {
			return false, nil, nil
		}
		cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, "test"); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("response lost after server committed")
	})
	if err := m.ReleaseVolume(context.Background(), cleanupKey, "same"); err == nil {
		t.Fatal("expected ambiguous failure")
	}
	hasVolume(t, m, "same", true)
	hasVolume(t, recovered(t, client), "same", false)
	fail = false
	readyCreatedPods(client)
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "same"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "same", true)
}

func TestPublishRecoveryReassertsOwnershipAfterAmbiguousCleanup(t *testing.T) {
	for _, failPersist := range []bool{false, true} {
		t.Run(fmt.Sprint("persist-fails=", failPersist), func(t *testing.T) {
			m, client := cleanupManager(t, "same")
			setTestHostMountBase(t, m, client, t.TempDir())
			ambiguous := true
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if ambiguous {
					cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, "test"); err != nil {
						return true, nil, err
					}
					return true, nil, errors.New("response lost after cleanup committed")
				}
				if failPersist {
					return true, nil, errors.New("ownership persistence unavailable")
				}
				return false, nil, nil
			})
			if err := m.ReleaseVolume(context.Background(), cleanupKey, "same"); err == nil {
				t.Fatal("expected ambiguous cleanup failure")
			}
			hasVolume(t, m, "same", true)
			hasVolume(t, recovered(t, client), "same", false)
			ambiguous = false
			client.ClearActions()
			readyCreatedPods(client)
			_, err := m.EnsureStandalone(context.Background(), lifecycleOptions(cleanupKey))
			if failPersist {
				if err == nil {
					t.Fatal("publish recovery created resources without durable ownership")
				}
				for _, action := range client.Actions() {
					if action.GetVerb() == "create" && action.GetResource().Resource == "pods" {
						t.Fatal("Pod created after ownership persistence failed")
					}
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				hasVolume(t, recovered(t, client), "same", true)
			}
		})
	}
}

func TestReadinessRejectsTerminatingPod(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	pod, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	if _, err := client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) { cancel(); return false, nil, nil })
	if err := m.WaitForStandaloneReady(ctx, cleanupKey, time.Second); err == nil {
		t.Fatal("terminating Pod reported ready")
	}
}
