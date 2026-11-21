/*
Copyright 2026 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package csi

import (
	"context"
	"errors"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// Fail after the intent is persisted but before readiness succeeds.
func failNewStage(t *testing.T, m *standaloneMountManagerImpl, client *fake.Clientset, volume string) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fail := true
	oldAttempt := m.mountEntry(cleanupKey).PendingStages[volume]
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail && m.mountEntry(cleanupKey).PendingStages[volume] != oldAttempt {
			cancel()
			return true, nil, ctx.Err()
		}
		return false, nil, nil
	})
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), volume); err == nil {
		t.Fatal("stage unexpectedly succeeded")
	}
	fail = false
	attempt := m.GetState().Mounts[cleanupKey].PendingStages[volume]
	if attempt == "" {
		t.Fatal("failed stage lost its cleanup identity")
	}
	return attempt
}

func retainPV(t *testing.T, client *fake.Clientset, volume string) {
	t.Helper()
	_, err := client.CoreV1().PersistentVolumes().Create(context.Background(), &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volume},
		Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{
			CSI: &corev1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: volume},
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAbandonedStageGCWithRetainedPVAndRestart(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "last", true: "shared"}[shared], func(t *testing.T) {
			volumes := []string{}
			if shared {
				volumes = append(volumes, "live")
			}
			m, client := cleanupManager(t, volumes...)
			failNewStage(t, m, client, "abandoned")
			retainPV(t, client, "abandoned")
			retainPV(t, client, "live")
			m = recovered(t, client)
			n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
			for range 2 {
				n.cleanupOrphanedStandalonePods()
			}
			hasVolume(t, recovered(t, client), "abandoned", false)
			pod, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
			if shared {
				hasVolume(t, recovered(t, client), "live", true)
				if err != nil || pod.UID != "original" {
					t.Fatalf("live consumer lost its Pod: %v", err)
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatalf("abandoned Pod still present: %v", err)
			}
		})
	}
}

func TestAbandonedStageCleanupRetriesFailures(t *testing.T) {
	for _, failure := range []string{"delete", "mount", "persist"} {
		t.Run(failure, func(t *testing.T) {
			m, client := cleanupManager(t)
			attempt := failNewStage(t, m, client, "new")
			fail := true
			client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
				if fail && failure == "delete" {
					return true, nil, errors.New("delete unavailable")
				}
				return false, nil, nil
			})
			client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if fail && failure == "persist" {
					return true, nil, errors.New("state unavailable")
				}
				return false, nil, nil
			})
			m.mountCleanup.readInfo = func() ([]byte, error) {
				if fail && failure == "mount" {
					return nil, errors.New("mountinfo unavailable")
				}
				return []byte(rootMountInfo), nil
			}
			if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", attempt); err == nil {
				t.Fatal("failed cleanup acknowledged")
			}
			hasVolume(t, recovered(t, client), "new", true)
			fail = false
			if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", attempt); err != nil {
				t.Fatal(err)
			}
			hasVolume(t, recovered(t, client), "new", false)
		})
	}
}

func TestAbandonedStageCleanupWaitsForSuccessfulRestage(t *testing.T) {
	m, client := cleanupManager(t, "live")
	oldAttempt := failNewStage(t, m, client, "new")
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if m.mountEntry(cleanupKey).PendingStages["new"] != oldAttempt {
			close(entered)
			<-proceed
		}
		return false, nil, nil
	})
	staged := make(chan error, 1)
	go func() {
		_, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new")
		staged <- err
	}()
	waitSignal(t, entered)
	// Snapshot the *current* attempt while ensure is in flight, not just an old token.
	attempt := m.GetState().Mounts[cleanupKey].PendingStages["new"]
	cleaned := make(chan error, 1)
	go func() { cleaned <- m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", attempt) }()
	waitLockUsers(t, &m.lifecycle, cleanupKey, 2)
	once.Do(func() { close(proceed) })
	if err := waitResult(t, staged); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, cleaned); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "new", true)
	hasVolume(t, m, "live", true)
	if m.GetState().Mounts[cleanupKey].PendingStages["new"] != "" {
		t.Fatal("successful stage remained reclaimable")
	}
}

func TestAbandonedStageTokenAndPublishProtection(t *testing.T) {
	m, client := cleanupManager(t, "live")
	old := failNewStage(t, m, client, "new")
	current := failNewStage(t, m, client, "new")
	if old == current {
		t.Fatal("restage reused attempt identity")
	}
	if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", old); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "new", true)
	for _, volume := range []string{"new", "absent"} {
		if _, err := m.EnsureVolume(context.Background(), lifecycleOptions(cleanupKey), volume); err == nil {
			t.Fatal("publish accepted incomplete/missing stage")
		}
	}
	if _, err := m.EnsureVolume(context.Background(), lifecycleOptions(cleanupKey), "live"); err != nil {
		t.Fatal(err)
	}
	if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", current); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "new", false)
	// A failed retry of an established/legacy reference never makes it reclaimable.
	fail := true
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("unavailable")
		}
		return false, nil, nil
	})
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "live"); err == nil {
		t.Fatal("stage succeeded")
	}
	fail = false
	if len(m.GetState().Mounts[cleanupKey].PendingStages) != 0 {
		t.Fatal("legacy ownership became reclaimable")
	}
}

func TestAbandonedStageLatePodCreate(t *testing.T) {
	for _, restage := range []bool{false, true} {
		t.Run(map[bool]string{false: "orphan", true: "restaged"}[restage], func(t *testing.T) {
			client := fake.NewClientset()
			m := recovered(t, client)
			setTestHostMountBase(t, m, client, t.TempDir())
			var late *corev1.Pod
			client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if late == nil {
					late = action.(ktesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
					return true, nil, context.DeadlineExceeded
				}
				return false, nil, nil
			})
			if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err == nil {
				t.Fatal("stage succeeded")
			}
			attempt := m.GetState().Mounts[cleanupKey].PendingStages["new"]
			if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", attempt); err != nil {
				t.Fatal(err)
			}
			late.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			if _, err := client.CoreV1().Pods("test").Create(context.Background(), late, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if restage {
				if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.CleanupOrphanedPods(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := client.CoreV1().Pods("test").Get(context.Background(), late.Name, metav1.GetOptions{})
			if restage && err != nil || !restage && !apierrors.IsNotFound(err) {
				t.Fatalf("unexpected Pod outcome: %v", err)
			}
			hasVolume(t, recovered(t, client), "new", restage)
		})
	}
}

func TestAbandonedStagePromotionResponseLost(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "late", true: "committed"}[committed], func(t *testing.T) {
			m, client := cleanupManager(t, "live")
			apply := versionedConfigMapUpdates(t, m, client)
			writes := 0
			var promotion *corev1.ConfigMap
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				writes++
				if writes != 2 {
					return false, nil, nil
				}
				promotion = action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
				if committed {
					if err := apply(promotion); err != nil {
						t.Fatal(err)
					}
				}
				return true, nil, context.DeadlineExceeded
			})
			if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err == nil {
				t.Fatal("uncertain promotion acknowledged")
			}
			attempt := m.GetState().Mounts[cleanupKey].PendingStages["new"]
			if promotion == nil || attempt == "" {
				t.Fatal("lost pending cleanup")
			}
			if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "new", attempt); err != nil {
				t.Fatal(err)
			}
			if err := apply(promotion); !apierrors.IsConflict(err) {
				t.Fatalf("late promotion restored reference: %v", err)
			}
			hasVolume(t, recovered(t, client), "new", false)
			hasVolume(t, recovered(t, client), "live", true)
		})
	}
}

func TestAbandonedStageMountIdentity(t *testing.T) {
	for _, annotated := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "annotated"}[annotated], func(t *testing.T) {
			m, client := cleanupManager(t, "live")
			pod, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if annotated {
				pod.Annotations = map[string]string{standaloneMountKeyAnnotation: cleanupKey}
			}
			if _, err := client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			collision := cleanupKey + "-collision"
			if m.getStandaloneName(collision) != pod.Name {
				t.Fatal("fixture must collide")
			}
			if _, err := m.StageVolume(context.Background(), lifecycleOptions(collision), "new"); err == nil {
				t.Fatal("reused another mount's Pod")
			}
			if m.mountEntry(collision) != nil {
				t.Fatal("known conflict recorded pending ownership")
			}
			// A pending entry written by an older attempt must also be reclaimable.
			attempt, err := m.beginStageLocked(context.Background(), collision, "new")
			if err != nil {
				t.Fatal(err)
			}
			if err := m.CleanupAbandonedStage(context.Background(), collision, "new", attempt); err != nil {
				t.Fatal(err)
			}
			hasVolume(t, recovered(t, client), "new", false)
			if _, err := client.CoreV1().Pods("test").Get(context.Background(), pod.Name, metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}
			hasVolume(t, m, "live", true)

			// An annotation is not permission to unmount an arbitrary host path.
			unsafeKey := "12345678/../../outside"
			unsafePod := m.buildStandalone(lifecycleOptions(unsafeKey), m.getStandaloneName(unsafeKey), "/unused")
			if _, err := client.CoreV1().Pods("test").Create(context.Background(), unsafePod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := m.CleanupOrphanedPods(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().Pods("test").Get(context.Background(), unsafePod.Name, metav1.GetOptions{}); err != nil {
				t.Fatal("unsafe identity was collected:", err)
			}
		})
	}
}
