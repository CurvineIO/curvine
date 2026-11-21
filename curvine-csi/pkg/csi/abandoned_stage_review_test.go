/*
Copyright 2026 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestStagePromotionMissingEntryFailsWithoutLosingCleanup(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	injected := false
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		entry := m.mountEntry(cleanupKey)
		if !injected && entry != nil && entry.PendingStages["new"] != "" {
			// Fault injection: normal lifecycle locking prevents this disappearance.
			// Drop only the in-memory entry after intent persistence, before promotion.
			m.mu.Lock()
			delete(m.state.Mounts, cleanupKey)
			m.mu.Unlock()
			injected = true
		}
		return false, nil, nil
	})
	path, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "new")
	if !injected {
		t.Fatal("missing-entry fault was not injected")
	}
	if err == nil || !strings.Contains(err.Error(), "missing stage state") || path != "" {
		t.Fatalf("missing ownership must not acknowledge staging: path=%q err=%v", path, err)
	}
	pod, err := client.CoreV1().Pods("test").Get(ctx, m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err != nil || pod.UID != "original" {
		t.Fatalf("failed promotion disturbed the Pod: %v", err)
	}
	m = recovered(t, client)
	entry := m.mountEntry(cleanupKey)
	if entry == nil || entry.PendingStages["new"] == "" {
		t.Fatal("failed promotion erased durable cleanup ownership")
	}
	for range 2 {
		if err := m.CleanupAbandonedStage(ctx, cleanupKey, "new", entry.PendingStages["new"]); err != nil {
			t.Fatal(err)
		}
	}
	hasVolume(t, recovered(t, client), "new", false)
}

func TestGCSnapshotPendingCannotReleaseRestagedVolume(t *testing.T) {
	m, client := cleanupManager(t)
	setTestHostMountBase(t, m, client, t.TempDir())
	failNewStage(t, m, client, "v")
	snapshot := m.GetState()
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	if err := n.cleanupPendingStages(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	readyCreatedPods(client)
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "v"); err != nil {
		t.Fatal(err)
	}
	retainPV(t, client, "v")
	// Model a PV List that observed absence before recreation/restaging.
	client.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, runtime.Object, error) { return true, &corev1.PersistentVolumeList{}, nil })
	if err := n.cleanupDeletedPVs(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "v", true)
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("restaged Pod was deleted:", err)
	}
}

func TestStageRejectsLateCreateCollision(t *testing.T) {
	client := fake.NewClientset()
	m := recovered(t, client)
	setTestHostMountBase(t, m, client, t.TempDir())
	var late *corev1.Pod
	client.PrependReactor("create", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		if late == nil {
			late = a.(ktesting.CreateAction).GetObject().(*corev1.Pod).DeepCopy()
			return true, nil, context.DeadlineExceeded
		}
		late.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
		if err := client.Tracker().Add(late); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, late.Name)
	})
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "a"); err == nil {
		t.Fatal("expected timeout")
	}
	attempt := m.mountEntry(cleanupKey).PendingStages["a"]
	if err := m.CleanupAbandonedStage(context.Background(), cleanupKey, "a", attempt); err != nil {
		t.Fatal(err)
	}
	collision := cleanupKey + "-collision"
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(collision), "b"); err == nil {
		t.Fatalf("stage B adopted late Pod A; pending=%q", m.mountEntry(collision).PendingStages["b"])
	}
	attempt = m.mountEntry(collision).PendingStages["b"]
	if err := m.CleanupAbandonedStage(context.Background(), collision, "b", attempt); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "b", false)
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), late.Name, metav1.GetOptions{}); err != nil {
		t.Fatal("foreign Pod deleted:", err)
	}
}

func TestOtherNodePodOwnership(t *testing.T) {
	for _, action := range []string{"adopt", "cleanup", "readiness", "recovery", "release"} {
		t.Run(action, func(t *testing.T) {
			client := fake.NewClientset()
			a := NewStandaloneManager(client, "test", "node-0", "image", "sa").(*standaloneMountManagerImpl)
			b := NewStandaloneManager(client, "test", "node-1", "image", "sa").(*standaloneMountManagerImpl)
			b.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) { return []byte(rootMountInfo), nil }}
			b.hostMountBaseDir = t.TempDir()
			if a.getStandaloneName(cleanupKey) != b.getStandaloneName(cleanupKey) {
				t.Fatal("fixture does not collide")
			}
			opts := lifecycleOptions(cleanupKey)
			opts.NodeName = "node-0"
			pod := a.buildStandalone(opts, a.getStandaloneName(cleanupKey), "/unused")
			pod.UID = "node0-pod"
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			if _, err := client.CoreV1().Pods("test").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			switch action {
			case "adopt":
				opts.NodeName = "node-1"
				if _, err := b.StageVolume(context.Background(), opts, "b"); err == nil {
					t.Fatal("node-1 accepted node-0 ready Pod")
				}
				if b.mountEntry(cleanupKey) != nil {
					t.Fatal("foreign node created local reference")
				}
			case "readiness":
				if err := b.WaitForStandaloneReady(context.Background(), cleanupKey, time.Second); err == nil {
					t.Fatal("foreign node passed readiness")
				}
			case "recovery":
				if _, err := b.beginStageLocked(context.Background(), cleanupKey, "b"); err != nil {
					t.Fatal(err)
				}
				pod.Status.Phase = corev1.PodFailed
				if _, err := client.CoreV1().Pods("test").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := b.RecoverState(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "release":
				if err := b.AddVolumeRef(context.Background(), cleanupKey, "b"); err != nil {
					t.Fatal(err)
				}
				if err := b.ReleaseVolume(context.Background(), cleanupKey, "b"); err == nil {
					t.Fatal("foreign node accepted established release")
				}
				hasVolume(t, b, "b", true)
			case "cleanup":
				attempt, err := b.beginStageLocked(context.Background(), cleanupKey, "b")
				if err != nil {
					t.Fatal(err)
				}
				if err := b.CleanupAbandonedStage(context.Background(), cleanupKey, "b", attempt); err != nil {
					t.Fatal(err)
				}
				hasVolume(t, b, "b", false)
			}
			current, err := client.CoreV1().Pods("test").Get(context.Background(), pod.Name, metav1.GetOptions{})
			if err != nil || current.UID != pod.UID {
				t.Fatal("foreign node Pod changed:", err)
			}
		})
	}
}

func TestReadinessOwnershipConflictDoesNotTriggerSelfHealing(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	pod, err := client.CoreV1().Pods("test").Get(ctx, m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = nil
	if _, err := client.CoreV1().Pods("test").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	gets := 0
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 3 { // preflight, ensure, then first readiness poll
			pod.Spec.NodeName = "other-node"
			pod.UID = "replacement"
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
			if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod, "test"); err != nil {
				t.Fatal(err)
			}
		}
		return false, nil, nil
	})
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "v"); err == nil {
		t.Fatal("accepted foreign readiness")
	}
	if m.mountEntry(cleanupKey).PendingStages["v"] == "" {
		t.Fatal("promoted foreign readiness")
	}
	for _, a := range client.Actions() {
		if a.GetVerb() == "delete" && a.GetResource().Resource == "pods" {
			t.Fatal("ownership conflict triggered deletion")
		}
	}
}

func TestSelfHealingDeletionFencesReplacementUID(t *testing.T) {
	for _, duringDelete := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-delete", true: "during-poll"}[duringDelete], func(t *testing.T) {
			ctx := context.Background()
			m, client := cleanupManager(t, "live")
			observed, err := client.CoreV1().Pods("test").Get(ctx, m.getStandaloneName(cleanupKey), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			replacement := observed.DeepCopy()
			replacement.UID = "replacement"
			resource := corev1.SchemeGroupVersion.WithResource("pods")
			client.PrependReactor("delete", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
				options := a.(ktesting.DeleteAction).GetDeleteOptions()
				if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != observed.UID {
					t.Fatal("missing observed-UID precondition")
				}
				if err := client.Tracker().Update(resource, replacement, "test"); err != nil {
					t.Fatal(err)
				}
				if !duringDelete {
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, observed.Name, context.Canceled)
				}
				// Accepted deletion of the old UID, followed by a replacement before polling.
				return true, nil, nil
			})
			if err := m.deleteStandaloneAndWait(ctx, cleanupKey, observed); err == nil {
				t.Fatal("replacement acknowledged as deleted")
			}
			current, err := client.CoreV1().Pods("test").Get(ctx, observed.Name, metav1.GetOptions{})
			if err != nil || current.UID != replacement.UID {
				t.Fatal("replacement lost", err)
			}
		})
	}
}
