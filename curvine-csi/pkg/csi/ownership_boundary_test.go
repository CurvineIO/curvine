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
	"testing"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestLegacyPendingCollisionChecksHostPath(t *testing.T) {
	m, client := cleanupManager(t)
	if _, err := m.beginStageLocked(context.Background(), cleanupKey, "old"); err != nil {
		t.Fatal(err)
	}
	collision := cleanupKey + "-collision"
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(collision), "new"); err == nil {
		t.Fatal("adopted legacy Pod at a different host path")
	}
	if m.mountEntry(collision) != nil {
		t.Fatal("rejected stage created ownership")
	}
	pod, _ := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err := m.validatePodOwnership(pod, cleanupKey); err != nil {
		t.Fatal("legitimate legacy Pod rejected", err)
	}
}

func TestReadyPodRequiresHostFuseMount(t *testing.T) {
	for _, mountType := range []string{"missing", "ext4", "fuse.curvinefs", "fuse"} {
		t.Run(mountType, func(t *testing.T) {
			m, _ := cleanupManager(t)
			m.mountReady = nil
			m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) {
				if mountType == "missing" {
					return []byte(rootMountInfo), nil
				}
				return []byte(rootMountInfo + "22 1 0:22 / " + m.GetHostMountPath(cleanupKey) + " rw - " + mountType + " curvinefs rw\n"), nil
			}}
			_, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new")
			good := mountType == "fuse.curvinefs" || mountType == "fuse"
			if (err == nil) != good {
				t.Fatalf("stage result=%v", err)
			}
			if !good && m.mountEntry(cleanupKey).PendingStages["new"] == "" {
				t.Fatal("failed stage lost cleanup attempt")
			}
		})
	}
}

func TestPendingCleanupConfirmsIntentBeforeDeletion(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	fail := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("write rejected")
		}
		return false, nil, nil
	})
	attempt, err := m.beginStageLocked(ctx, cleanupKey, "pending")
	if err == nil {
		t.Fatal("expected failed persist")
	}
	if err := m.CleanupAbandonedStage(ctx, cleanupKey, "pending", attempt); err == nil {
		t.Fatal("deleted without durable intent")
	}
	if _, err := client.CoreV1().Pods("test").Get(ctx, m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("discovery Pod deleted", err)
	}
	fail = false
	m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) { return nil, errors.New("mountinfo unavailable") }}
	if err := m.CleanupAbandonedStage(ctx, cleanupKey, "pending", attempt); err == nil {
		t.Fatal("expected mount failure")
	}
	restored := recovered(t, client)
	if restored.mountEntry(cleanupKey).PendingStages["pending"] != attempt {
		t.Fatal("intent not durable")
	}
	for range 2 {
		if err := restored.CleanupAbandonedStage(ctx, cleanupKey, "pending", attempt); err != nil {
			t.Fatal(err)
		}
	}
	hasVolume(t, recovered(t, client), "pending", false)
}

func TestLostStateQuarantinesOldPodsAcrossWritesAndRestart(t *testing.T) {
	for _, missing := range []string{"configmap", "state.json"} {
		t.Run(missing, func(t *testing.T) {
			ctx := context.Background()
			m, client := cleanupManager(t, "live")
			pod := m.buildStandalone(lifecycleOptions(cleanupKey), m.getStandaloneName(cleanupKey), m.GetHostMountPath(cleanupKey))
			if _, err := client.CoreV1().Pods("test").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			name := StateConfigMapPrefix + m.nodeName
			if missing == "configmap" {
				if err := client.CoreV1().ConfigMaps("test").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				cm, _ := client.CoreV1().ConfigMaps("test").Get(ctx, name, metav1.GetOptions{})
				delete(cm.Data, "state.json")
				if _, err := client.CoreV1().ConfigMaps("test").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			m = recovered(t, client)
			if err := m.saveState(ctx); err != nil {
				t.Fatal(err)
			}
			m = recovered(t, client)
			if !m.GetState().OwnershipUnknown {
				t.Fatal("new write restored false trust")
			}
			for range 2 {
				if err := m.CleanupOrphanedPods(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.DeleteStandalone(ctx, cleanupKey); err == nil {
				t.Fatal("direct cleanup accepted unknown owner")
			}
			if _, err := client.CoreV1().Pods("test").Get(ctx, pod.Name, metav1.GetOptions{}); err != nil {
				t.Fatal("live Pod deleted")
			}
			// Fresh resources in this recovery epoch remain reclaimable.
			key := "fresh-distinct-key"
			fresh := m.buildStandalone(lifecycleOptions(key), m.getStandaloneName(key), m.GetHostMountPath(key))
			if _, err := client.CoreV1().Pods("test").Create(ctx, fresh, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := m.CleanupOrphanedPods(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().Pods("test").Get(ctx, fresh.Name, metav1.GetOptions{}); err == nil {
				t.Fatal("fresh orphan not reclaimed")
			}
		})
	}
}

func TestEmptyCleanupDoesNotDependOnPVList(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) { return nil, errors.New("unavailable") }}
	if err := m.DeleteStandalone(ctx, cleanupKey); err == nil {
		t.Fatal("expected host cleanup failure")
	}
	m = recovered(t, client)
	client.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("forbidden") })
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	for range 2 {
		_ = n.cleanupDeletedPVs(ctx, m.GetState())
	}
	if m.mountEntry(cleanupKey) != nil {
		t.Fatal("empty cleanup blocked by PV listing")
	}
}

func TestNodeUnstageWaitsForStagePreflight(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	setTestHostMountBase(t, m, client, t.TempDir())
	readyCreatedPods(client)
	entered, proceed := make(chan struct{}), make(chan struct{})
	first := true
	client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if first {
			first = false
			close(entered)
			<-proceed
		}
		return false, nil, nil
	})
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	stage := make(chan error, 1)
	go func() {
		_, err := n.NodeStageVolume(ctx, &csipb.NodeStageVolumeRequest{VolumeId: "cluster@/@volume", VolumeContext: map[string]string{"master-addrs": "localhost:8995"}})
		stage <- err
	}()
	waitSignal(t, entered)
	timed, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := n.NodeUnstageVolume(timed, &csipb.NodeUnstageVolumeRequest{VolumeId: "cluster@/@volume"}); err == nil {
		t.Fatal("unstage acknowledged in-flight stage")
	}
	close(proceed)
	if err := waitResult(t, stage); err != nil {
		t.Fatal(err)
	}
	if _, err := n.NodeUnstageVolume(ctx, &csipb.NodeUnstageVolumeRequest{VolumeId: "cluster@/@volume"}); err != nil {
		t.Fatal(err)
	}
}

func TestMatchingLatePodCanBeDeletedDespiteCollidingOwner(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	a := cleanupKey
	b := a + "-collision"
	attempt, err := m.beginStageLocked(ctx, a, "abandoned")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.addVolumeRefLocked(ctx, b, "live"); err != nil {
		t.Fatal(err)
	}
	pod := m.buildStandalone(lifecycleOptions(a), m.getStandaloneName(a), m.GetHostMountPath(a))
	pod.UID = "late-a"
	if _, err := client.CoreV1().Pods("test").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.CleanupAbandonedStage(ctx, a, "abandoned", attempt); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "live", true)
	if _, err := client.CoreV1().Pods("test").Get(ctx, pod.Name, metav1.GetOptions{}); err == nil {
		t.Fatal("late A still blocks B")
	}
	setTestHostMountBase(t, m, client, t.TempDir())
	readyCreatedPods(client)
	if _, err := m.EnsureVolume(ctx, lifecycleOptions(b), "live"); err != nil {
		t.Fatal("B failed to recover", err)
	}
}

func TestLateReadyPodAfterCleanupCannotPublishLocalDirectory(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t)
	setTestHostMountBase(t, m, client, t.TempDir())
	m.mountReady = nil
	name := m.getStandaloneName(cleanupKey)
	if err := client.CoreV1().Pods("test").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	attempt, err := m.beginStageLocked(ctx, cleanupKey, "old")
	if err != nil {
		t.Fatal(err)
	}
	mounted := true
	late := true
	m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) {
		if mounted {
			return targetMountInfo(m.GetHostMountPath(cleanupKey)), nil
		}
		return []byte(rootMountInfo), nil
	}, run: func(context.Context, ...string) ([]byte, error) {
		if late {
			late = false
			pod := m.buildStandalone(lifecycleOptions(cleanupKey), name, m.GetHostMountPath(cleanupKey))
			pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
			if _, err := client.CoreV1().Pods("test").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		mounted = false
		return nil, nil
	}}
	if err := m.CleanupAbandonedStage(ctx, cleanupKey, "old", attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "new"); err == nil {
		t.Fatal("Ready-but-unmounted Pod acknowledged stage")
	}
	entry := m.mountEntry(cleanupKey)
	if entry == nil || entry.PendingStages["new"] == "" {
		t.Fatal("late Pod lost retry owner")
	}
	if _, err := m.EnsureVolume(ctx, lifecycleOptions(cleanupKey), "new"); err == nil {
		t.Fatal("published incomplete stage")
	}
	if err := m.CleanupAbandonedStage(ctx, cleanupKey, "new", entry.PendingStages["new"]); err != nil {
		t.Fatal(err)
	}
	readyCreatedPods(client)
	client.PrependReactor("create", "pods", func(ktesting.Action) (bool, runtime.Object, error) { mounted = true; return false, nil, nil })
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "new"); err != nil {
		t.Fatal("restage did not recover", err)
	}
}
