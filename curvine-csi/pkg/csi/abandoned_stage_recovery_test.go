/*
Copyright 2026 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestOrphanUnmountFailureRemainsDiscoverable(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	m := recovered(t, client)
	// A delayed Create arrived after earlier cleanup dropped the state entry.
	pod := m.buildStandalone(lifecycleOptions(cleanupKey), m.getStandaloneName(cleanupKey), m.GetHostMountPath(cleanupKey))
	pod.UID = "late-create"
	if _, err := client.CoreV1().Pods("test").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	present, fail := true, true
	calls := 0
	mount := &mountCleanup{
		readInfo: func() ([]byte, error) {
			if present {
				return targetMountInfo(m.GetHostMountPath(cleanupKey)), nil
			}
			return []byte(rootMountInfo), nil
		},
		run: func(context.Context, ...string) ([]byte, error) {
			calls++
			if fail {
				return nil, errors.New("unmount temporarily busy")
			}
			present = false
			return nil, nil
		},
	}
	m.mountCleanup = mount
	if err := m.CleanupOrphanedPods(ctx); err == nil {
		t.Fatal("first cleanup must fail at unmount")
	}
	if _, err := client.CoreV1().Pods("test").Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("fixture Pod still exists: %v", err)
	}
	firstCalls := calls
	fail = false
	m = recovered(t, client)
	m.mountCleanup = mount
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	for range 2 {
		n.cleanupOrphanedStandalonePods()
	}
	if present {
		t.Fatalf("host mount stranded after restart and two GC passes: unmount calls first=%d total=%d, durable mounts=%d", firstCalls, calls, len(m.GetState().Mounts))
	}
}

func TestRejectedCollisionEventuallyReleasesReference(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t, "live")
	pod, err := client.CoreV1().Pods("test").Get(ctx, m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations = map[string]string{standaloneMountKeyAnnotation: cleanupKey}
	if _, err := client.CoreV1().Pods("test").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	rejectedKey := cleanupKey + "-collision"
	if m.getStandaloneName(rejectedKey) != pod.Name {
		t.Fatal("fixture must collide")
	}
	if _, err := m.StageVolume(ctx, lifecycleOptions(rejectedKey), "rejected"); err == nil {
		t.Fatal("colliding stage must be rejected")
	}
	if m.mountEntry(rejectedKey) != nil {
		t.Fatal("known conflict created ownership")
	}
	// Seed the older implementation's already-recorded pending attempt.
	if _, err := m.beginStageLocked(ctx, rejectedKey, "rejected"); err != nil {
		t.Fatal(err)
	}
	retainPV(t, client, "live")
	retainPV(t, client, "rejected")
	m = recovered(t, client)
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	for range 2 {
		n.cleanupOrphanedStandalonePods()
	}
	hasVolume(t, m, "live", true)
	current, err := client.CoreV1().Pods("test").Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || current.UID != pod.UID {
		t.Fatal("live owner was disturbed", err)
	}
	if _, found := m.FindMountKeyByVolumeID("rejected"); found {
		t.Fatal("rejected pending reference remains after restart and two GC passes; cleanup requires the healthy colliding owner to leave")
	}
}

func TestOrphanCleanupPersistsBeforePodDeletion(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "response-lost"}[committed], func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewClientset()
			m := recovered(t, client)
			pod := m.buildStandalone(lifecycleOptions(cleanupKey), m.getStandaloneName(cleanupKey), m.GetHostMountPath(cleanupKey))
			if _, err := client.CoreV1().Pods("test").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			fail := true
			client.PrependReactor("create", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
				if !fail {
					return false, nil, nil
				}
				if committed {
					if err := client.Tracker().Add(a.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)); err != nil {
						t.Fatal(err)
					}
				}
				return true, nil, context.DeadlineExceeded
			})
			if err := m.CleanupOrphanedPods(ctx); err == nil {
				t.Fatal("unconfirmed intent accepted")
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" && a.GetResource().Resource == "pods" {
					t.Fatal("deleted before confirmed persistence")
				}
			}
			fail = false
			// Restart must rediscover either the Pod or the committed cleanup record.
			m = recovered(t, client)
			if err := m.CleanupOrphanedPods(ctx); err != nil {
				t.Fatal(err)
			}
			if len(recovered(t, client).GetState().Mounts) != 0 {
				t.Fatal("completed orphan left state")
			}
		})
	}
}

func TestRejectedCollisionRetainsLocalCleanupAfterMountFailure(t *testing.T) {
	ctx := context.Background()
	m, client := cleanupManager(t, "live")
	key := cleanupKey + "-collision"
	attempt, err := m.beginStageLocked(ctx, key, "rejected")
	if err != nil {
		t.Fatal(err)
	}
	m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) { return nil, errors.New("mountinfo unavailable") }}
	if err := m.CleanupAbandonedStage(ctx, key, "rejected", attempt); err == nil {
		t.Fatal("failed local cleanup acknowledged")
	}
	m = recovered(t, client)
	hasVolume(t, m, "rejected", true)
	if err := m.CleanupAbandonedStage(ctx, key, "rejected", attempt); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "rejected", false)
	hasVolume(t, m, "live", true)
	pod, err := client.CoreV1().Pods("test").Get(ctx, m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err != nil || pod.UID != "original" {
		t.Fatal("live owner changed", err)
	}
}
