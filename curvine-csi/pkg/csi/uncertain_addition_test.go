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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// Cover both first-reference Create and shared-mount Update. A failed RPC does
// not tell us whether the API server stored the new cleanup association.
func TestUnstageUncertainAddition(t *testing.T) {
	for _, first := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			for _, restart := range []bool{false, true} {
				t.Run(fmt.Sprintf("first=%v/committed=%v/restart=%v", first, committed, restart), func(t *testing.T) {
					var m *standaloneMountManagerImpl
					var client *fake.Clientset
					verb := "update"
					if first {
						client = fake.NewClientset()
						m = recovered(t, client)
						verb = "create"
					} else {
						m, client = cleanupManager(t, "existing")
					}
					client.ClearActions()
					failed := false
					client.PrependReactor(verb, "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
						if failed {
							return false, nil, nil
						}
						failed = true
						if committed {
							var err error
							if first {
								err = client.Tracker().Create(action.GetResource(), action.(ktesting.CreateAction).GetObject(), "test")
							} else {
								err = client.Tracker().Update(action.GetResource(), action.(ktesting.UpdateAction).GetObject(), "test")
							}
							if err != nil {
								t.Fatal(err)
							}
						}
						return true, nil, errors.New("addition response lost")
					})
					if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err == nil {
						t.Fatal("stage must report uncertain persistence")
					}
					for _, action := range client.Actions() {
						if action.GetResource().Resource == "pods" && action.GetVerb() != "get" {
							t.Fatal("stage mutated resources before confirmed persistence")
						}
					}
					hasVolume(t, m, "new", true)
					restored := recovered(t, client)
					hasVolume(t, restored, "new", committed)
					if restart {
						m = restored
					}

					// Cleanup must remain retryable while persistence is unavailable,
					// including an addition known only to this process's memory.
					unavailable := true
					for _, write := range []string{"create", "update"} {
						client.PrependReactor(write, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
							if unavailable {
								return true, nil, errors.New("API unavailable")
							}
							return false, nil, nil
						})
					}
					n := &nodeServiceStandalone{standaloneManager: m}
					if !restart || committed {
						if err := unstage(n, "new"); err == nil {
							t.Fatal("uncertain cleanup falsely succeeded")
						}
						hasVolume(t, m, "new", true)
					}
					unavailable = false
					for i := 0; i < 2; i++ {
						if err := unstage(n, "new"); err != nil {
							t.Fatal(err)
						}
					}
					hasVolume(t, m, "new", false)
					hasVolume(t, recovered(t, client), "new", false)
					if !first {
						hasVolume(t, recovered(t, client), "existing", true)
						pod, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
						if err != nil || pod.UID != "original" {
							t.Fatalf("shared consumer lost its Pod: %v, %v", pod, err)
						}
						if err := unstage(n, "existing"); err != nil {
							t.Fatal(err)
						}
						if len(recovered(t, client).GetState().Mounts) != 0 {
							t.Fatal("last consumer left stale ownership")
						}
					}
				})
			}
		}
	}
}

func TestUnstageWaitsForUncertainAddition(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	failed := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if failed {
			return false, nil, nil
		}
		failed = true
		if err := client.Tracker().Update(action.GetResource(), action.(ktesting.UpdateAction).GetObject(), "test"); err != nil {
			return true, nil, err
		}
		close(entered)
		<-proceed
		return true, nil, errors.New("addition response lost")
	})
	staged := make(chan error, 1)
	go func() {
		_, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new")
		staged <- err
	}()
	waitSignal(t, entered)
	// Lookup must see the in-flight intent, so Unstage waits on the lifecycle
	// lock instead of returning success while the write is in flight.
	hasVolume(t, m, "new", true)
	cleaned := make(chan error, 1)
	go func() { cleaned <- unstage(&nodeServiceStandalone{standaloneManager: m}, "new") }()
	waitLockUsers(t, &m.lifecycle, cleanupKey, 2)
	once.Do(func() { close(proceed) })
	if err := waitResult(t, staged); err == nil {
		t.Fatal("stage unexpectedly succeeded")
	}
	if err := waitResult(t, cleaned); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "new", false)
	hasVolume(t, m, "existing", true)
}

// Cancellation after the server commit is also uncertain. A subsequent stage
// must reassert the same reference, not double-count it or reset its live peer.
func TestRestageAfterUncertainAddition(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if failed {
			return false, nil, nil
		}
		failed = true
		if err := client.Tracker().Update(action.GetResource(), action.(ktesting.UpdateAction).GetObject(), "test"); err != nil {
			return true, nil, err
		}
		cancel()
		return true, nil, ctx.Err()
	})
	if _, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "new"); err == nil {
		t.Fatal("canceled staging succeeded")
	}
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err != nil {
		t.Fatal(err)
	}
	entry := recovered(t, client).GetState().Mounts[cleanupKey]
	if entry.RefCount != 2 || len(entry.Volumes) != 2 {
		t.Fatalf("retry duplicated ownership: %+v", entry)
	}
	hasVolume(t, m, "existing", true)
	hasVolume(t, m, "new", true)
}

func TestGarbageCollectionReleasesUncertainAddition(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "live-pv"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: DriverName, VolumeHandle: "existing"}}}}
	if _, err := client.CoreV1().PersistentVolumes().Create(context.Background(), pv, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	failed := false
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if failed {
			return false, nil, nil
		}
		failed = true
		if err := client.Tracker().Update(action.GetResource(), action.(ktesting.UpdateAction).GetObject(), "test"); err != nil {
			return true, nil, err
		}
		return true, nil, errors.New("addition response lost")
	})
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "deleted"); err == nil {
		t.Fatal("stage unexpectedly succeeded")
	}
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	n.cleanupOrphanedStandalonePods()
	n.cleanupOrphanedStandalonePods()
	hasVolume(t, recovered(t, client), "deleted", false)
	hasVolume(t, m, "existing", true)
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("shared consumer lost Pod:", err)
	}
}
