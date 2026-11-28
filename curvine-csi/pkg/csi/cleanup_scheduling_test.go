/*
Copyright 2026 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
)

func TestCleanupTasksBoundConcurrencyAndSerializePodGroups(t *testing.T) {
	entered := make(chan struct{}, cleanupWorkers)
	proceed := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	var active, peak, finished atomic.Int32
	jobs := make(map[string][]cleanupTask)
	for i := range cleanupWorkers * 3 {
		var perPod atomic.Int32
		for range 2 {
			jobs[fmt.Sprint(i)] = append(jobs[fmt.Sprint(i)], func(ctx context.Context) error {
				if perPod.Add(1) != 1 {
					t.Error("same Pod ran concurrently")
				}
				now := active.Add(1)
				for old := peak.Load(); now > old; old = peak.Load() {
					if peak.CompareAndSwap(old, now) {
						break
					}
				}
				select {
				case entered <- struct{}{}:
				default:
				}
				<-proceed
				active.Add(-1)
				perPod.Add(-1)
				finished.Add(1)
				return nil
			})
		}
	}
	done := make(chan error, 1)
	go func() { done <- runCleanupTasks(context.Background(), jobs) }()
	for range cleanupWorkers {
		waitSignal(t, entered)
	}
	once.Do(func() { close(proceed) })
	if err := waitResult(t, done); err != nil {
		t.Fatal(err)
	}
	if peak.Load() != cleanupWorkers || finished.Load() != cleanupWorkers*6 {
		t.Fatalf("peak=%d finished=%d", peak.Load(), finished.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runCleanupTasks(ctx, map[string][]cleanupTask{"canceled": {func(context.Context) error { t.Fatal("ran canceled task"); return nil }}}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type blockedPendingManager struct {
	StandaloneMountManager
	entered chan struct{}
	proceed chan struct{}
	orphan  chan struct{}
}

func (m *blockedPendingManager) CleanupAbandonedStage(ctx context.Context, key, volume, attempt string) error {
	close(m.entered)
	select {
	case <-m.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *blockedPendingManager) CleanupOrphanedPods(ctx context.Context) error {
	close(m.orphan)
	return nil
}

func TestCleanupCategoriesProgressWithBlockedPendingStage(t *testing.T) {
	m, client := cleanupManager(t, "pending")
	m.state.Mounts[cleanupKey].PendingStages = map[string]string{"pending": "attempt"}
	wrapped := &blockedPendingManager{StandaloneMountManager: m, entered: make(chan struct{}), proceed: make(chan struct{}), orphan: make(chan struct{})}
	var once sync.Once
	defer once.Do(func() { close(wrapped.proceed) })
	listed := make(chan struct{})
	client.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, runtime.Object, error) { close(listed); return false, nil, nil })
	n := &nodeServiceStandalone{standaloneManager: wrapped, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	done := make(chan struct{})
	go func() { n.cleanupOrphanedStandalonePods(); close(done) }()
	waitSignal(t, wrapped.entered)
	waitSignal(t, wrapped.orphan)
	waitSignal(t, listed)
	select {
	case <-done:
		t.Fatal("pending barrier was bypassed")
	default:
	}
	// Other phases have run while pending cleanup is still blocked.
	once.Do(func() { close(wrapped.proceed) })
	waitSignal(t, done)
}
