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
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/uuid"
)

const standaloneRecoveryAnnotation = "curvine.io/ownership-recovery-id"

const standaloneMountKeyAnnotation = "curvine.io/standalone-mount-key"

var errStandaloneOwnership = errors.New("standalone ownership conflict")

// Names are shortened and may collide, including across nodes. Legacy Pods
// lack the annotation, but still have an authoritative spec.nodeName.
func (m *standaloneMountManagerImpl) validatePodOwnership(pod *corev1.Pod, key string) error {
	unknown, recoveryID := m.ownershipRecovery()
	if unknown && pod.Annotations[standaloneRecoveryAnnotation] != recoveryID {
		return fmt.Errorf("%w: Pod ownership predates lost state", errStandaloneOwnership)
	}
	if pod.Name != m.getStandaloneName(key) || pod.Spec.NodeName != m.nodeName {
		return fmt.Errorf("%w: Pod %s belongs to node %s, expected %s", errStandaloneOwnership, pod.Name, pod.Spec.NodeName, m.nodeName)
	}
	if owner := pod.Annotations[standaloneMountKeyAnnotation]; owner != "" && owner != key {
		return fmt.Errorf("%w: Pod %s belongs to mount key %s", errStandaloneOwnership, pod.Name, owner)
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "fuse-mount" && volume.HostPath != nil && volume.HostPath.Path == m.GetHostMountPath(key) {
			return nil
		}
	}
	return fmt.Errorf("%w: Pod %s has no matching FUSE host path", errStandaloneOwnership, pod.Name)
}

func (m *standaloneMountManagerImpl) validateStageOwnership(ctx context.Context, key string) error {
	if err := m.checkPodOwnership(key); err != nil {
		return err
	}
	pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, m.getStandaloneName(key), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return m.validatePodOwnership(pod, key)
}

// Caller holds the physical Pod lifecycle lock. Legacy Pods lack a full-key
// annotation, so also protect ownership recorded under a colliding mount key.
func (m *standaloneMountManagerImpl) checkPodOwnership(mountKey string) error {
	podName := m.getStandaloneName(mountKey)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for key, entry := range m.state.Mounts {
		if key != mountKey && m.getStandaloneName(key) == podName {
			for _, volume := range entry.Volumes {
				if entry.PendingStages[volume] == "" {
					return fmt.Errorf("%w: standalone %s is also referenced by a different mount key", errStandaloneOwnership, podName)
				}
			}
		}
	}
	return nil
}

// Caller holds the mount lifecycle lock through readiness and promotion. A
// retry of established ownership must never turn a live consumer into garbage.
func (m *standaloneMountManagerImpl) beginStageLocked(ctx context.Context, key, volume string) (string, error) {
	entry := m.mountEntry(key)
	if entry != nil && slices.Contains(entry.Volumes, volume) && entry.PendingStages[volume] == "" {
		return "", m.addVolumeRefLocked(ctx, key, volume)
	}
	attempt := string(uuid.NewUUID())
	err := m.updateState(ctx, func(state *StandaloneState) bool {
		entry := state.Mounts[key]
		if entry == nil {
			entry = &StandaloneStateEntry{PodName: m.getStandaloneName(key), CreatedAt: time.Now()}
			state.Mounts[key] = entry
		}
		if !slices.Contains(entry.Volumes, volume) {
			entry.Volumes = append(entry.Volumes, volume)
		}
		entry.RefCount = len(entry.Volumes)
		if entry.PendingStages == nil {
			entry.PendingStages = make(map[string]string)
		}
		entry.PendingStages[volume] = attempt
		return true
	})
	return attempt, err
}

// The snapshot token is only a candidate. Recheck it after waiting for an
// in-flight stage: successful staging removes it; a retry replaces it. This
// also covers process death before readiness because the intent is durable.
func (m *standaloneMountManagerImpl) CleanupAbandonedStage(ctx context.Context, key, volume, attempt string) error {
	ctx, cancel := context.WithTimeout(ctx, standaloneCleanupTimeout)
	defer cancel()
	unlock, err := m.lifecycle.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer unlock()
	entry := m.mountEntry(key)
	if attempt == "" || entry == nil || entry.PendingStages[volume] != attempt {
		return nil
	}
	// Memory contains uncertain additions too. Confirm durable intent before
	// deleting the Pod that might otherwise be the only discovery source.
	if err := m.saveState(ctx); err != nil {
		return err
	}
	if len(entry.Volumes) == 1 && entry.Volumes[0] == volume {
		// A rejected/late collision must not require the healthy owner to leave.
		// Keep this reference until this key's local host mount is cleaned, but
		// never delete a Pod owned by another mount key or node.
		return m.deleteStandaloneResourcesLocked(ctx, key, true)
	}
	return m.releaseVolumeLocked(ctx, key, volume)
}

// A canceled Pod Create can still commit after reference cleanup observed
// NotFound. Discover such late Pods independently of the ConfigMap. Only Pods
// annotated by this implementation, for this node and deterministic mount
// identity, qualify. Recheck current ownership under the same lifecycle lock
// used by staging so a stale Pod-list snapshot cannot delete a restaged mount.
func (m *standaloneMountManagerImpl) CleanupOrphanedPods(ctx context.Context) error {
	listCtx, cancel := context.WithTimeout(ctx, volumeCleanupTimeout)
	pods, err := m.client.CoreV1().Pods(m.namespace).List(listCtx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{
			StandaloneLabelApp:  StandaloneLabelAppValue,
			StandaloneLabelNode: m.nodeName,
		}).String(),
	})
	cancel()
	if err != nil {
		return err
	}
	jobs := make(map[string][]cleanupTask)
	for _, pod := range pods.Items {
		key := pod.Annotations[standaloneMountKeyAnnotation]
		unknown, recoveryID := m.ownershipRecovery()
		if unknown && pod.Annotations[standaloneRecoveryAnnotation] != recoveryID {
			continue
		}
		if key == "" || key == "." || key == ".." || strings.ContainsAny(key, "/\\") ||
			pod.Name != m.getStandaloneName(key) || pod.Spec.NodeName != m.nodeName {
			continue
		}
		if entry := m.mountEntry(key); entry != nil && len(entry.Volumes) > 0 {
			continue
		}
		jobs[pod.Name] = append(jobs[pod.Name], func(ctx context.Context) error {
			if err := m.DeleteStandalone(ctx, key); err != nil {
				// The authoritative reference check is under the lifecycle lock.
				if entry := m.mountEntry(key); entry == nil || len(entry.Volumes) == 0 {
					return err
				}
			}
			return nil
		})
	}
	return runCleanupTasks(ctx, jobs)
}

const cleanupWorkers = 4

type cleanupTask func(context.Context) error

// Group by physical Pod identity so one busy mount cannot occupy every worker.
// Categories use separate pools; lifecycle locks still serialize shared Pods.
func runCleanupTasks(ctx context.Context, groups map[string][]cleanupTask) error {
	jobs := make(chan []cleanupTask, len(groups))
	for _, tasks := range groups {
		jobs <- tasks
	}
	close(jobs)
	var workers sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for range min(cleanupWorkers, len(groups)) {
		workers.Go(func() {
			for tasks := range jobs {
				for _, task := range tasks {
					if ctx.Err() != nil {
						return
					}
					if err := task(ctx); err != nil {
						mu.Lock()
						failures = append(failures, err)
						mu.Unlock()
					}
				}
			}
		})
	}
	workers.Wait()
	return errors.Join(append(failures, ctx.Err())...)
}
