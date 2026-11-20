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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/uuid"
)

const standaloneMountKeyAnnotation = "curvine.io/standalone-mount-key"

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
					return fmt.Errorf("standalone %s is also referenced by a different mount key", podName)
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
	var failures []error
	for _, pod := range pods.Items {
		key := pod.Annotations[standaloneMountKeyAnnotation]
		if key == "" || key == "." || key == ".." || strings.ContainsAny(key, "/\\") ||
			pod.Name != m.getStandaloneName(key) || pod.Spec.NodeName != m.nodeName {
			continue
		}
		if entry := m.mountEntry(key); entry != nil && len(entry.Volumes) > 0 {
			continue
		}
		if err := m.DeleteStandalone(ctx, key); err != nil {
			// A referenced mount is normal, not a collection error. DeleteStandalone
			// performs the authoritative check after acquiring its lifecycle lock.
			if entry := m.mountEntry(key); entry == nil || len(entry.Volumes) == 0 {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}
