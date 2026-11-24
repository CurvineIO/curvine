/*
Copyright 2026 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package csi

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/uuid"
)

func (m *standaloneMountManagerImpl) markOwnershipUnknown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = &StandaloneState{Mounts: make(map[string]*StandaloneStateEntry), OwnershipUnknown: true, RecoveryID: string(uuid.NewUUID())}
}

// Pod readiness is not evidence of propagation into this process's mount
// namespace. Never publish the underlying ordinary directory as a volume.
func (m *standaloneMountManagerImpl) verifyHostMount(ctx context.Context, path string) error {
	if m.mountReady != nil {
		return m.mountReady(ctx, path)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	probe := m.mountCleanup
	if probe == nil {
		probe = newMountCleanup()
	}
	data, err := probe.readInfo()
	if err != nil {
		return err
	}
	if _, err := parseMountInfo(data); err != nil {
		return err
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	found := false
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), " ")
		target, err := unescapeMountPath(fields[4])
		if err != nil {
			return err
		}
		if target != path {
			continue
		}
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				if found || !(fields[i+1] == "fuse.curvinefs" || (fields[i+1] == "fuse" && fields[i+2] == "curvinefs")) {
					return fmt.Errorf("ambiguous or non-Curvine mount at %s", path)
				}
				found = true
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Curvine FUSE mount not visible at %s", path)
	}
	return nil
}

func (m *standaloneMountManagerImpl) ownershipRecovery() (bool, string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.OwnershipUnknown, m.state.RecoveryID
}

func (m *standaloneMountManagerImpl) VerifyMount(ctx context.Context, path string) error {
	return m.verifyHostMount(ctx, path)
}
