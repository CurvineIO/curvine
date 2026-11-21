// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func newStateTestManager() *standaloneMountManagerImpl {
	return &standaloneMountManagerImpl{
		client:    fake.NewSimpleClientset(),
		namespace: "curvine",
		nodeName:  "node-1",
		state: &StandaloneState{
			Mounts: make(map[string]*StandaloneStateEntry),
		},
	}
}

func TestSaveStateCreatesConfigMap(t *testing.T) {
	m := newStateTestManager()
	m.state.Mounts["mount-key"] = &StandaloneStateEntry{
		PodName:  "pod-1",
		RefCount: 1,
		Volumes:  []string{"vol-a"},
	}

	if err := m.saveState(context.Background()); err != nil {
		t.Fatalf("saveState: %v", err)
	}

	cm, err := m.client.CoreV1().ConfigMaps(m.namespace).Get(
		context.Background(), StateConfigMapPrefix+m.nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get ConfigMap: %v", err)
	}
	if cm.Labels[StandaloneLabelApp] != StandaloneLabelAppValue {
		t.Fatalf("app label = %q", cm.Labels[StandaloneLabelApp])
	}

	var state StandaloneState
	if err := json.Unmarshal([]byte(cm.Data["state.json"]), &state); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}
	entry, ok := state.Mounts["mount-key"]
	if !ok || entry.RefCount != 1 || len(entry.Volumes) != 1 {
		t.Fatalf("unexpected persisted state: %+v", state.Mounts)
	}
}

func TestSaveStateUpdatesExistingConfigMapAndKeepsOtherData(t *testing.T) {
	m := newStateTestManager()
	ctx := context.Background()

	existing := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:        StateConfigMapPrefix + m.nodeName,
			Namespace:   m.namespace,
			Annotations: map[string]string{"owned-by": "test"},
			Labels:      map[string]string{"extra": "keep-me"},
		},
		Data: map[string]string{
			"state.json": `{"mounts":{}}`,
			"other.json": `{"keep":"me"}`,
		},
	}
	if _, err := m.client.CoreV1().ConfigMaps(m.namespace).Create(ctx, existing, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed ConfigMap: %v", err)
	}

	m.mu.Lock()
	m.state.Mounts["mount-key"] = &StandaloneStateEntry{PodName: "pod-1", Volumes: []string{"vol-a"}, RefCount: 1}
	m.mu.Unlock()

	if err := m.saveState(ctx); err != nil {
		t.Fatalf("saveState: %v", err)
	}

	cm, err := m.client.CoreV1().ConfigMaps(m.namespace).Get(ctx, StateConfigMapPrefix+m.nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get ConfigMap: %v", err)
	}
	if cm.Data["other.json"] != `{"keep":"me"}` {
		t.Fatalf("unrelated data key was dropped: %v", cm.Data)
	}
	if cm.Annotations["owned-by"] != "test" {
		t.Fatalf("annotation was dropped: %v", cm.Annotations)
	}
	if cm.Labels["extra"] != "keep-me" {
		t.Fatalf("label was dropped: %v", cm.Labels)
	}
	if cm.Data["state.json"] == `{"mounts":{}}` {
		t.Fatal("state.json was not updated")
	}
}

// A blind Update without metadata.resourceVersion is rejected by the API
// server; when a write races, the save must re-read and retry instead of
// failing NodeStage.
func TestSaveStateRetriesOnConflict(t *testing.T) {
	cs := fake.NewSimpleClientset()
	m := newStateTestManager()
	m.client = cs
	m.state.Mounts["mount-key"] = &StandaloneStateEntry{PodName: "pod-1", Volumes: []string{"vol-a"}, RefCount: 1}

	var conflicts int32 = 2
	cs.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if atomic.AddInt32(&conflicts, -1) >= 0 {
			return true, nil, apierrors.NewConflict(
				schema.GroupResource{Resource: "configmaps"}, StateConfigMapPrefix+m.nodeName, fmt.Errorf("please apply your changes to the latest version"))
		}
		return false, nil, nil
	})

	if err := m.saveState(context.Background()); err != nil {
		t.Fatalf("saveState did not retry past conflicts: %v", err)
	}

	cm, err := cs.CoreV1().ConfigMaps(m.namespace).Get(context.Background(), StateConfigMapPrefix+m.nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get ConfigMap: %v", err)
	}
	if cm.Data["state.json"] == "" {
		t.Fatal("state.json is empty after retried save")
	}
}
