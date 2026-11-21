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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/retry"
)

// The default fake tracker does not enforce resourceVersion or model no-op
// updates. Model both explicitly, including Kubernetes' unconditional updates
// when resourceVersion is omitted. Tests drive delayed completion sequentially.
func versionedConfigMapUpdates(t *testing.T, m *standaloneMountManagerImpl, client *fake.Clientset) func(*corev1.ConfigMap) error {
	t.Helper()
	resource := corev1.SchemeGroupVersion.WithResource("configmaps")
	name := StateConfigMapPrefix + m.nodeName
	obj, err := client.Tracker().Get(resource, m.namespace, name)
	if err != nil {
		t.Fatal(err)
	}
	initial := obj.(*corev1.ConfigMap).DeepCopy()
	initial.ResourceVersion = "revision-0"
	if err := client.Tracker().Update(resource, initial, m.namespace); err != nil {
		t.Fatal(err)
	}
	revision := 0
	apply := func(candidate *corev1.ConfigMap) error {
		obj, err := client.Tracker().Get(resource, m.namespace, name)
		if err != nil {
			return err
		}
		current := obj.(*corev1.ConfigMap)
		if candidate.ResourceVersion != "" && candidate.ResourceVersion != current.ResourceVersion {
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, name, fmt.Errorf("stale resourceVersion"))
		}
		next := candidate.DeepCopy()
		next.ResourceVersion = current.ResourceVersion
		if reflect.DeepEqual(next, current) {
			return nil
		}
		revision++
		next.ResourceVersion = fmt.Sprintf("revision-%d", revision)
		return client.Tracker().Update(resource, next, m.namespace)
	}
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		candidate := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
		if err := apply(candidate); err != nil {
			return true, nil, err
		}
		obj, err := client.Tracker().Get(resource, m.namespace, name)
		return true, obj, err
	})
	return apply
}

func TestDelayedCleanupCannotEraseRestagedOwnership(t *testing.T) {
	for _, directEnsure := range []bool{false, true} {
		for _, completesFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("ensure=%v/cleanup-first=%v", directEnsure, completesFirst), func(t *testing.T) {
				m, client := cleanupManager(t, "same")
				setTestHostMountBase(t, m, client, t.TempDir())
				apply := versionedConfigMapUpdates(t, m, client)
				var delayed *corev1.ConfigMap
				client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					if delayed != nil {
						return false, nil, nil
					}
					delayed = action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
					// The client sees a timeout, but the server request remains pending.
					return true, nil, context.DeadlineExceeded
				})
				if err := m.ReleaseVolume(context.Background(), cleanupKey, "same"); err == nil {
					t.Fatal("cleanup unexpectedly succeeded")
				}
				if delayed == nil {
					t.Fatal("cleanup did not reach persistence")
				}
				hasVolume(t, m, "same", true)
				if completesFirst {
					if err := apply(delayed); err != nil {
						t.Fatal(err)
					}
					hasVolume(t, recovered(t, client), "same", false)
				}
				readyCreatedPods(client)
				var err error
				if directEnsure {
					_, err = m.EnsureStandalone(context.Background(), lifecycleOptions(cleanupKey))
				} else {
					_, err = m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "same")
				}
				if err != nil {
					t.Fatal(err)
				}
				if !completesFirst {
					if err := apply(delayed); !apierrors.IsConflict(err) {
						t.Fatalf("late cleanup must conflict, got %v", err)
					}
				}
				restored := recovered(t, client)
				restored.hostMountBaseDir = m.hostMountBaseDir
				hasVolume(t, restored, "same", true)
				if err := unstage(&nodeServiceStandalone{standaloneManager: restored}, "same"); err != nil {
					t.Fatal(err)
				}
				hasVolume(t, recovered(t, client), "same", false)
				_, err = client.CoreV1().Pods(m.namespace).Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
				if !apierrors.IsNotFound(err) {
					t.Fatalf("recovered cleanup left a Pod: %v", err)
				}
			})
		}
	}
}

func TestPersistStateReassertionAdvancesVersion(t *testing.T) {
	m, client := cleanupManager(t, "same")
	versionedConfigMapUpdates(t, m, client)
	ctx := context.Background()
	maps := client.CoreV1().ConfigMaps(m.namespace)
	before, err := maps.Get(ctx, StateConfigMapPrefix+m.nodeName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.saveState(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := maps.Get(ctx, before.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if before.Data["state.json"] != after.Data["state.json"] {
		t.Fatal("ownership changed during reassertion")
	}
	if before.ResourceVersion == after.ResourceVersion {
		t.Fatal("identical ownership reassertion did not fence pending writes")
	}
}

func TestPersistStateConflictPreservesOtherMetadata(t *testing.T) {
	m, client := cleanupManager(t, "same")
	apply := versionedConfigMapUpdates(t, m, client)
	ctx := context.Background()
	maps := client.CoreV1().ConfigMaps(m.namespace)
	name := StateConfigMapPrefix + m.nodeName
	attempts := 0
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts != 1 {
			return false, nil, nil
		}
		// An unrelated actor updates the ConfigMap between our Get and Update.
		obj, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), m.namespace, name)
		if err != nil {
			t.Fatal(err)
		}
		current := obj.(*corev1.ConfigMap).DeepCopy()
		current.Data["other"] = "keep-data"
		current.Labels["other"] = "keep-label"
		current.Annotations["other"] = "keep-annotation"
		if err := apply(current); err != nil {
			t.Fatal(err)
		}
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, name, fmt.Errorf("concurrent update"))
	})
	if err := m.AddVolumeRef(ctx, cleanupKey, "new"); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("update attempts=%d, want 2", attempts)
	}
	cm, err := maps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["other"] != "keep-data" || cm.Labels["other"] != "keep-label" || cm.Annotations["other"] != "keep-annotation" {
		t.Fatalf("unrelated metadata lost: %+v", cm)
	}
	if cm.Annotations[stateWriteIDAnnotation] == "" {
		t.Fatal("missing write ID")
	}
	restored := recovered(t, client)
	hasVolume(t, restored, "same", true)
	hasVolume(t, restored, "new", true)
}

func TestPersistStateCreateAndConcurrentCreate(t *testing.T) {
	for _, raced := range []bool{false, true} {
		t.Run(fmt.Sprint(raced), func(t *testing.T) {
			client := fake.NewClientset()
			m := recovered(t, client)
			creates, updates := 0, 0
			client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				creates++
				cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
				if cm.ResourceVersion != "" {
					t.Fatal("Create must not specify resourceVersion")
				}
				if !raced {
					return false, nil, nil
				}
				// Another actor creates the object after our NotFound response.
				competitor := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cm.Name, Namespace: cm.Namespace, ResourceVersion: "created-elsewhere", Annotations: map[string]string{"other": "keep"}}}
				if err := client.Tracker().Create(action.GetResource(), competitor, m.namespace); err != nil {
					t.Fatal(err)
				}
				return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "configmaps"}, cm.Name)
			})
			client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				updates++
				cm := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap)
				if !raced || cm.ResourceVersion != "created-elsewhere" || cm.Annotations["other"] != "keep" {
					t.Fatalf("retry did not reread created object: %+v", cm)
				}
				return false, nil, nil
			})
			if err := m.AddVolumeRef(context.Background(), cleanupKey, "new"); err != nil {
				t.Fatal(err)
			}
			if creates != 1 || (raced && updates != 1) || (!raced && updates != 0) {
				t.Fatalf("creates=%d updates=%d", creates, updates)
			}
			hasVolume(t, recovered(t, client), "new", true)
		})
	}
}

func TestPersistStateFailuresRetainOwnership(t *testing.T) {
	for _, mode := range []string{"get", "conflict", "timeout", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			m, client := cleanupManager(t, "same")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failed, attempts := true, 0
			verb := "update"
			if mode == "get" {
				verb = "get"
			}
			client.PrependReactor(verb, "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
				if !failed {
					return false, nil, nil
				}
				attempts++
				if mode == "conflict" || mode == "cancel" {
					if mode == "cancel" {
						cancel()
					}
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "state", fmt.Errorf("conflict"))
				}
				return true, nil, context.DeadlineExceeded
			})
			client.ClearActions()
			_, err := m.StageVolume(ctx, lifecycleOptions(cleanupKey), "new")
			if err == nil {
				t.Fatal("stage succeeded after persistence failure")
			}
			switch mode {
			case "conflict":
				if !apierrors.IsConflict(err) || attempts != retry.DefaultRetry.Steps {
					t.Fatalf("conflict not propagated after bounded retry: attempts=%d err=%v", attempts, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || attempts != 1 {
					t.Fatalf("cancellation not propagated: attempts=%d err=%v", attempts, err)
				}
			default:
				if !errors.Is(err, context.DeadlineExceeded) || attempts != 1 {
					t.Fatalf("ambiguous error retried or lost: attempts=%d err=%v", attempts, err)
				}
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource == "pods" && action.GetVerb() != "get" {
					t.Fatal("resources mutated after persistence failure")
				}
			}
			hasVolume(t, m, "same", true)
			hasVolume(t, m, "new", true)
			failed = false
			if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err != nil {
				t.Fatal(err)
			}
			hasVolume(t, recovered(t, client), "new", true)
		})
	}
}

func TestDelayedAdditionCannotRestoreUnstagedReference(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	apply := versionedConfigMapUpdates(t, m, client)
	var delayed *corev1.ConfigMap
	client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if delayed != nil {
			return false, nil, nil
		}
		delayed = action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
		return true, nil, context.DeadlineExceeded
	})
	if _, err := m.StageVolume(context.Background(), lifecycleOptions(cleanupKey), "new"); err == nil {
		t.Fatal("staging unexpectedly succeeded")
	}
	if delayed == nil {
		t.Fatal("addition did not reach persistence")
	}
	if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "new"); err != nil {
		t.Fatal(err)
	}
	if err := apply(delayed); !apierrors.IsConflict(err) {
		t.Fatalf("late addition must conflict, got %v", err)
	}
	restored := recovered(t, client)
	hasVolume(t, restored, "new", false)
	hasVolume(t, restored, "existing", true)
	if _, err := client.CoreV1().Pods(m.namespace).Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("shared consumer lost Pod:", err)
	}
}
