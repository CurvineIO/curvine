/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/
package csi

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const cleanupKey = "test-mount-key"

func cleanupManager(t *testing.T, volumes ...string) (*standaloneMountManagerImpl, *fake.Clientset) {
	t.Helper()
	client := fake.NewClientset()
	m := NewStandaloneManager(client, "test", "node", "test-image", "sa").(*standaloneMountManagerImpl)
	m.mountReady = func(context.Context, string) error { return nil }
	m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) { return []byte(rootMountInfo), nil }}
	m.state.Mounts[cleanupKey] = &StandaloneStateEntry{PodName: m.getStandaloneName(cleanupKey), Volumes: volumes, RefCount: len(volumes)}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: m.getStandaloneName(cleanupKey), Namespace: "test", UID: "original"}, Spec: corev1.PodSpec{NodeName: "node", Volumes: []corev1.Volume{{Name: "fuse-mount", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: m.GetHostMountPath(cleanupKey)}}}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if _, err := client.CoreV1().Pods("test").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.saveState(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m, client
}
func unstage(n *nodeServiceStandalone, volume string) error {
	_, err := n.NodeUnstageVolume(context.Background(), &csipb.NodeUnstageVolumeRequest{VolumeId: volume, StagingTargetPath: "/stage"})
	return err
}
func recovered(t *testing.T, client *fake.Clientset) *standaloneMountManagerImpl {
	t.Helper()
	m := NewStandaloneManager(client, "test", "node", "test-image", "sa").(*standaloneMountManagerImpl)
	m.mountReady = func(context.Context, string) error { return nil }
	m.mountCleanup = &mountCleanup{readInfo: func() ([]byte, error) { return []byte(rootMountInfo), nil }}
	pods, _ := client.CoreV1().Pods("test").List(context.Background(), metav1.ListOptions{})
	for _, pod := range pods.Items {
		for _, v := range pod.Spec.Volumes {
			if v.Name == "fuse-mount" && v.HostPath != nil {
				m.hostMountBaseDir = filepath.Dir(filepath.Dir(v.HostPath.Path))
				break
			}
		}
	}
	if err := m.RecoverState(context.Background()); err != nil {
		t.Fatal(err)
	}
	return m
}
func hasVolume(t *testing.T, m *standaloneMountManagerImpl, v string, want bool) {
	t.Helper()
	_, found := m.FindMountKeyByVolumeID(v)
	if found != want {
		t.Fatalf("volume %s found=%v want=%v", v, found, want)
	}
}

func TestUnstageSharedVolumeAndPersistenceRetry(t *testing.T) {
	m, client := cleanupManager(t, "a", "b")
	fail := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("state unavailable")
		}
		return false, nil, nil
	})
	n := &nodeServiceStandalone{standaloneManager: m}
	if err := unstage(n, "a"); status.Code(err) != codes.Internal {
		t.Fatalf("expected failure: %v", err)
	}
	hasVolume(t, m, "a", true)
	hasVolume(t, recovered(t, client), "a", true)
	fail = false
	if err := unstage(n, "a"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "a", false)
	hasVolume(t, m, "b", true)
	if _, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{}); err != nil {
		t.Fatal("shared FUSE deleted:", err)
	}
	if err := m.DeleteStandalone(context.Background(), cleanupKey); err == nil {
		t.Fatal("must reject deletion with active refs")
	}
	if err := unstage(n, "a"); err != nil {
		t.Fatal(err)
	}
	if err := unstage(n, "b"); err != nil {
		t.Fatal(err)
	}
	if err := unstage(n, "b"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "b", false)
}

func TestUnstageLastReferenceSurvivesDeleteFailureAndRestart(t *testing.T) {
	m, client := cleanupManager(t, "last")
	fail := true
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("delete unavailable")
		}
		return false, nil, nil
	})
	if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "last"); status.Code(err) != codes.Internal {
		t.Fatalf("got %v", err)
	}
	hasVolume(t, m, "last", true)
	m = recovered(t, client)
	hasVolume(t, m, "last", true)
	fail = false
	if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "last"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "last", false)
}

func TestUnstageWaitsForDeletionAndRetainsStateOnSaveFailure(t *testing.T) {
	m, client := cleanupManager(t, "last")
	pending := true
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		if pending {
			return true, nil, nil
		}
		return false, nil, nil
	})
	n := &nodeServiceStandalone{standaloneManager: m}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := n.NodeUnstageVolume(ctx, &csipb.NodeUnstageVolumeRequest{VolumeId: "last", StagingTargetPath: "/stage"}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("accepted deletion must not mean complete")
	}
	hasVolume(t, m, "last", true)
	pending = false
	failSave := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if failSave {
			return true, nil, errors.New("persist unavailable")
		}
		return false, nil, nil
	})
	if err := unstage(n, "last"); err == nil {
		t.Fatal("save failure must propagate")
	}
	_, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected pod absent: %v", err)
	}
	hasVolume(t, m, "last", true)
	m = recovered(t, client)
	hasVolume(t, m, "last", true)
	failSave = false
	if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "last"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "last", false)
}

func TestAddVolumePersistenceFailureRetainsCleanupReference(t *testing.T) {
	m, client := cleanupManager(t, "existing")
	fail := true
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
		if fail {
			return true, nil, errors.New("persist unavailable")
		}
		return false, nil, nil
	})
	if err := m.AddVolumeRef(context.Background(), cleanupKey, "new"); err == nil {
		t.Fatal("expected failure")
	}
	hasVolume(t, m, "new", true)
	hasVolume(t, recovered(t, client), "new", false)
	hasVolume(t, m, "existing", true)
	fail = false
	if err := m.AddVolumeRef(context.Background(), cleanupKey, "new"); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, recovered(t, client), "new", true)
}

func TestUnpublishDoesNotReleaseSharedConsumer(t *testing.T) {
	m, _ := cleanupManager(t, "shared")
	n := &nodeServiceStandalone{standaloneManager: m, mountCleanup: &mountCleanup{readInfo: func() ([]byte, error) { return []byte(rootMountInfo), nil }, run: func(context.Context, ...string) ([]byte, error) { t.Fatal("unexpected unmount"); return nil, nil }}}
	if _, err := n.NodeUnpublishVolume(context.Background(), &csipb.NodeUnpublishVolumeRequest{VolumeId: "shared", TargetPath: "/already-unmounted"}); err != nil {
		t.Fatal(err)
	}
	hasVolume(t, m, "shared", true)
}

func TestUnstageRetainsReferenceUntilHostMountDisappears(t *testing.T) {
	m, client := cleanupManager(t, "last")
	present := true
	allowUnmount := false
	calls := 0
	m.mountReady = func(context.Context, string) error { return nil }
	m.mountCleanup = &mountCleanup{
		readInfo: func() ([]byte, error) {
			if present {
				return targetMountInfo(m.GetHostMountPath(cleanupKey)), nil
			}
			return []byte(rootMountInfo), nil
		},
		run: func(context.Context, ...string) ([]byte, error) {
			calls++
			if allowUnmount {
				present = false
			}
			return nil, nil
		},
	}
	n := &nodeServiceStandalone{standaloneManager: m}
	if err := unstage(n, "last"); err == nil {
		t.Fatal("residual host mount must fail cleanup")
	}
	hasVolume(t, m, "last", true)
	hasVolume(t, recovered(t, client), "last", true)
	allowUnmount = true
	if err := unstage(n, "last"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("unmount calls %d", calls)
	}
	hasVolume(t, recovered(t, client), "last", false)
}

func TestGarbageCollectionRetriesAfterPodAlreadyDisappeared(t *testing.T) {
	m, client := cleanupManager(t, "orphan")
	if err := client.CoreV1().Pods("test").Delete(context.Background(), m.getStandaloneName(cleanupKey), metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	n.cleanupOrphanedStandalonePods()
	hasVolume(t, recovered(t, client), "orphan", false)
}

func TestConcurrentSharedUnstagesAndRetry(t *testing.T) {
	m, client := cleanupManager(t, "a", "b")
	n := &nodeServiceStandalone{standaloneManager: m}
	results := make(chan error, 2)
	for _, v := range []string{"a", "b"} {
		go func(v string) { results <- unstage(n, v) }(v)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if err := unstage(n, "a"); err != nil {
		t.Fatal(err)
	}
	if err := unstage(n, "b"); err != nil {
		t.Fatal(err)
	}
	if len(recovered(t, client).GetState().Mounts) != 0 {
		t.Fatal("state leaked")
	}
}

func TestUnstageDoesNotDeleteReplacementPod(t *testing.T) {
	m, client := cleanupManager(t, "last")
	m.mountCleanup.readInfo = func() ([]byte, error) {
		return targetMountInfo(m.GetHostMountPath(cleanupKey)), nil
	}
	m.mountCleanup.run = func(context.Context, ...string) ([]byte, error) {
		t.Fatal("must not unmount a replacement Pod's mount")
		return nil, nil
	}
	client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(ktesting.DeleteAction).GetDeleteOptions()
		if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "original" {
			t.Fatal("cleanup must fence deletion by the observed Pod UID")
		}
		name := action.(ktesting.DeleteAction).GetName()
		resource := corev1.SchemeGroupVersion.WithResource("pods")
		obj, err := client.Tracker().Get(resource, "test", name)
		if err != nil {
			t.Fatal(err)
		}
		replacement := obj.(*corev1.Pod).DeepCopy()
		replacement.UID = "replacement"
		if err := client.Tracker().Update(resource, replacement, "test"); err != nil {
			t.Fatal(err)
		}
		// The API server rejects deletion when the name now identifies a new Pod.
		return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, name, errors.New("UID precondition failed"))
	})
	if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "last"); status.Code(err) != codes.Internal {
		t.Fatalf("expected retryable cleanup failure, got %v", err)
	}
	hasVolume(t, m, "last", true)
	hasVolume(t, recovered(t, client), "last", true)
	pod, err := client.CoreV1().Pods("test").Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
	if err != nil || pod.UID != "replacement" {
		t.Fatalf("replacement Pod must survive cleanup: pod=%v err=%v", pod, err)
	}
}

func TestGarbageCollectionListFailurePreservesOwnership(t *testing.T) {
	m, client := cleanupManager(t, "live")
	client.ClearActions()
	client.PrependReactor("list", "persistentvolumes", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API server unavailable")
	})
	n := &nodeServiceStandalone{standaloneManager: m, k8sClient: &K8sClient{clientset: client, namespace: "test"}}
	n.cleanupOrphanedStandalonePods()
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" || action.GetVerb() == "update" {
			t.Fatalf("failed discovery must not mutate ownership/resources: %v", action)
		}
	}
	hasVolume(t, recovered(t, client), "live", true)
}

func setTestHostMountBase(t *testing.T, m *standaloneMountManagerImpl, client *fake.Clientset, base string) {
	t.Helper()
	oldBase := filepath.Dir(filepath.Dir(m.GetHostMountPath(cleanupKey)))
	m.hostMountBaseDir = base
	m.mountReady = func(context.Context, string) error { return nil } // fake Kubernetes readiness models propagation
	pods, err := client.CoreV1().Pods("test").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods.Items {
		for i, v := range pod.Spec.Volumes {
			if v.Name == "fuse-mount" && v.HostPath != nil {
				pod.Spec.Volumes[i].HostPath.Path = base + strings.TrimPrefix(v.HostPath.Path, oldBase)
			}
		}
		if _, err := client.CoreV1().Pods("test").Update(context.Background(), &pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
}
