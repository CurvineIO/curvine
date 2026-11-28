/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"fmt"
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
	ktesting "k8s.io/client-go/testing"
)

func TestUnstageWaitsForGracefulPodDeletion(t *testing.T) {
	for _, terminating := range []bool{false, true} {
		t.Run(fmt.Sprint(terminating), func(t *testing.T) {
			m, client := cleanupManager(t, "last")
			resource := corev1.SchemeGroupVersion.WithResource("pods")
			name := m.getStandaloneName(cleanupKey)
			pending, polls, deletes, cleanups := false, 0, 0, 0
			if terminating {
				obj, err := client.Tracker().Get(resource, m.namespace, name)
				if err != nil {
					t.Fatal(err)
				}
				pod := obj.(*corev1.Pod).DeepCopy()
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				if err := client.Tracker().Update(resource, pod, m.namespace); err != nil {
					t.Fatal(err)
				}
			}
			client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				deletes++
				opts := action.(ktesting.DeleteAction).GetDeleteOptions()
				if opts.GracePeriodSeconds != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "original" {
					t.Fatalf("graceful UID-fenced deletion required: %+v", opts)
				}
				pending = true
				return true, nil, nil
			})
			client.PrependReactor("get", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
				if !pending {
					pending = terminating
					return false, nil, nil
				}
				polls++
				if polls == 1 {
					return false, nil, nil
				}
				if err := client.Tracker().Delete(resource, m.namespace, name); err != nil {
					t.Fatal(err)
				}
				return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, name)
			})
			m.mountCleanup.readInfo = func() ([]byte, error) {
				cleanups++
				if polls < 2 {
					t.Fatal("host cleanup ran before Pod disappearance")
				}
				return []byte(rootMountInfo), nil
			}
			if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "last"); err != nil {
				t.Fatal(err)
			}
			wantDeletes := 1
			if terminating {
				wantDeletes = 0
			}
			if deletes != wantDeletes || cleanups != 1 {
				t.Fatalf("deletes=%d cleanups=%d", deletes, cleanups)
			}
			hasVolume(t, recovered(t, client), "last", false)
		})
	}
}

func TestUnstageDeletionWaitFailuresRetainState(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "api", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			m, client := cleanupManager(t, "last")
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			pending, failed := false, true
			client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
				if !failed {
					return false, nil, nil
				}
				pending = true
				return true, nil, nil
			})
			client.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
				if !pending || !failed {
					return false, nil, nil
				}
				switch mode {
				case "cancel":
					cancel()
				case "api":
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "pod", fmt.Errorf("API denied"))
				case "replacement":
					name := action.(ktesting.GetAction).GetName()
					obj, err := client.Tracker().Get(action.GetResource(), m.namespace, name)
					if err != nil {
						t.Fatal(err)
					}
					pod := obj.(*corev1.Pod).DeepCopy()
					pod.UID = "replacement"
					if err := client.Tracker().Update(action.GetResource(), pod, m.namespace); err != nil {
						t.Fatal(err)
					}
					return true, pod, nil
				}
				return false, nil, nil
			})
			cleanups := 0
			m.mountCleanup.readInfo = func() ([]byte, error) { cleanups++; return []byte(rootMountInfo), nil }
			n := &nodeServiceStandalone{standaloneManager: m}
			_, err := n.NodeUnstageVolume(ctx, &csipb.NodeUnstageVolumeRequest{VolumeId: "last", StagingTargetPath: "/stage"})
			want := codes.Internal
			if mode == "cancel" {
				want = codes.Canceled
			}
			if mode == "deadline" {
				want = codes.DeadlineExceeded
			}
			if status.Code(err) != want {
				t.Fatalf("got %v, want %v", err, want)
			}
			if cleanups != 0 {
				t.Fatal("host mount inspected before confirmed Pod absence")
			}
			hasVolume(t, m, "last", true)
			failed = false
			hasVolume(t, recovered(t, client), "last", true)
			if mode == "replacement" {
				pod, err := client.CoreV1().Pods(m.namespace).Get(context.Background(), m.getStandaloneName(cleanupKey), metav1.GetOptions{})
				if err != nil || pod.UID != "replacement" {
					t.Fatalf("replacement not preserved: %v %v", pod, err)
				}
				return
			}
			if err := unstage(n, "last"); err != nil {
				t.Fatal(err)
			}
			hasVolume(t, recovered(t, client), "last", false)
		})
	}
}

func TestUnstageKeepsTimeForHostCleanup(t *testing.T) {
	m, _ := cleanupManager(t, "last")
	present := true
	m.mountCleanup.readInfo = func() ([]byte, error) {
		if present {
			return targetMountInfo(m.GetHostMountPath(cleanupKey)), nil
		}
		return []byte(rootMountInfo), nil
	}
	m.mountCleanup.run = func(ctx context.Context, _ ...string) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= time.Duration(StandaloneTerminationGracePeriod)*time.Second {
			t.Fatal("outer unstage budget leaves no room beyond graceful termination")
		}
		present = false
		return nil, nil
	}
	if err := unstage(&nodeServiceStandalone{standaloneManager: m}, "last"); err != nil {
		t.Fatal(err)
	}
	if present {
		t.Fatal("host cleanup not performed")
	}
}
