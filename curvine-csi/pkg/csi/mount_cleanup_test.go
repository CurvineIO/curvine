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
	"strings"
	"testing"
	"time"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const rootMountInfo = "1 0 0:1 / / rw - rootfs rootfs rw\n"

func targetMountInfo(target string) []byte {
	return []byte(rootMountInfo + fmt.Sprintf("42 1 0:42 /subdir %s rw shared:7 - fuse.curvinefs curvine rw\n", target))
}

func TestMountInfoContains(t *testing.T) {
	for _, tc := range []struct {
		name, info, target string
		want, wantErr      bool
	}{
		{"disconnected bind", string(targetMountInfo("/missing/target")), "/missing/target", true, false},
		{"absent", rootMountInfo, "/missing/target", false, false},
		{"exact match", string(targetMountInfo("/target/child")), "/target", false, false},
		{"escaped path", string(targetMountInfo(`/with\040space\134slash\011tab\012newline`)), "/with space\\slash\ttab\nnewline", true, false},
		{"Unicode bind root", rootMountInfo + "42 1 0:42 /reports\u00a02026 /target rw - fuse.curvinefs curvine rw\n", "/target", true, false},
		{"Unicode mountpoint", string(targetMountInfo("/target\u00a02026")), "/target\u00a02026", true, false},
		{"Unicode is not a separator", string(targetMountInfo("/target\u00a02026")), "/target", false, false},
		{"Unicode and escaped whitespace", string(targetMountInfo("/训练\u2003集\\040data\u202f2026")), "/训练\u2003集 data\u202f2026", true, false},
		{"empty", "", "/target", false, true},
		{"malformed", "42 1 0:42 / /target rw", "/target", false, true},
		{"empty field", "42  1 0:42 / /target rw - fuse.curvinefs curvine rw\n", "/target", false, true},
		{"invalid escape", string(targetMountInfo(`/bad\xx1`)), "/target", false, true},
		{"oversized record", strings.Repeat("x", 1024*1024+1), "/target", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mountInfoContains([]byte(tc.info), tc.target)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

func TestNodeUnpublishDisconnectedMountAndIdempotency(t *testing.T) {
	// The path intentionally does not exist on the test machine. Mountinfo alone
	// is authoritative: an os.Stat/mountpoint short circuit would skip this unmount.
	target := "/not-present/disconnected\u00a0fuse"
	present := true
	calls := 0
	n := &nodeServiceStandalone{mountCleanup: &mountCleanup{
		readInfo: func() ([]byte, error) {
			if present {
				return targetMountInfo(target), nil
			}
			return []byte(rootMountInfo), nil
		},
		run: func(_ context.Context, args ...string) ([]byte, error) {
			calls++
			if !reflect.DeepEqual(args, []string{"--", target}) {
				t.Fatalf("unexpected command: %v", args)
			}
			present = false
			return nil, nil
		},
	}}
	// No manager is supplied: unpublish must not release shared volume state.
	req := &csipb.NodeUnpublishVolumeRequest{VolumeId: "shared-volume", TargetPath: target}
	for i := 0; i < 2; i++ {
		if _, err := n.NodeUnpublishVolume(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("unmount calls=%d", calls)
	}
}

func TestNodeUnpublishFailuresAreRetryable(t *testing.T) {
	for _, mode := range []string{"detect", "unmount", "verify", "residual"} {
		t.Run(mode, func(t *testing.T) {
			reads, calls := 0, 0
			failure := true
			present := true
			n := &nodeServiceStandalone{mountCleanup: &mountCleanup{
				readInfo: func() ([]byte, error) {
					reads++
					if failure && (mode == "detect" || mode == "verify" && reads > 1) {
						return nil, errors.New("mountinfo unavailable")
					}
					if present {
						return targetMountInfo("/target"), nil
					}
					return []byte(rootMountInfo), nil
				},
				run: func(_ context.Context, args ...string) ([]byte, error) {
					calls++
					if failure && mode == "unmount" {
						return []byte("busy"), errors.New("umount failed")
					}
					if !failure || mode != "residual" {
						present = false
					}
					return nil, nil
				},
			}}
			req := &csipb.NodeUnpublishVolumeRequest{VolumeId: "v", TargetPath: "/target"}
			if _, err := n.NodeUnpublishVolume(context.Background(), req); status.Code(err) != codes.Internal {
				t.Fatalf("expected Internal: %v", err)
			}
			if mode == "detect" && calls != 0 {
				t.Fatal("must not unmount after detection error")
			}
			failure = false
			if _, err := n.NodeUnpublishVolume(context.Background(), req); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnmountFallbackAndConcurrentDetachment(t *testing.T) {
	for _, concurrent := range []bool{false, true} {
		t.Run(fmt.Sprint(concurrent), func(t *testing.T) {
			present := true
			calls := 0
			m := &mountCleanup{
				readInfo: func() ([]byte, error) {
					if present {
						return targetMountInfo("/target"), nil
					}
					return []byte(rootMountInfo), nil
				},
				run: func(_ context.Context, args ...string) ([]byte, error) {
					calls++
					if calls == 1 {
						if concurrent {
							present = false
						}
						return nil, errors.New("normal unmount failed")
					}
					if !reflect.DeepEqual(args, []string{"-l", "--", "/target"}) {
						t.Fatalf("unexpected fallback %v", args)
					}
					present = false
					return nil, nil
				},
			}
			if err := m.unmount(context.Background(), "/target"); err != nil {
				t.Fatal(err)
			}
			want := 2
			if concurrent {
				want = 1
			}
			if calls != want {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}

func TestNodeUnpublishCancellation(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprint(expired), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := codes.Canceled
			if expired {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				want = codes.DeadlineExceeded
			}
			calls := 0
			n := &nodeServiceStandalone{mountCleanup: &mountCleanup{
				readInfo: func() ([]byte, error) { return targetMountInfo("/target"), nil },
				run:      func(ctx context.Context, _ ...string) ([]byte, error) { calls++; cancel(); return nil, ctx.Err() },
			}}
			_, err := n.NodeUnpublishVolume(ctx, &csipb.NodeUnpublishVolumeRequest{VolumeId: "v", TargetPath: "/target"})
			if status.Code(err) != want {
				t.Fatalf("got %v", err)
			}
			if calls > 1 {
				t.Fatal("must not try lazy fallback after cancellation")
			}
		})
	}
}

func TestNodeUnpublishStackedMountRequiresRetry(t *testing.T) {
	layers, calls := 2, 0
	n := &nodeServiceStandalone{mountCleanup: &mountCleanup{
		readInfo: func() ([]byte, error) {
			info := rootMountInfo
			for i := 0; i < layers; i++ {
				info += fmt.Sprintf("%d 1 0:42 /subdir /target rw - fuse.curvinefs curvine rw\n", 42+i)
			}
			return []byte(info), nil
		},
		run: func(context.Context, ...string) ([]byte, error) {
			calls++
			layers-- // An ordinary umount removes only the top mount.
			return nil, nil
		},
	}}
	req := &csipb.NodeUnpublishVolumeRequest{VolumeId: "v", TargetPath: "/target"}
	if _, err := n.NodeUnpublishVolume(context.Background(), req); status.Code(err) != codes.Internal {
		t.Fatalf("must not report success with a remaining mount: %v", err)
	}
	if layers != 1 || calls != 1 {
		t.Fatalf("layers=%d calls=%d", layers, calls)
	}
	for i := 0; i < 2; i++ {
		if _, err := n.NodeUnpublishVolume(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	if layers != 0 || calls != 2 {
		t.Fatalf("retry must detach the last layer exactly once: layers=%d calls=%d", layers, calls)
	}
}
