/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestUnmountResolvesAliasesWithoutProbingDisconnectedEndpoint(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	real := filepath.Join(base, "real")
	if err := os.MkdirAll(filepath.Join(real, "child"), 0755); err != nil {
		t.Fatal(err)
	}
	endpoint := filepath.Join(real, "fuse")
	parentAlias := filepath.Join(base, "parent-alias")
	finalAlias := filepath.Join(base, "final-alias")
	childAlias := filepath.Join(base, "child-alias")
	for link, target := range map[string]string{parentAlias: "real", finalAlias: endpoint, childAlias: filepath.Join(real, "child")} {
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range map[string]string{
		"parent":               filepath.Join(parentAlias, "fuse"),
		"final":                finalAlias,
		"dotdot after symlink": childAlias + "/../fuse",
	} {
		t.Run(name, func(t *testing.T) {
			present := true
			calls := 0
			cleanup := &mountCleanup{
				readInfo: func() ([]byte, error) {
					if present {
						return targetMountInfo(endpoint), nil
					}
					return []byte(rootMountInfo), nil
				},
				lstat: func(path string) (os.FileInfo, error) {
					if path == endpoint {
						t.Fatal("probed disconnected endpoint")
					}
					return os.Lstat(path)
				},
				run: func(_ context.Context, args ...string) ([]byte, error) {
					calls++
					if len(args) != 2 || args[0] != "--" || args[1] != endpoint {
						t.Fatalf("unmount used alias: %v", args)
					}
					present = false
					return nil, nil
				},
			}
			if err := cleanup.unmount(context.Background(), target); err != nil {
				t.Fatal(err)
			}
			// After detachment an absent endpoint may be probed on an idempotent retry.
			cleanup.lstat = os.Lstat
			if err := cleanup.unmount(context.Background(), target); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("unmount calls=%d", calls)
			}
		})
	}
}

func TestUnmountRejectsInconclusiveAliasResolution(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink("alias", alias); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"loop", "lstat failure", "readlink failure"} {
		t.Run(mode, func(t *testing.T) {
			cleanup := &mountCleanup{
				readInfo: func() ([]byte, error) { return []byte(rootMountInfo), nil },
				run: func(context.Context, ...string) ([]byte, error) {
					t.Fatal("must not unmount unresolved path")
					return nil, nil
				},
			}
			if mode == "lstat failure" {
				cleanup.lstat = func(string) (os.FileInfo, error) { return nil, os.ErrPermission }
			}
			if mode == "readlink failure" {
				cleanup.readlink = func(string) (string, error) { return "", errors.New("readlink failed") }
			}
			if err := cleanup.unmount(context.Background(), alias+"/fuse"); err == nil {
				t.Fatal("inconclusive detection reported success")
			}
		})
	}
}
