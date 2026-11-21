/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const volumeCleanupTimeout = 30 * time.Second

// Read the CSI process's mount namespace without stat/open on the FUSE target.
// Filesystem probes may fail with ENOTCONN even though a kernel mount remains.
type mountCleanup struct {
	readInfo func() ([]byte, error)
	run      func(context.Context, ...string) ([]byte, error)
	lstat    func(string) (os.FileInfo, error)
	readlink func(string) (string, error)
}

func newMountCleanup() *mountCleanup {
	return &mountCleanup{
		readInfo: func() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") },
		run: func(ctx context.Context, args ...string) ([]byte, error) {
			cmd := exec.CommandContext(ctx, "umount", args...)
			cmd.WaitDelay = time.Second
			return cmd.CombinedOutput()
		},
	}
}

func cleanupStatus(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	return status.Errorf(codes.Internal, "volume cleanup failed: %v", err)
}

func (m *mountCleanup) mounted(ctx context.Context, target string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	data, err := m.readInfo()
	if err != nil {
		return false, fmt.Errorf("read mountinfo: %w", err)
	}
	return mountInfoContains(data, target)
}

func (m *mountCleanup) unmount(ctx context.Context, target string) error {
	if !filepath.IsAbs(target) {
		return fmt.Errorf("target path must be absolute: %q", target)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := m.readInfo()
	if err != nil {
		return fmt.Errorf("read mountinfo: %w", err)
	}
	mounts, err := parseMountInfo(data)
	if err != nil {
		return err
	}
	target, err = m.resolveMountPath(ctx, target, mounts)
	if err != nil {
		return err
	}
	mounted := mounts[target]
	if !mounted {
		return nil
	}
	output, err := m.run(ctx, "--", target)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Another unpublish may have already detached the same target.
		stillMounted, checkErr := m.mounted(ctx, target)
		if checkErr != nil {
			return checkErr
		}
		if !stillMounted {
			return nil
		}
		// Preserve the existing lazy fallback, but verify target detachment below.
		output, err = m.run(ctx, "-l", "--", target)
		if err != nil {
			return fmt.Errorf("unmount %s: %w (%s)", target, err, output)
		}
	}
	mounted, err = m.mounted(ctx, target)
	if err != nil {
		return err
	}
	if mounted {
		return fmt.Errorf("target %s remains mounted after unmount", target)
	}
	return nil
}

func mountInfoContains(data []byte, target string) (bool, error) {
	mounts, err := parseMountInfo(data)
	return mounts[target], err
}

func parseMountInfo(data []byte) (map[string]bool, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	mounts := make(map[string]bool)
	for scanner.Scan() {
		// Linux escapes ASCII whitespace in paths, but leaves Unicode whitespace
		// untouched. strings.Fields would split valid roots and mountpoints.
		fields := strings.Split(scanner.Text(), " ")
		for _, field := range fields {
			if field == "" {
				return nil, fmt.Errorf("empty mountinfo field")
			}
		}
		// mount ID, parent ID, major:minor, root, mountpoint, options,
		// optional fields, "-", fstype, source, super options.
		separator := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				separator = i
				break
			}
		}
		if separator < 6 || len(fields) < separator+4 {
			return nil, fmt.Errorf("malformed mountinfo record")
		}
		path, err := unescapeMountPath(fields[4])
		if err != nil {
			return nil, err
		}
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("non-absolute mountinfo path")
		}
		mounts[filepath.Clean(path)] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan mountinfo: %w", err)
	}
	// A real Linux mount namespace always has at least its root mount. Treat
	// an empty/truncated input as detection failure, never absence.
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("empty mountinfo")
	}
	return mounts, nil
}

func unescapeMountPath(path string) (string, error) {
	var out strings.Builder
	for i := 0; i < len(path); i++ {
		if path[i] != '\\' {
			out.WriteByte(path[i])
			continue
		}
		if i+3 >= len(path) {
			return "", fmt.Errorf("invalid mountinfo path escape")
		}
		value, err := strconv.ParseUint(path[i+1:i+4], 8, 8)
		if err != nil {
			return "", fmt.Errorf("invalid mountinfo path escape: %w", err)
		}
		out.WriteByte(byte(value))
		i += 3
	}
	return out.String(), nil
}

// Resolve aliases component by component, checking mountinfo before lstat. A
// disconnected mount's root must never need a successful endpoint probe. Do not
// clean '..' before resolving symlinks: link/.. follows the link before ascending.
func (m *mountCleanup) resolveMountPath(ctx context.Context, target string, mounts map[string]bool) (string, error) {
	if target == filepath.Clean(target) && mounts[target] {
		return target, nil
	}
	lstat, readlink := m.lstat, m.readlink
	if lstat == nil {
		lstat = os.Lstat
	}
	if readlink == nil {
		readlink = os.Readlink
	}
	parts := strings.Split(target, string(os.PathSeparator))
	resolved := string(os.PathSeparator)
	links := 0
	for len(parts) > 0 {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		part := parts[0]
		parts = parts[1:]
		if part == "" || part == "." {
			continue
		}
		if part == ".." {
			resolved = filepath.Dir(resolved)
			continue
		}
		next := filepath.Join(resolved, part)
		// The kernel mount table already proves this component is a mount root.
		if mounts[next] {
			resolved = next
			continue
		}
		info, err := lstat(next)
		if os.IsNotExist(err) {
			// No mount entry at this resolved component and the backing path is
			// absent. The remaining suffix cannot name an existing mount.
			for _, rest := range parts {
				if rest == ".." {
					return "", fmt.Errorf("cannot resolve missing ancestor %s", next)
				}
			}
			return filepath.Join(append([]string{next}, parts...)...), nil
		}
		if err != nil {
			return "", fmt.Errorf("resolve mount path %s: %w", next, err)
		}
		if info.Mode()&os.ModeSymlink == 0 {
			if len(parts) > 0 && !info.IsDir() {
				return "", fmt.Errorf("mount path ancestor %s is not a directory", next)
			}
			resolved = next
			continue
		}
		links++
		if links > 40 {
			return "", fmt.Errorf("too many symlinks resolving %s", target)
		}
		link, err := readlink(next)
		if err != nil {
			return "", fmt.Errorf("read mount path symlink %s: %w", next, err)
		}
		if filepath.IsAbs(link) {
			resolved = string(os.PathSeparator)
		}
		parts = append(strings.Split(link, string(os.PathSeparator)), parts...)
	}
	return resolved, nil
}
