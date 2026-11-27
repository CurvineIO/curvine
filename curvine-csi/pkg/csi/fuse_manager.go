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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"k8s.io/klog"
)

// FuseManager manages FUSE process lifecycle
type FuseManager struct {
	mountState  *MountState
	processes   map[string]*FuseProcess // key: mnt-path@cluster-id
	mutex       sync.RWMutex
	healthCheck *HealthChecker

	// Recovery decisions are seams so the branch selection can be tested
	// without a live FUSE mount.
	//
	// mountedInTable reports whether the kernel mount table lists the path. It
	// must not stat the path: stat on a FUSE mount whose daemon died fails with
	// ENOTCONN even though the mount is still attached, and that is exactly the
	// state the recovery has to unmount.
	mountedInTable func(path string) (bool, error)
	// lazyUnmount detaches the path, tolerating "not mounted".
	lazyUnmount func(path string) error
}

// FuseProcess represents a running FUSE process
type FuseProcess struct {
	MntPath     string
	ClusterID   string
	MasterAddrs string
	FSPath      string
	PID         int
	Cmd         *exec.Cmd
	StartedAt   time.Time
}

// NewFuseManager creates a new FUSE manager
func NewFuseManager(mountState *MountState) *FuseManager {
	fm := &FuseManager{
		mountState:     mountState,
		processes:      make(map[string]*FuseProcess),
		mountedInTable: mountTableHas,
		lazyUnmount:    umountLazy,
	}

	// Start health checker
	fm.healthCheck = NewHealthChecker(fm)
	go fm.healthCheck.Start()

	return fm
}

// getProcessKey generates a key for process map
func getProcessKey(mntPath, clusterID string) string {
	return fmt.Sprintf("%s@%s", mntPath, clusterID)
}

// StartFuseProcess starts a FUSE process for the given mount point
func (fm *FuseManager) StartFuseProcess(ctx context.Context, mntPath, clusterID, masterAddrs, fsPath string, fuseParams map[string]string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	key := getProcessKey(mntPath, clusterID)

	// Bring the mount point into a state a fresh FUSE process can mount on.
	// An unowned mount left by a previous plugin is detached and restaged, and
	// leftovers in an unmounted directory are cleared.
	reuse, err := fm.prepareMountPoint(mntPath, key)
	if err != nil {
		return err
	}
	if reuse {
		return nil
	}

	// Build FUSE command
	fuseBinaryPath := "/opt/curvine/curvine-fuse"
	fuseArgv := BuildFuseExecArgs(FuseExecArgsInput{
		MasterAddrs: masterAddrs,
		FSPath:      fsPath,
		MntPath:     mntPath,
		Passthrough: fuseParams,
	})
	args := []string{fuseBinaryPath}
	if debugEnabled := os.Getenv("FUSE_DEBUG_ENABLED"); debugEnabled == "true" || debugEnabled == "1" {
		args = append(args, "-d")
		klog.Infof("FUSE debug mode enabled for %s", key)
	}
	args = append(args, fuseArgv...)

	// Use background context for FUSE process, as it should run independently
	// of the request context. The request context may be cancelled after the
	// request completes, which would terminate the FUSE process.
	cmd := exec.CommandContext(context.Background(), args[0], args[1:]...)

	// Set up log file
	logFile := filepath.Join("/tmp", fmt.Sprintf("curvine-fuse-%s.log", strings.ReplaceAll(key, "/", "_")))
	logFileHandle, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}
	defer logFileHandle.Close()

	cmd.Stdout = logFileHandle
	cmd.Stderr = logFileHandle

	// Start process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start FUSE process: %v", err)
	}

	// Wait a bit to check if process started successfully
	time.Sleep(500 * time.Millisecond)
	if cmd.Process == nil {
		return fmt.Errorf("FUSE process failed to start")
	}

	// Verify mount point is accessible (with timeout to avoid blocking)
	// Note: FUSE mount points may take a moment to become fully accessible
	klog.Infof("Verifying mount point %s for FUSE process (PID: %d)", mntPath, cmd.Process.Pid)
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()

	verifyDone := make(chan error, 1)
	go func() {
		verifyDone <- fm.verifyMountPoint(mntPath)
	}()

	select {
	case err := <-verifyDone:
		if err != nil {
			// Kill the process if mount failed
			klog.Errorf("Mount point verification failed for %s: %v", mntPath, err)
			cmd.Process.Kill()
			cmd.Process.Wait()
			// Do not leave a half-initialized directory behind: the next retry
			// would otherwise fail with "Mnt ... is not empty".
			if cleanupErr := fm.cleanupMountPoint(mntPath); cleanupErr != nil {
				klog.Warningf("Failed to clean up mount point %s after verification failure: %v", mntPath, cleanupErr)
			}
			return fmt.Errorf("failed to verify mount point: %v", err)
		}
		klog.Infof("Mount point %s verified successfully", mntPath)
	case <-verifyCtx.Done():
		// Timeout - assume mount is OK if process is running
		klog.Warningf("Mount point verification timeout for %s, but FUSE process is running (PID: %d)", mntPath, cmd.Process.Pid)
	}

	proc := &FuseProcess{
		MntPath:     mntPath,
		ClusterID:   clusterID,
		MasterAddrs: masterAddrs,
		FSPath:      fsPath,
		PID:         cmd.Process.Pid,
		Cmd:         cmd,
		StartedAt:   time.Now(),
	}

	fm.processes[key] = proc

	// Update mount state (do this in a goroutine to avoid blocking)
	klog.Infof("Updating mount state for %s (PID: %d)", key, proc.PID)
	mountInfo := &MountInfo{
		MntPath:     mntPath,
		FSPath:      fsPath, // curvine-path that FUSE mounts
		MasterAddrs: masterAddrs,
		ClusterID:   clusterID,
		FusePID:     proc.PID,
		RefCount:    0,
		Volumes:     []string{},
	}
	// Update mount state asynchronously to avoid blocking NodeStageVolume.
	// A restage must keep the volumes and reference count the entry already
	// carries: replacing it with an empty one would let a later unstage stop
	// this daemon while another volume still uses it.
	go func() {
		if err := fm.mountState.UpsertMountRuntime(mountInfo); err != nil {
			klog.Warningf("Failed to update mount state: %v", err)
		} else {
			klog.Infof("Mount state updated successfully for %s", key)
		}
	}()

	klog.Infof("Started FUSE process for %s (PID: %d)", key, proc.PID)

	// Monitor process in background
	go fm.monitorProcess(key, proc)

	return nil
}

// StopFuseProcess stops a FUSE process
func (fm *FuseManager) StopFuseProcess(mntPath, clusterID string) error {
	fm.mutex.Lock()
	defer fm.mutex.Unlock()

	key := getProcessKey(mntPath, clusterID)
	proc, exists := fm.processes[key]
	if !exists {
		klog.Warningf("FUSE process not found for %s", key)
		return nil
	}

	// Unmount first
	if err := fm.unmount(mntPath); err != nil {
		klog.Warningf("Failed to unmount %s: %v", mntPath, err)
	}

	// Kill process
	if proc.Cmd != nil && proc.Cmd.Process != nil {
		if err := proc.Cmd.Process.Kill(); err != nil {
			klog.Warningf("Failed to kill FUSE process %d: %v", proc.PID, err)
		}
		proc.Cmd.Process.Wait()
	}

	delete(fm.processes, key)

	// Remove from mount state
	if err := fm.mountState.RemoveMount(mntPath, clusterID); err != nil {
		klog.Warningf("Failed to remove mount state: %v", err)
	}

	klog.Infof("Stopped FUSE process for %s", key)
	return nil
}

// GetFuseProcess retrieves FUSE process information
func (fm *FuseManager) GetFuseProcess(mntPath, clusterID string) (*FuseProcess, bool) {
	fm.mutex.RLock()
	defer fm.mutex.RUnlock()

	key := getProcessKey(mntPath, clusterID)
	proc, exists := fm.processes[key]
	if exists {
		// Return a copy
		procCopy := *proc
		return &procCopy, true
	}
	return nil, false
}

// prepareMountPoint decides what to do with an existing mount point. It runs
// with fm.mutex held, so it only does bounded work: one mount-table read, an
// optional lazy unmount and directory removal.
//
// A mount that is not in the process map cannot be adopted: embedded mode runs
// the daemon as this container's child, so the mount belongs to a previous
// container (or a surge peer). Reusing it would leave the health checker and
// StopFuseProcess blind to it. Only a mount owned by a running process is
// reused; everything else is detached and restaged.
func (fm *FuseManager) prepareMountPoint(mntPath, key string) (bool, error) {
	if proc, exists := fm.processes[key]; exists {
		if isProcessRunning(proc.PID) {
			klog.Infof("Mount point %s is in use by our FUSE process (PID: %d), will reuse", mntPath, proc.PID)
			return true, nil
		}
		delete(fm.processes, key)
	}

	mounted, err := fm.mountedInTable(mntPath)
	if err != nil {
		return false, fmt.Errorf("cannot inspect mount table for %s: %v", mntPath, err)
	}
	if mounted {
		klog.Warningf("Mount point %s is mounted but not owned by this plugin (stale mount from a previous csi-plugin), detaching and restaging", mntPath)
	} else if dirHasEntries(mntPath) {
		klog.Warningf("Mount point directory %s holds leftovers from a previous FUSE session, clearing it", mntPath)
	} else {
		klog.Infof("Mount point directory %s is ready", mntPath)
		return false, nil
	}

	if err := fm.cleanupMountPoint(mntPath); err != nil {
		return false, err
	}
	return false, nil
}

// mountTableHas reports whether path is listed in the kernel mount table.
// It deliberately does not stat the path: os.Stat on a FUSE mount whose daemon
// died returns ENOTCONN even though the mount is still attached, so a stat-based
// "is it mounted" check skips the unmount for exactly the mounts this recovery
// exists for.
func mountTableHas(path string) (bool, error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false, err
	}
	cleanPath := filepath.Clean(path)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[1] == cleanPath {
			return true, nil
		}
	}
	return false, nil
}

// umountLazy detaches path with a lazy unmount, treating "not mounted" and a
// missing path as success. The syscall is used instead of umount(8): it cannot
// block on the FUSE daemon, needs no request context and reports the cases as
// errnos instead of message text (EINVAL: not a mount point, ENOENT: no path).
func umountLazy(path string) error {
	err := syscall.Unmount(path, syscall.MNT_DETACH)
	switch {
	case err == nil,
		errors.Is(err, syscall.EINVAL),
		errors.Is(err, syscall.ENOENT):
		return nil
	default:
		return fmt.Errorf("umount %s: %w", path, err)
	}
}

// dirHasEntries reports whether path exists and contains entries.
func dirHasEntries(path string) bool {
	entries, err := os.ReadDir(path)
	return err == nil && len(entries) > 0
}

// clearDirectoryTree removes the empty directory chains a previous FUSE session
// left under path, refusing to delete anything else: once the path is detached
// from the mount table, a remaining regular file means something nobody here
// understands is in there, and deleting it could lose data.
func clearDirectoryTree(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s exists and is not a directory", path)
	}
	return removeEmptyDirs(path)
}

func removeEmptyDirs(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		child := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			if err := removeEmptyDirs(child); err != nil {
				return err
			}
			continue
		}
		return fmt.Errorf("refusing to delete %s: mount point is not empty after unmount", child)
	}
	return os.Remove(path)
}

// cleanupMountPoint detaches a stale mount (if any) and recreates an empty
// mount point directory so a fresh FUSE process can mount on it.
func (fm *FuseManager) cleanupMountPoint(mntPath string) error {
	mounted, err := fm.mountedInTable(mntPath)
	if err != nil {
		return fmt.Errorf("cannot inspect mount table for %s: %v", mntPath, err)
	}
	if mounted {
		if err := fm.lazyUnmount(mntPath); err != nil {
			return fmt.Errorf("failed to unmount stale mount point %s: %v", mntPath, err)
		}
		// The detach must be observable in the mount table before the
		// directory is touched: deleting through a still-attached FUSE mount
		// would recurse into the Curvine namespace.
		stillMounted, err := fm.mountedInTable(mntPath)
		if err != nil {
			return fmt.Errorf("cannot verify unmount of %s: %v", mntPath, err)
		}
		if stillMounted {
			return fmt.Errorf("mount point %s is still mounted after unmount", mntPath)
		}
		klog.Infof("Successfully unmounted stale mount point: %s", mntPath)
	}

	if err := clearDirectoryTree(mntPath); err != nil {
		return fmt.Errorf("failed to clear mount point directory %s: %v", mntPath, err)
	}
	if err := os.MkdirAll(mntPath, 0750); err != nil {
		return fmt.Errorf("failed to recreate mount point directory %s: %v", mntPath, err)
	}
	return nil
}

// verifyMountPoint verifies that mount point is accessible
func (fm *FuseManager) verifyMountPoint(mntPath string) error {
	// Check if directory exists
	if _, err := os.Stat(mntPath); err != nil {
		return fmt.Errorf("mount point does not exist: %v", err)
	}

	// Check if it's a mount point
	cmd := exec.Command("mountpoint", "-q", mntPath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("not a mount point: %v", err)
	}

	return nil
}

// unmount unmounts a mount point
func (fm *FuseManager) unmount(mntPath string) error {
	cmd := exec.Command("umount", mntPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Check if already unmounted
		if strings.Contains(string(output), "not mounted") {
			return nil
		}
		return fmt.Errorf("failed to unmount %s: %v, output: %s", mntPath, err, string(output))
	}
	return nil
}

// monitorProcess monitors a FUSE process and handles abnormal exits
func (fm *FuseManager) monitorProcess(key string, proc *FuseProcess) {
	if proc.Cmd == nil || proc.Cmd.Process == nil {
		return
	}

	state, err := proc.Cmd.Process.Wait()
	if err != nil {
		klog.Errorf("Error waiting for FUSE process %s (PID: %d): %v", key, proc.PID, err)
		return
	}

	if !state.Success() {
		klog.Errorf("FUSE process %s (PID: %d) exited unexpectedly with code %d", key, proc.PID, state.ExitCode())

		// Remove from processes map
		fm.mutex.Lock()
		delete(fm.processes, key)
		fm.mutex.Unlock()

		// Update mount state in place: writing back a copy would replace the
		// entry and could drop a volume a concurrent publish just registered.
		if err := fm.mountState.UpdateFusePID(proc.MntPath, proc.ClusterID, 0); err != nil {
			klog.Warningf("Failed to clear FUSE PID for %s: %v", proc.MntPath, err)
		}
	}
}

// isProcessRunning checks if a process is still running
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Send signal 0 to check if process exists
	// Signal 0 doesn't actually send a signal, it just checks if the process exists
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		// Process doesn't exist or we don't have permission
		return false
	}
	return true
}

// HealthChecker periodically checks FUSE process health
type HealthChecker struct {
	fm            *FuseManager
	checkInterval time.Duration
	stopCh        chan struct{}
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(fm *FuseManager) *HealthChecker {
	return &HealthChecker{
		fm:            fm,
		checkInterval: 30 * time.Second,
		stopCh:        make(chan struct{}),
	}
}

// Start starts the health checker
func (hc *HealthChecker) Start() {
	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkHealth()
		case <-hc.stopCh:
			return
		}
	}
}

// Stop stops the health checker
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
}

// checkHealth checks health of all FUSE processes
func (hc *HealthChecker) checkHealth() {
	hc.fm.mutex.RLock()
	processes := make(map[string]*FuseProcess)
	for k, v := range hc.fm.processes {
		procCopy := *v
		processes[k] = &procCopy
	}
	hc.fm.mutex.RUnlock()

	for key, proc := range processes {
		if !isProcessRunning(proc.PID) {
			klog.Warningf("FUSE process %s (PID: %d) is not running, cleaning up", key, proc.PID)

			// Remove from processes
			hc.fm.mutex.Lock()
			delete(hc.fm.processes, key)
			hc.fm.mutex.Unlock()

			// Update mount state in place (see monitorProcess).
			if err := hc.fm.mountState.UpdateFusePID(proc.MntPath, proc.ClusterID, 0); err != nil {
				klog.Warningf("Failed to clear FUSE PID for %s: %v", proc.MntPath, err)
			}
		}
	}
}
