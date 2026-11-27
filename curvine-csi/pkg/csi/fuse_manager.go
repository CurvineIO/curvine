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
	"fmt"
	"io"
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
		mountState: mountState,
		processes:  make(map[string]*FuseProcess),
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

	// Check if process already exists
	if proc, exists := fm.processes[key]; exists {
		// Verify process is still running
		if isProcessRunning(proc.PID) {
			klog.Infof("FUSE process already running for %s (PID: %d)", key, proc.PID)
			return nil
		}
		// Process died, remove it
		delete(fm.processes, key)
	}

	// Ensure mount point directory exists and is ready
	_, statErr := os.Stat(mntPath)

	if statErr != nil && !os.IsNotExist(statErr) {
		// Stat failed with error other than "not exist" (e.g., "transport endpoint is not connected")
		// This usually means broken mount point - try to cleanup
		klog.Warningf("Mount point %s stat failed: %v, attempting cleanup", mntPath, statErr)

		// Safety check: verify no FUSE process is using this mount point
		// This prevents cleaning up a mount that's actually in use
		if proc, exists := fm.processes[key]; exists {
			if isProcessRunning(proc.PID) {
				return fmt.Errorf("mount point %s has errors but FUSE process is still running (PID: %d) - manual intervention required",
					mntPath, proc.PID)
			}
			// Process not running, safe to remove from map and cleanup
			delete(fm.processes, key)
		}

		if err := fm.cleanupMountPoint(mntPath); err != nil {
			return err
		}
		klog.Infof("Successfully cleaned up stale mount point: %s", mntPath)
		statErr = nil // Reset to treat as not exist
	}

	if statErr == nil {
		// Directory exists and accessible - check if it's already mounted
		klog.Infof("Mount point directory %s exists, checking if mounted", mntPath)

		if mounted, mountErr := isMountPoint(mntPath); mountErr == nil && mounted {
			// Already mounted - verify if it's safe to reuse
			klog.Infof("Mount point %s is already mounted, checking if it's in use", mntPath)

			if proc, exists := fm.processes[key]; exists {
				if isProcessRunning(proc.PID) {
					// Process exists and running, safe to reuse
					klog.Infof("Mount point %s is in use by our FUSE process (PID: %d), will reuse", mntPath, proc.PID)
					return nil
				}
				// Process not running, remove stale entry
				delete(fm.processes, key)
			}

			// The mount survived a csi-plugin restart, so the process map is
			// empty and the FusePID persisted in the mount-state Secret was
			// allocated in a previous container's PID namespace - it cannot be
			// trusted (and may even name an unrelated process after reuse).
			// Only an actual read tells a live FUSE mount from one whose
			// daemon is gone: mountpoint(1) still reports a dead mount.
			if mountPointUsable(mntPath) {
				klog.Warningf("Mount point %s is mounted and readable but not in the process map, reusing it", mntPath)
				return nil
			}

			klog.Warningf("Mount point %s is mounted but its FUSE daemon is gone (stale mount from a previous csi-plugin), unmounting and restaging", mntPath)
			if err := fm.cleanupMountPoint(mntPath); err != nil {
				return err
			}
		} else if dirHasEntries(mntPath) {
			// Leftovers of a previous FUSE session (the driver creates the
			// fs-path sub-directories inside the mount point). curvine-fuse
			// refuses to mount over a non-empty directory ("Mnt ... is not
			// empty"), so the directory must be cleared before mounting.
			klog.Warningf("Mount point directory %s is not mounted but not empty, clearing leftovers before mounting", mntPath)
			if err := fm.cleanupMountPoint(mntPath); err != nil {
				return err
			}
		} else {
			// Not mounted, empty directory - reuse it
			klog.Infof("Reusing existing mount point directory: %s", mntPath)
		}
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
	// Update mount state asynchronously to avoid blocking NodeStageVolume
	go func() {
		if err := fm.mountState.AddMount(mountInfo); err != nil {
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

// mountPointUsable reports whether path can actually be read. A FUSE mount
// whose daemon is gone is still reported by mountpoint(1), but every read
// fails with "transport endpoint is not connected", so the only reliable probe
// is to read the directory.
func mountPointUsable(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	return err == nil || err == io.EOF
}

// dirHasEntries reports whether path exists and contains entries.
func dirHasEntries(path string) bool {
	entries, err := os.ReadDir(path)
	return err == nil && len(entries) > 0
}

// cleanupMountPoint detaches a stale mount (if any) and recreates an empty
// mount point directory so a fresh FUSE process can mount on it.
func (fm *FuseManager) cleanupMountPoint(mntPath string) error {
	if mounted, err := isMountPoint(mntPath); err == nil && mounted {
		cmd := exec.Command("umount", "-l", mntPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			outputStr := string(output)
			if !strings.Contains(outputStr, "not mounted") && !strings.Contains(outputStr, "not found") {
				return fmt.Errorf("failed to unmount stale mount point %s: %v, output: %s",
					mntPath, err, outputStr)
			}
			klog.Infof("Mount point %s already unmounted or not a mount point", mntPath)
		} else {
			klog.Infof("Successfully unmounted stale mount point: %s", mntPath)
		}
	}

	if err := os.RemoveAll(mntPath); err != nil {
		return fmt.Errorf("failed to remove stale mount point directory %s: %v", mntPath, err)
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

		// Update mount state
		if mountInfo, exists := fm.mountState.GetMount(proc.MntPath, proc.ClusterID); exists {
			mountInfo.FusePID = 0
			fm.mountState.AddMount(mountInfo)
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

			// Update mount state
			if mountInfo, exists := hc.fm.mountState.GetMount(proc.MntPath, proc.ClusterID); exists {
				mountInfo.FusePID = 0
				hc.fm.mountState.AddMount(mountInfo)
			}
		}
	}
}
