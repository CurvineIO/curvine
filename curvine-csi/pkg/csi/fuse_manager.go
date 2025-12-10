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

		// Try to unmount (lazy unmount to handle broken mounts)
		umountCmd := exec.Command("umount", "-l", mntPath)
		if output, err := umountCmd.CombinedOutput(); err != nil {
			klog.Warningf("Unmount %s failed (continuing anyway): %v, output: %s", mntPath, err, string(output))
		}

		// Remove directory
		if err := os.RemoveAll(mntPath); err != nil {
			klog.Warningf("Failed to remove %s: %v", mntPath, err)
		}
		statErr = nil // Reset to treat as not exist
	}

	if statErr == nil {
		// Directory exists and accessible - check if it's already mounted
		klog.Infof("Mount point directory %s exists, checking if mounted", mntPath)

		cmd := exec.Command("mountpoint", "-q", mntPath)
		if err := cmd.Run(); err == nil {
			// Already mounted
			klog.Warningf("Mount point %s is already mounted but not in process map, will reuse", mntPath)
			return nil
		}

		// Not mounted - reuse directory
		klog.Infof("Reusing existing mount point directory: %s", mntPath)
	} else if os.IsNotExist(statErr) {
		// Directory doesn't exist, create it
		if err := os.MkdirAll(mntPath, 0750); err != nil {
			return fmt.Errorf("failed to create mount point directory %s: %v", mntPath, err)
		}
		klog.Infof("Created mount point directory: %s", mntPath)
	}

	// Build FUSE command
	fuseBinaryPath := "/opt/curvine/curvine-fuse"
	args := []string{fuseBinaryPath}

	// Add debug flag if FUSE_DEBUG_ENABLED is set
	if debugEnabled := os.Getenv("FUSE_DEBUG_ENABLED"); debugEnabled == "true" || debugEnabled == "1" {
		args = append(args, "-d")
		klog.Infof("FUSE debug mode enabled for %s", key)
	}

	// Add master-addrs
	if masterAddrs != "" {
		args = append(args, "--master-addrs", masterAddrs)
	}

	// Add paths
	args = append(args, "--fs-path", fsPath)
	args = append(args, "--mnt-path", mntPath)

	// Add FUSE parameters
	for key, value := range fuseParams {
		args = append(args, fmt.Sprintf("--%s", key), value)
	}

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
