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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"k8s.io/klog"
)


// ExecuteWithRetry executes command with retry and timeout control
func ExecuteWithRetry(cmd *exec.Cmd, maxRetries int, retryInterval, timeout time.Duration) ([]byte, error) {
	var output []byte
	var err error

	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, Command: %s, Args: %v, MaxRetries: %d, Timeout: %v",
		requestID, cmd.Path, cmd.Args, maxRetries, timeout)

	for i := 0; i <= maxRetries; i++ {
		if i > 0 {
			klog.Warningf("RequestID: %s, Retrying command (attempt %d/%d): %s %v",
				requestID, i, maxRetries, cmd.Path, cmd.Args)
			time.Sleep(retryInterval)
		}

		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Create new command with context
		execCmd := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		execCmd.Env = cmd.Env
		execCmd.Dir = cmd.Dir

		// Execute command
		output, err = execCmd.CombinedOutput()

		// Check if it's a timeout error
		if ctx.Err() == context.DeadlineExceeded {
			klog.Errorf("RequestID: %s, Command timed out after %v: %s %v",
				requestID, timeout, cmd.Path, cmd.Args)
			err = fmt.Errorf("command timed out after %v", timeout)
			continue
		}

		// If command executes successfully, return result
		if err == nil {
			klog.Infof("RequestID: %s, Command executed successfully: %s %v",
				requestID, cmd.Path, cmd.Args)
			return output, nil
		}

		klog.Errorf("RequestID: %s, Command failed: %s %v, Error: %v, Output: %s",
			requestID, cmd.Path, cmd.Args, err, string(output))
	}

	return output, fmt.Errorf("command failed after %d retries: %v, output: %s",
		maxRetries, err, string(output))
}

// waitForMount waits for mount point to appear and provides detailed error info
func waitForMount(mountPath string, timeout time.Duration) error {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Define check interval
	tickInterval := 500 * time.Millisecond
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	// Record attempt count
	attempts := 0
	maxAttempts := int(timeout / tickInterval)

	for {
		select {
		case <-ctx.Done():
			// Get mount status information
			mountInfo := getMountInfo(mountPath)
			return fmt.Errorf("timeout waiting for mount point %s to appear after %d attempts. Mount info: %s", 
				mountPath, attempts, mountInfo)
		case <-ticker.C:
			attempts++
			
			// Check if path exists
			if _, err := os.Stat(mountPath); err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return fmt.Errorf("error checking mount path %s: %v", mountPath, err)
			}

			// Check if it's a mount point
			cmd := exec.Command("mountpoint", "-q", mountPath)
			err := cmd.Run()
			if err == nil {
				// Mount point exists
				return nil
			}
			
			// If tried half the attempts but still not successful, record detailed info
			if attempts == maxAttempts/2 {
				mountInfo := getMountInfo(mountPath)
				klog.Warningf("Still waiting for mount point %s after %d attempts. Mount info: %s", 
					mountPath, attempts, mountInfo)
			}
		}
	}
}

// getMountInfo gets detailed mount point info for debugging
func getMountInfo(mountPath string) string {
	// Check if directory exists
	if _, err := os.Stat(mountPath); err != nil {
		return fmt.Sprintf("Directory status: %v", err)
	}
	
	// Check mount point status
	mountpointCmd := exec.Command("mountpoint", "-v", mountPath)
	mountpointOutput, err := mountpointCmd.CombinedOutput()
	mountpointInfo := string(mountpointOutput)
	if err != nil {
		mountpointInfo += fmt.Sprintf(" (error: %v)", err)
	}
	
	// Get system mount info
	findmntCmd := exec.Command("sh", "-c", fmt.Sprintf("findmnt %s 2>&1 || echo 'Not found in findmnt'", mountPath))
	findmntOutput, _ := findmntCmd.CombinedOutput()
	
	// Get process info
	fuseProcessCmd := exec.Command("sh", "-c", fmt.Sprintf("ps aux | grep -E 'fuse|%s' | grep -v grep || echo 'No related processes found'", 
		strings.ReplaceAll(mountPath, "/", "\\/")))
	fuseProcessOutput, _ := fuseProcessCmd.CombinedOutput()
	
	return fmt.Sprintf("Mountpoint check: %s\nFindmnt output: %s\nRelated processes: %s", 
		mountpointInfo, string(findmntOutput), string(fuseProcessOutput))
}

// ValidatePath validates if path is safe, prevents path traversal attacks
func ValidatePath(path string) error {
	// Check if path contains patterns that may cause path traversal
	if strings.Contains(path, "../") || strings.Contains(path, "./") {
		return fmt.Errorf("path contains invalid characters: %s", path)
	}

	// Check if path starts with /
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must be absolute: %s", path)
	}

	return nil
}

// generateRequestID generates unique request ID
func generateRequestID() string {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		// If random number generation fails, use timestamp
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("req-%s", hex.EncodeToString(bytes))
}

// WithMetrics records operation metrics
func WithMetrics(operation string, fn func() error) error {
	startTime := time.Now()
	requestID := generateRequestID()

	klog.Infof("RequestID: %s, Operation: %s started", requestID, operation)

	err := fn()

	duration := time.Since(startTime)
	if err != nil {
		klog.Errorf("RequestID: %s, Operation: %s failed after %v: %v",
			requestID, operation, duration, err)
		// Here can add metrics collection code to record failure count and latency
	} else {
		klog.Infof("RequestID: %s, Operation: %s completed successfully in %v",
			requestID, operation, duration)
		// Here can add metrics collection code to record success count and latency
	}

	return err
}
