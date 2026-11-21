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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

// ExecuteWithRetry executes command with retry and timeout control.
//
// Cancelling ctx stops both the retry loop and the running command. The final
// error wraps the last failure with %w so callers can use errors.Is/errors.As
// to tell a missing CLI apart from a Curvine-side "path not found".
func ExecuteWithRetry(ctx context.Context, cmd *exec.Cmd, maxRetries int, retryInterval, timeout time.Duration, acceptableErrors []string) ([]byte, error) {
	requestID := generateRequestID()
	cmdInfo := fmt.Sprintf("%s %v", cmd.Path, cmd.Args)

	klog.Infof("RequestID: %s, Command: %s, MaxRetries: %d, Timeout: %v",
		requestID, cmdInfo, maxRetries, timeout)

	var lastOutput []byte
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Wait before retry (skip first attempt), but stay cancellable.
		if attempt > 0 {
			klog.Warningf("RequestID: %s, Retrying command (attempt %d/%d): %s",
				requestID, attempt, maxRetries, cmdInfo)
			select {
			case <-ctx.Done():
				return lastOutput, fmt.Errorf("command cancelled before attempt %d/%d: %w, output: %s",
					attempt, maxRetries, ctx.Err(), string(lastOutput))
			case <-time.After(retryInterval):
			}
		}

		if err := ctx.Err(); err != nil {
			return lastOutput, fmt.Errorf("command cancelled: %w", err)
		}

		// Execute command with timeout
		output, err := executeCommandWithTimeout(ctx, cmd, timeout)

		// Check if execution should be treated as success
		if err == nil || isAcceptableError(err, output, acceptableErrors) {
			if err != nil {
				klog.Infof("RequestID: %s, Command error is acceptable, treating as success: %s",
					requestID, cmdInfo)
			} else {
				klog.Infof("RequestID: %s, Command executed successfully: %s", requestID, cmdInfo)
			}
			return output, nil
		}

		// Log failure and continue retry
		lastOutput = output
		lastErr = err
		klog.Errorf("RequestID: %s, Command failed: %s, Error: %v, Output: %s",
			requestID, cmdInfo, err, string(output))

		// A binary that cannot be launched will never succeed on retry.
		if nonRetryableCommandError(err) {
			return lastOutput, fmt.Errorf("command failed and is not retryable: %w, output: %s",
				err, string(lastOutput))
		}
	}

	return lastOutput, fmt.Errorf("command failed after %d retries: %w, output: %s",
		maxRetries, lastErr, string(lastOutput))
}

// nonRetryableCommandError reports whether the command never ran (missing or
// non-executable binary, unreadable path). Retrying cannot fix these.
func nonRetryableCommandError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, exec.ErrNotFound) || errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrPermission) {
		return true
	}
	var execErr *exec.Error
	if errors.As(err, &execErr) {
		return true
	}
	var pathErr *os.PathError
	return errors.As(err, &pathErr)
}

// backendStatusError maps a CLI/back-end failure to a gRPC status.
//
// Timeouts and cancellations are retryable. A controller that cannot launch the
// CLI is a deployment problem and must stay visible instead of being retried
// forever. Status errors produced closer to the source (for example
// InvalidArgument from path validation) are preserved.
func backendStatusError(op string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.Errorf(codes.Unavailable, "%s: %v", op, err)
	}
	if nonRetryableCommandError(err) {
		return status.Errorf(codes.FailedPrecondition, "%s: curvine CLI unavailable: %v", op, err)
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown && st.Code() != codes.Internal {
		return status.Errorf(st.Code(), "%s: %v", op, err)
	}
	return status.Errorf(codes.Internal, "%s: %v", op, err)
}

// executeCommandWithTimeout executes a command with timeout control, honouring
// cancellation of the parent context as well.
func executeCommandWithTimeout(ctx context.Context, cmd *exec.Cmd, timeout time.Duration) ([]byte, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Create new command with context
	execCmd := exec.CommandContext(attemptCtx, cmd.Path, cmd.Args[1:]...)
	execCmd.Env = cmd.Env
	execCmd.Dir = cmd.Dir

	// Execute command, capturing stderr so error classification sees it.
	output, err := execCmd.CombinedOutput()

	if ctx.Err() != nil {
		return output, fmt.Errorf("command cancelled: %w", ctx.Err())
	}
	// Check if it's a timeout error
	if attemptCtx.Err() == context.DeadlineExceeded {
		return output, fmt.Errorf("command timed out after %v: %w", timeout, context.DeadlineExceeded)
	}

	return output, err
}

// isAcceptableError checks if the error contains any acceptable error patterns
func isAcceptableError(err error, output []byte, acceptableErrors []string) bool {
	if err == nil || len(acceptableErrors) == 0 {
		return false
	}

	errMsg := err.Error()
	outputStr := string(output)

	for _, acceptable := range acceptableErrors {
		if strings.Contains(errMsg, acceptable) || strings.Contains(outputStr, acceptable) {
			return true
		}
	}

	return false
}

// waitForMount waits for mount point to appear and provides detailed error info
//
//nolint:unused
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
//
//nolint:unused
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
