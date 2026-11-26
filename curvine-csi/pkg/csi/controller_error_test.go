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
	"os"
	"os/exec"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// exitError returns a real *exec.ExitError, the only error shape that proves
// the CLI actually ran and reported a failure.
func exitError(t *testing.T) *exec.ExitError {
	t.Helper()
	err := exec.Command("sh", "-c", "exit 1").Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("expected *exec.ExitError, got %v", err)
	}
	return exitErr
}

func TestIsCurvineNotFound(t *testing.T) {
	exitErr := exitError(t)
	missingCLI := exec.Command("/nonexistent/curvine-cli", "fs", "ls", "/data/pvc-1").Run()

	cases := []struct {
		name   string
		output string
		path   string
		err    error
		want   bool
	}{
		{
			name:   "cli reports not found",
			output: "Error: path /data/pvc-1 not found",
			path:   "/data/pvc-1",
			err:    exitErr,
			want:   true,
		},
		{
			name:   "posix style output",
			output: "ls: /data/pvc-1: No such file or directory",
			path:   "/data/pvc-1",
			err:    exitErr,
			want:   true,
		},
		{
			name:   "does not exist",
			output: "directory /data/pvc-1 does not exist",
			path:   "/data/pvc-1",
			err:    exitErr,
			want:   true,
		},
		{
			name:   "master unreachable is not a missing path",
			output: "dial tcp 192.0.2.1:8995: connect: connection refused",
			path:   "/data/pvc-1",
			err:    exitErr,
			want:   false,
		},
		{
			name:   "permission denied is not a missing path",
			output: "Cannot remove '/data/pvc-1': permission denied",
			path:   "/data/pvc-1",
			err:    exitErr,
			want:   false,
		},
		{
			name:   "another path being missing is not this path",
			output: "Error: path /data/other not found",
			path:   "/data/pvc-1",
			err:    exitErr,
			want:   false,
		},
		{
			name:   "success is not a missing path",
			output: "total 0",
			path:   "/data/pvc-1",
			err:    nil,
			want:   false,
		},
		{
			// "fork/exec ...: no such file or directory" must never be read as
			// "the target directory is gone".
			name:   "missing cli is not a missing path",
			output: "",
			path:   "/data/pvc-1",
			err:    missingCLI,
			want:   false,
		},
		{
			// The marker appears only inside the path itself.
			name:   "path containing marker words",
			output: "Cannot remove '/data/not found': permission denied",
			path:   "/data/not found",
			err:    exitErr,
			want:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCurvineNotFound([]byte(tc.output), tc.err, tc.path); got != tc.want {
				t.Fatalf("isCurvineNotFound(%q, %v, %q) = %v, want %v",
					tc.output, tc.err, tc.path, got, tc.want)
			}
		})
	}
}

func TestNonRetryableCommandError(t *testing.T) {
	missingCLI := exec.Command("/nonexistent/curvine-cli").Run()

	if !nonRetryableCommandError(missingCLI) {
		t.Fatalf("missing CLI error %v should not be retried", missingCLI)
	}
	if !nonRetryableCommandError(exec.ErrNotFound) {
		t.Fatal("exec.ErrNotFound should not be retried")
	}
	if nonRetryableCommandError(exitError(t)) {
		t.Fatal("a non-zero exit status is retryable")
	}
	if nonRetryableCommandError(context.DeadlineExceeded) {
		t.Fatal("a timeout is retryable")
	}
	if nonRetryableCommandError(nil) {
		t.Fatal("nil is not an error")
	}
}

func TestExecuteWithRetryHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ExecuteWithRetry(ctx, exec.Command("true"), 3, time.Millisecond, time.Second, nil)
	if err == nil {
		t.Fatal("expected an error for a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled in the chain, got %v", err)
	}
}

func TestExecuteWithRetryKeepsMissingCLIType(t *testing.T) {
	_, err := ExecuteWithRetry(context.Background(), exec.Command("/nonexistent/curvine-cli"),
		5, time.Millisecond, time.Second, nil)
	// Depending on the Go version this is an *exec.Error or an *os.PathError;
	// both must stay reachable through the retry wrapper.
	var execErr *exec.Error
	var pathErr *os.PathError
	if !errors.As(err, &execErr) && !errors.As(err, &pathErr) {
		t.Fatalf("expected a launch error in the chain, got %v", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist in the chain, got %v", err)
	}
}

func TestBackendStatusError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want codes.Code
	}{
		{"timeout is retryable", context.DeadlineExceeded, codes.Unavailable},
		{"cancellation is retryable", context.Canceled, codes.Unavailable},
		{"missing cli is a deployment error", exec.ErrNotFound, codes.FailedPrecondition},
		{"unknown failure stays internal", errors.New("boom"), codes.Internal},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, ok := status.FromError(backendStatusError("op", tc.err))
			if !ok {
				t.Fatalf("expected a gRPC status, got %v", backendStatusError("op", tc.err))
			}
			if st.Code() != tc.want {
				t.Fatalf("got %v, want %v", st.Code(), tc.want)
			}
		})
	}
}
