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
	"errors"
	"testing"
)

func TestIsCurvineNotFound(t *testing.T) {
	missing := errors.New("exit status 1")

	cases := []struct {
		name   string
		output string
		err    error
		want   bool
	}{
		{
			name:   "cli reports not found",
			output: "Error: path /data/pvc-1 not found",
			err:    missing,
			want:   true,
		},
		{
			name:   "posix style output",
			output: "ls: /data/pvc-1: No such file or directory",
			err:    missing,
			want:   true,
		},
		{
			name:   "does not exist",
			output: "directory /data/pvc-1 does not exist",
			err:    missing,
			want:   true,
		},
		{
			name:   "master unreachable is not a missing path",
			output: "dial tcp 10.0.0.1:8995: connect: connection refused",
			err:    missing,
			want:   false,
		},
		{
			name:   "permission denied is not a missing path",
			output: "permission denied",
			err:    missing,
			want:   false,
		},
		{
			name:   "success is not a missing path",
			output: "total 0",
			err:    nil,
			want:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCurvineNotFound([]byte(tc.output), tc.err); got != tc.want {
				t.Fatalf("isCurvineNotFound(%q, %v) = %v, want %v", tc.output, tc.err, got, tc.want)
			}
		})
	}
}
