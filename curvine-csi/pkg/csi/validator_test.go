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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateStorageClassParamsRejectsMntPath(t *testing.T) {
	params := map[string]string{
		"master-addrs": "m1:8995",
		"fs-path":      "/data",
		"mnt-path":     "/custom/mnt",
	}

	_, err := ValidateStorageClassParams(params, "test-req")
	if err == nil {
		t.Fatal("expected error for user-provided mnt-path")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", st.Code())
	}
}

func TestValidateStorageClassParamsGeneratesMntPath(t *testing.T) {
	params := map[string]string{
		"master-addrs": "m1:8995",
		"fs-path":      "/data",
		"io-threads":   "4",
	}

	got, err := ValidateStorageClassParams(params, "test-req")
	if err != nil {
		t.Fatalf("ValidateStorageClassParams() error = %v", err)
	}
	want := ComputeFuseMntPath(GenerateMountKeyWithFuseParams("m1:8995", "/data", map[string]string{
		"io-threads": "4",
	}))
	if got.MntPath != want {
		t.Fatalf("MntPath = %q, want %q", got.MntPath, want)
	}
}
