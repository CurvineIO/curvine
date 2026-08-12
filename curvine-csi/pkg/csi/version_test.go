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
	"encoding/json"
	"testing"
)

func TestGetVersionJSONUsesComponentVersionSchema(t *testing.T) {
	versionJSON, err := GetVersionJSON()
	if err != nil {
		t.Fatalf("GetVersionJSON() returned error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(versionJSON), &decoded); err != nil {
		t.Fatalf("version JSON should be valid JSON: %v", err)
	}

	expectedKeys := []string{
		"component",
		"release_version",
		"git_commit",
		"git_tag",
		"git_branch",
		"protocol_version",
		"min_protocol_version",
		"capabilities",
	}
	for _, key := range expectedKeys {
		if _, ok := decoded[key]; !ok {
			t.Fatalf("version JSON missing key %q: %s", key, versionJSON)
		}
	}

	if decoded["component"] != componentName {
		t.Fatalf("component = %v, want %s", decoded["component"], componentName)
	}
	if decoded["DriverVersion"] != nil {
		t.Fatalf("version JSON should use shared snake_case schema: %s", versionJSON)
	}
	if capabilities, ok := decoded["capabilities"].([]any); !ok || len(capabilities) != 0 {
		t.Fatalf("capabilities = %#v, want empty JSON array", decoded["capabilities"])
	}
}
