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
	"fmt"
	"runtime"
)

// These are set during build time via -ldflags
var (
	driverVersion = "unknown"
	gitCommit     = "unknown"
	gitTag        = ""
	gitBranch     = ""
	buildDate     = "unknown"
)

// VersionInfo struct
type VersionInfo struct {
	DriverVersion string `json:"DriverVersion"`
	GitCommit     string `json:"GitCommit"`
	GitTag        string `json:"GitTag,omitempty"`
	GitBranch     string `json:"GitBranch,omitempty"`
	BuildDate     string `json:"BuildDate"`
	GoVersion     string `json:"GoVersion"`
	Compiler      string `json:"Compiler"`
	Platform      string `json:"Platform"`
}

// GetVersion returns VersionInfo
func GetVersion() VersionInfo {
	return VersionInfo{
		DriverVersion: driverVersion,
		GitCommit:     gitCommit,
		GitTag:        gitTag,
		GitBranch:     gitBranch,
		BuildDate:     buildDate,
		GoVersion:     runtime.Version(),
		Compiler:      runtime.Compiler,
		Platform:      fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// GetVersionJSON returns version in JSON
func GetVersionJSON() (string, error) {
	info := GetVersion()
	marshalled, err := json.MarshalIndent(&info, "", "  ")
	if err != nil {
		return "", err
	}
	return string(marshalled), nil
}

// GetVersionString returns a simple version string: "version (commit: commit-id, tag/branch: name)"
// This format matches other Curvine components for consistency
func GetVersionString() string {
	version := driverVersion
	if version == "" || version == "unknown" {
		version = "dev"
	}
	commit := gitCommit
	if commit == "" || commit == "unknown" {
		commit = "unknown"
	}

	// Build source info: prefer tag over branch
	sourceInfo := ""
	if gitTag != "" && gitTag != "unknown" {
		sourceInfo = fmt.Sprintf(", tag: %s", gitTag)
	} else if gitBranch != "" && gitBranch != "unknown" && gitBranch != "HEAD" {
		sourceInfo = fmt.Sprintf(", branch: %s", gitBranch)
	}

	return fmt.Sprintf("%s (commit: %s%s)", version, commit, sourceInfo)
}
