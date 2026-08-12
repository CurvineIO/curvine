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

const (
	componentName      = "csi"
	protocolVersion    = 1
	minProtocolVersion = 1
)

// ComponentVersion is the machine-readable version schema shared by Curvine components.
type ComponentVersion struct {
	Component          string   `json:"component"`
	ReleaseVersion     string   `json:"release_version"`
	GitCommit          string   `json:"git_commit"`
	GitTag             string   `json:"git_tag"`
	GitBranch          string   `json:"git_branch"`
	ProtocolVersion    int      `json:"protocol_version"`
	MinProtocolVersion int      `json:"min_protocol_version"`
	Capabilities       []string `json:"capabilities"`
}

// GetVersion returns the shared ComponentVersion schema.
func GetVersion() ComponentVersion {
	return ComponentVersion{
		Component:          componentName,
		ReleaseVersion:     normalizedReleaseVersion(),
		GitCommit:          normalizedGitCommit(),
		GitTag:             gitTag,
		GitBranch:          normalizedGitBranch(),
		ProtocolVersion:    protocolVersion,
		MinProtocolVersion: minProtocolVersion,
		Capabilities:       []string{},
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
	version := GetVersion()

	// Build source info: prefer tag over branch
	sourceInfo := ""
	if version.GitTag != "" && version.GitTag != "unknown" {
		sourceInfo = fmt.Sprintf(", tag: %s", version.GitTag)
	} else if version.GitBranch != "" && version.GitBranch != "unknown" && version.GitBranch != "HEAD" {
		sourceInfo = fmt.Sprintf(", branch: %s", version.GitBranch)
	}

	return fmt.Sprintf("%s (commit: %s%s)", version.ReleaseVersion, version.GitCommit, sourceInfo)
}

func GetRuntimeVersionMetadata() map[string]string {
	return map[string]string{
		"go-version": runtime.Version(),
		"compiler":   runtime.Compiler,
		"platform":   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

func normalizedReleaseVersion() string {
	if driverVersion == "" || driverVersion == "unknown" {
		return "dev"
	}
	return driverVersion
}

func normalizedGitCommit() string {
	if gitCommit == "" {
		return "unknown"
	}
	return gitCommit
}

func normalizedGitBranch() string {
	if gitBranch == "HEAD" {
		return ""
	}
	return gitBranch
}
