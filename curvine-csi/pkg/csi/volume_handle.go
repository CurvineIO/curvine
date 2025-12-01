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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"k8s.io/klog"
)

const (
	// VolumeHandleSeparator separates components in VolumeHandle
	VolumeHandleSeparator = "@"
)

// VolumeHandleComponents represents parsed components of a VolumeHandle
type VolumeHandleComponents struct {
	ClusterID string
	FSPath    string
	PVName    string
}

// GenerateClusterID generates a cluster ID from master-addrs using SHA256 hash
// Returns first 8 hex characters of the hash
func GenerateClusterID(masterAddrs string) string {
	hash := sha256.Sum256([]byte(masterAddrs))
	clusterID := hex.EncodeToString(hash[:])[:8]
	klog.V(5).Infof("Generated cluster-id: %s from master-addrs: %s", clusterID, masterAddrs)
	return clusterID
}

// GenerateMountKey generates a unique mount key from master-addrs and fs-path using SHA256 hash
// This ensures different StorageClasses with same master-addrs but different fs-path get different mount points
// Returns first 8 hex characters of the hash
func GenerateMountKey(masterAddrs, fsPath string) string {
	combined := fmt.Sprintf("%s|%s", masterAddrs, fsPath)
	hash := sha256.Sum256([]byte(combined))
	mountKey := hex.EncodeToString(hash[:])[:8]
	klog.V(5).Infof("Generated mount-key: %s from master-addrs: %s, fs-path: %s", mountKey, masterAddrs, fsPath)
	return mountKey
}

// GenerateVolumeHandle generates a VolumeHandle from components
// Format: {cluster-id}@{fs-path}@{pv-name}
func GenerateVolumeHandle(masterAddrs, fsPath, pvName string) string {
	clusterID := GenerateClusterID(masterAddrs)
	volumeHandle := fmt.Sprintf("%s%s%s%s%s", clusterID, VolumeHandleSeparator, fsPath, VolumeHandleSeparator, pvName)
	klog.V(5).Infof("Generated VolumeHandle: %s (cluster-id: %s, fs-path: %s, pv-name: %s)",
		volumeHandle, clusterID, fsPath, pvName)
	return volumeHandle
}

// ParseVolumeHandle parses a VolumeHandle into its components
// Returns cluster-id, fs-path, pv-name, and error
func ParseVolumeHandle(volumeHandle string) (*VolumeHandleComponents, error) {
	if volumeHandle == "" {
		return nil, fmt.Errorf("volume handle is empty")
	}

	parts := strings.Split(volumeHandle, VolumeHandleSeparator)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid volume handle format: %s, expected format: {cluster-id}@{fs-path}@{pv-name}", volumeHandle)
	}

	components := &VolumeHandleComponents{
		ClusterID: parts[0],
		FSPath:    parts[1],
		PVName:    parts[2],
	}

	klog.V(5).Infof("Parsed VolumeHandle: %s -> cluster-id: %s, fs-path: %s, pv-name: %s",
		volumeHandle, components.ClusterID, components.FSPath, components.PVName)

	return components, nil
}

// GetCurvinePath calculates the curvine filesystem path from VolumeHandle components
// Returns fs-path + pv-name (e.g., "/" + "pvc-abc" = "/pvc-abc")
func GetCurvinePath(fsPath, pvName string) string {
	// Ensure fs-path ends with / if it's not root
	curvinePath := fsPath
	if curvinePath != "/" && !strings.HasSuffix(curvinePath, "/") {
		curvinePath += "/"
	}
	curvinePath += pvName
	return curvinePath
}
