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
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

// StorageClassParams represents validated StorageClass parameters
type StorageClassParams struct {
	MasterAddrs string
	MntPath     string
	FSPath      string
	PathType    string
	// Optional FUSE parameters
	FuseParams map[string]string
}

// ValidateStorageClassParams validates StorageClass parameters
func ValidateStorageClassParams(params map[string]string, requestID string) (*StorageClassParams, error) {
	validated := &StorageClassParams{
		FuseParams: make(map[string]string),
	}

	// Validate master-addrs (required) - must validate first as it's needed for mnt-path generation
	masterAddrs, ok := params["master-addrs"]
	if !ok || masterAddrs == "" {
		klog.Errorf("RequestID: %s, Parameter 'master-addrs' is required", requestID)
		return nil, status.Error(codes.InvalidArgument, "Parameter 'master-addrs' is required")
	}
	if err := ValidateMasterAddrs(masterAddrs); err != nil {
		klog.Errorf("RequestID: %s, Invalid master-addrs format: %v", requestID, err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid master-addrs format: %v", err)
	}
	validated.MasterAddrs = masterAddrs

	// Validate fs-path first (needed for mnt-path generation)
	fsPath, ok := params["fs-path"]
	if !ok || fsPath == "" {
		fsPath = "/"
		klog.Infof("RequestID: %s, fs-path not specified, using default: /", requestID)
	}
	if err := ValidatePath(fsPath); err != nil {
		klog.Errorf("RequestID: %s, Invalid fs-path: %v", requestID, err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid fs-path: %v", err)
	}
	validated.FSPath = fsPath

	// Generate mnt-path automatically (mnt-path parameter is now optional and internal)
	// If mnt-path is provided (legacy support), use it; otherwise generate it
	mntPath, ok := params["mnt-path"]
	if !ok || mntPath == "" {
		// Auto-generate mnt-path based on mount-key (master-addrs + fs-path)
		// This ensures different StorageClasses with same master-addrs but different fs-path get different mount points
		// Format: /var/lib/kubelet/plugins/kubernetes.io/csi/curvine/{mount-key}/fuse-mount
		mountKey := GenerateMountKey(masterAddrs, fsPath)
		clusterID := GenerateClusterID(masterAddrs) // Keep for logging
		mntPath = fmt.Sprintf("/var/lib/kubelet/plugins/kubernetes.io/csi/curvine/%s/fuse-mount", mountKey)
		klog.Infof("RequestID: %s, Auto-generated mnt-path: %s (mount-key: %s, cluster-id: %s, fs-path: %s)",
			requestID, mntPath, mountKey, clusterID, fsPath)
	} else {
		// User provided mnt-path (legacy mode)
		klog.Warningf("RequestID: %s, Using user-provided mnt-path: %s (deprecated, will auto-generate in future)",
			requestID, mntPath)
		if err := ValidatePath(mntPath); err != nil {
			klog.Errorf("RequestID: %s, Invalid mnt-path: %v", requestID, err)
			return nil, status.Errorf(codes.InvalidArgument, "Invalid mnt-path: %v", err)
		}
	}
	validated.MntPath = mntPath

	// Validate path-type (optional, default to "Directory")
	pathType, ok := params["path-type"]
	if !ok || pathType == "" {
		pathType = "Directory"
	}
	if pathType != "Directory" && pathType != "DirectoryOrCreate" {
		klog.Errorf("RequestID: %s, Invalid path-type: %s, must be 'Directory' or 'DirectoryOrCreate'", requestID, pathType)
		return nil, status.Error(codes.InvalidArgument, "path-type must be 'Directory' or 'DirectoryOrCreate'")
	}
	validated.PathType = pathType

	// Collect optional FUSE parameters
	fuseParamKeys := []string{
		"io-threads", "worker-threads", "mnt-per-task", "clone-fd",
		"fuse-channel-size", "stream-channel-size", "auto-cache",
		"direct-io", "kernel-cache", "cache-readdir", "entry-timeout",
		"attr-timeout", "negative-timeout", "max-background",
		"congestion-threshold", "node-cache-size", "node-cache-timeout",
		"master-hostname", "master-rpc-port", "master-web-port", "mnt-number",
	}

	for _, key := range fuseParamKeys {
		if value, ok := params[key]; ok && value != "" {
			validated.FuseParams[key] = value
		}
	}

	klog.Infof("RequestID: %s, Validated StorageClass parameters: master-addrs=%s, mnt-path=%s, fs-path=%s, path-type=%s",
		requestID, validated.MasterAddrs, validated.MntPath, validated.FSPath, validated.PathType)

	return validated, nil
}

// ValidateMasterAddrs validates master-addrs format
// Format: host:port,host:port,...
func ValidateMasterAddrs(masterAddrs string) error {
	if masterAddrs == "" {
		return fmt.Errorf("master-addrs cannot be empty")
	}

	addrs := strings.Split(masterAddrs, ",")
	if len(addrs) == 0 {
		return fmt.Errorf("master-addrs must contain at least one address")
	}

	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			return fmt.Errorf("empty address in master-addrs")
		}

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("invalid address format '%s': %v", addr, err)
		}

		if host == "" {
			return fmt.Errorf("host cannot be empty in address '%s'", addr)
		}

		if port == "" {
			return fmt.Errorf("port cannot be empty in address '%s'", addr)
		}
	}

	return nil
}

// ValidateClusterConnection optionally validates cluster connection
// This is a placeholder for future implementation
func ValidateClusterConnection(masterAddrs string, requestID string) error {
	// TODO: Implement actual connection validation if needed
	// For now, just validate the format
	return ValidateMasterAddrs(masterAddrs)
}
