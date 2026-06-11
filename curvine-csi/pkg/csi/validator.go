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

// StorageClassParams represents validated StorageClass parameters.
type StorageClassParams struct {
	MasterAddrs string
	FSPath      string
	PathType    string
	Passthrough map[string]string
}

// ValidateStorageClassParams validates StorageClass parameters.
func ValidateStorageClassParams(params map[string]string, requestID string) (*StorageClassParams, error) {
	if err := RejectDisallowedVolumeParameters(params, nil, requestID); err != nil {
		return nil, err
	}

	masterAddrs, ok := params["master-addrs"]
	if !ok || masterAddrs == "" {
		klog.Errorf("RequestID: %s, Parameter 'master-addrs' is required", requestID)
		return nil, status.Error(codes.InvalidArgument, "Parameter 'master-addrs' is required")
	}
	if err := ValidateMasterAddrs(masterAddrs); err != nil {
		klog.Errorf("RequestID: %s, Invalid master-addrs format: %v", requestID, err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid master-addrs format: %v", err)
	}

	fsPath, ok := params["fs-path"]
	if !ok || fsPath == "" {
		fsPath = "/"
		klog.Infof("RequestID: %s, fs-path not specified, using default: /", requestID)
	}
	if err := ValidateFSPath(fsPath); err != nil {
		klog.Errorf("RequestID: %s, Invalid fs-path: %v", requestID, err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid fs-path: %v", err)
	}

	pathType, ok := params["path-type"]
	if !ok || pathType == "" {
		pathType = "Directory"
	}
	if pathType != "Directory" && pathType != "DirectoryOrCreate" {
		klog.Errorf("RequestID: %s, Invalid path-type: %s, must be 'Directory' or 'DirectoryOrCreate'", requestID, pathType)
		return nil, status.Error(codes.InvalidArgument, "path-type must be 'Directory' or 'DirectoryOrCreate'")
	}

	passthrough := CollectPassthroughParams(params, nil)

	klog.Infof("RequestID: %s, Validated StorageClass parameters: master-addrs=%s, fs-path=%s, path-type=%s, passthrough-keys=%d",
		requestID, masterAddrs, fsPath, pathType, len(passthrough))

	return &StorageClassParams{
		MasterAddrs: masterAddrs,
		FSPath:      fsPath,
		PathType:    pathType,
		Passthrough: passthrough,
	}, nil
}

// ValidateMasterAddrs validates master-addrs format.
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

// ValidateFSPath validates fs-path for volume handle generation and mount usage.
func ValidateFSPath(fsPath string) error {
	if err := ValidatePath(fsPath); err != nil {
		return err
	}
	if strings.Contains(fsPath, VolumeHandleSeparator) {
		return fmt.Errorf("fs-path must not contain %q", VolumeHandleSeparator)
	}
	return nil
}
