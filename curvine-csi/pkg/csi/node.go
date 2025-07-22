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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)


var (
	volumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		},
		{
			Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		},
	}
)

type nodeService struct {
	nodeID string
	config *Config
}

var _ csi.NodeServer = &nodeService{}

func newNodeService(nodeID string, config *Config) *nodeService {
	return &nodeService{
		nodeID: nodeID,
		config: config,
	}
}

// Two ways use curvine
// 1. curvine path -> node staging path -> pod target path
// 2. curvine path -> pod target path
// NodeStageVolume is called by the CO when a workload that wants to use the specified volume is placed (scheduled) on a node.
func (n *nodeService) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	klog.Infof("NodeStageVolume, request: %v", request)

	// Check if staging path exists
	if _, err := os.Stat(request.GetStagingTargetPath()); err == nil {
		klog.Infof("NodeStageVolume, staging path already exists: %v", request.GetStagingTargetPath())
		return &csi.NodeStageVolumeResponse{}, nil
	}

	// Create staging directory
	stageingPath := request.GetStagingTargetPath()
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, Creating staging directory at: %s", requestID, stageingPath)

	if err := os.MkdirAll(stageingPath, 0750); err != nil {
		klog.Errorf("RequestID: %s, Failed to create staging directory at %s: %v", requestID, stageingPath, err)
		return nil, status.Errorf(codes.Internal, "Failed to create staging directory at %s: %v", stageingPath, err)
	}
	klog.Infof("RequestID: %s, Successfully created staging directory at: %s", requestID, stageingPath)

	//TODO use curvine-fuse mount to staging dir
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume is called by the CO when a workload that was using the specified volume is being moved to a different node.
func (n *nodeService) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeUnstageVolume called with request: %+v", requestID, request)
	return &csi.NodeUnstageVolumeResponse{}, nil

	//umount staging dir
}

// NodePublishVolume mounts the volume on the node.
func (n *nodeService) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		klog.Errorf("RequestID: %s, Volume ID not provided in NodePublishVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// First try to get curvinePath from publishContext
	curvinePath := request.GetPublishContext()["curvinePath"]
	// If not in publishContext, try to get from volumeContext
	if curvinePath == "" {
		curvinePath = request.GetVolumeContext()["curvinePath"]
		if curvinePath == "" {
			// If neither exists, use volumeID as curvinePath
			curvinePath = volumeID
			klog.Infof("RequestID: %s, curvinePath not found in publishContext or volumeContext, using volumeID as curvinePath: %s", requestID, curvinePath)
		}
	} else {
		klog.Infof("RequestID: %s, Using curvinePath from publishContext: %s", requestID, curvinePath)
	}

	target := request.GetTargetPath()
	if len(target) == 0 {
		klog.Errorf("RequestID: %s, Target path not provided for volume ID: %s", requestID, volumeID)
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	volCap := request.GetVolumeCapability()
	if volCap == nil {
		klog.Errorf("RequestID: %s, Volume capability not provided for volume ID: %s", requestID, volumeID)
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if !isValidVolumeCapabilities([]*csi.VolumeCapability{volCap}) {
		klog.Errorf("RequestID: %s, Volume capability not supported for volume ID: %s, capability: %+v",
			requestID, volumeID, volCap)
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	readOnly := false
	if request.GetReadonly() || request.VolumeCapability.AccessMode.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		readOnly = true
	}
	klog.Infof("RequestID: %s, Volume %s will be mounted as %s at %s",
		requestID, volumeID, map[bool]string{true: "read-only", false: "read-write"}[readOnly], target)

	// Build mount options
	var mountFlags []string
	if m := volCap.GetMount(); m != nil {
		mountFlags = append(mountFlags, m.MountFlags...)
	}

	if readOnly {
		mountFlags = append(mountFlags, "ro")
	}

	// Format mount options as command line arguments
	var mountOptions string
	if len(mountFlags) > 0 {
		mountOptions = "-o " + strings.Join(mountFlags, ",")
	}

	klog.Infof("RequestID: %s, Mounting volume %s with options: %s", requestID, volumeID, mountOptions)

	// Use curvine-client binary to mount volume
	var cmd *exec.Cmd
	if mountOptions == "" {
		cmd = exec.Command(n.config.FuseBinaryPath, "-d", "-c", n.config.ConfigFile, "--fs-path", curvinePath, "--mnt-path", target)
	} else {
		cmd = exec.Command(n.config.FuseBinaryPath, "-d", "-c", n.config.ConfigFile, "--fs-path", curvinePath, "--mnt-path", target, mountOptions)
	}

	// Create log file for output redirection
	logFilePath := "/tmp/curvine-fuse.log"
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to create log file %s: %v", requestID, logFilePath, err)
		return nil, status.Errorf(codes.Internal, "Failed to create log file: %v", err)
	}
	defer logFile.Close()

	// Redirect stdout and stderr to log file
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Use nohup to start command, prevent hang signal interference
	nohupCmd := exec.Command("nohup", append([]string{cmd.Path}, cmd.Args[1:]...)...)
	nohupCmd.Stdout = logFile
	nohupCmd.Stderr = logFile

	// Start command
	klog.Infof("RequestID: %s, Starting nohup command: %s %v", requestID, nohupCmd.Path, nohupCmd.Args)
	err = nohupCmd.Start()

	// Record PID for subsequent management
	klog.Infof("RequestID: %s, Started curvine-fuse with PID %d", requestID, nohupCmd.Process.Pid)

	// Asynchronously wait for process to end to catch abnormal exit
	go func() {
		state, err := nohupCmd.Process.Wait()
		if err != nil {
			klog.Errorf("RequestID: %s, Error waiting for nohup process: %v", requestID, err)
			return
		}

		// Check if process exited normally
		if !state.Success() {
			// Read log file content
			logContent := "Failed fetch nohup log"
			if logBytes, readErr := os.ReadFile(logFilePath); readErr == nil {
				logContent = string(logBytes)
			}

			exitCode := state.ExitCode()
			klog.Errorf("RequestID: %s, curvine-fuse process exited unexpectedly with code %d. Log content: %s",
				requestID, exitCode, logContent)

			// Check if mount point still exists
			cmd := exec.Command("mountpoint", "-q", target)
			if err := cmd.Run(); err != nil {
				klog.Errorf("RequestID: %s, Mount point %s is no longer valid after process exit", requestID, target)
			}
		}
	}()

	// Check if mount was successful
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to mount volume %s at %s: %v",
			requestID, volumeID, target, err)
		return nil, status.Errorf(codes.Internal, "Failed to mount volume %s at %s: %v",
			volumeID, target, err)
	}

	klog.Infof("RequestID: %s, Successfully mounted volume %s at %s", requestID, volumeID, target)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmount the volume from the target path
func (n *nodeService) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeUnpublishVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		klog.Errorf("RequestID: %s, Volume ID not provided in NodeUnpublishVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	target := request.GetTargetPath()
	if len(target) == 0 {
		klog.Errorf("RequestID: %s, Target path not provided for volume ID: %s", requestID, volumeID)
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	// Find log file associated with this volume
	logFilePath := fmt.Sprintf("/tmp/curvine-fuse-%s.log", volumeID)

	// Use system umount command to unmount volume
	klog.Infof("RequestID: %s, Unmounting volume %s from %s", requestID, volumeID, target)
	cmd := exec.Command("umount", target)

	// Execute command with retry and timeout
	output, err := ExecuteWithRetry(
		cmd,
		n.config.RetryCount,
		time.Duration(n.config.RetryInterval)*time.Second,
		time.Duration(n.config.CommandTimeout)*time.Second,
	)

	// Try to find and terminate related curvine-fuse processes
	// Use ps and grep to find processes related to mount point
	findCmd := exec.Command("sh", "-c", fmt.Sprintf("ps aux | grep '%s' | grep -v grep | awk '{print $2}'", target))
	findOutput, findErr := findCmd.CombinedOutput()
	if findErr == nil && len(findOutput) > 0 {
		// Found processes, try to terminate them
		pids := strings.Split(strings.TrimSpace(string(findOutput)), "\n")
		for _, pid := range pids {
			if pid == "" {
				continue
			}
			klog.Infof("RequestID: %s, Attempting to terminate curvine-fuse process with PID %s", requestID, pid)
			killCmd := exec.Command("kill", pid)
			killCmd.Run() // Ignore error because process may have already terminated
		}
	}

	if err != nil {
		klog.Errorf("RequestID: %s, Failed to unmount volume %s from %s: %v, output: %s",
			requestID, volumeID, target, err, string(output))
		return nil, status.Errorf(codes.Internal, "Failed to unmount volume %s from %s: %v, output: %s",
			volumeID, target, err, string(output))
	}

	klog.Infof("RequestID: %s, Successfully unmounted volume %s from %s", requestID, volumeID, target)

	// Clean up log file
	if _, err := os.Stat(logFilePath); err == nil {
		klog.Infof("RequestID: %s, Removing log file: %s", requestID, logFilePath)
		if err := os.Remove(logFilePath); err != nil {
			klog.Warningf("RequestID: %s, Failed to remove log file %s: %v", requestID, logFilePath, err)
			// Don't return error because unmount was successful
		}
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats get the volume stats
func (n *nodeService) NodeGetVolumeStats(ctx context.Context, request *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeGetVolumeStats called with request: %+v", requestID, request)
	return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats not implemented")
}

// NodeExpandVolume expand the volume
func (n *nodeService) NodeExpandVolume(ctx context.Context, request *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeExpandVolume called with request: %+v", requestID, request)
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume not implemented")
}

// NodeGetCapabilities get the node capabilities
func (n *nodeService) NodeGetCapabilities(ctx context.Context, request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeGetCapabilities called with request: %+v", requestID, request)

	// According to CSI spec v1.8.0, need to return node capabilities
	// Here we declare support for STAGE_UNSTAGE_VOLUME capability, indicating support for NodeStageVolume and NodeUnstageVolume methods
	capabilities := []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
	}

	response := &csi.NodeGetCapabilitiesResponse{
		Capabilities: capabilities,
	}

	klog.Infof("RequestID: %s, NodeGetCapabilities returning capabilities: %+v", requestID, response)
	return response, nil
}

// NodeGetInfo get the node info
func (n *nodeService) NodeGetInfo(ctx context.Context, request *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeGetInfo called with request: %+v", requestID, request)

	// According to CSI spec v1.8.0, node_id field in NodeGetInfoResponse is required
	// Other fields like max_volumes_per_node and accessible_topology are optional
	response := &csi.NodeGetInfoResponse{
		NodeId: n.nodeID,
		// Can set maximum number of volumes node can publish, if not set or zero, CO will decide how many volumes can be published
		// MaxVolumesPerNode: 0,
		// Can set node topology info, if not set, CO will assume this node can access all volumes
		// AccessibleTopology: nil,
	}

	klog.Infof("RequestID: %s, NodeGetInfo returning: %+v", requestID, response)
	return response, nil
}

func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(cap *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			if c.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}
