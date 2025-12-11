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
	nodeID      string
	fuseManager *FuseManager
	mountState  *MountState
	k8sClient   *K8sClient
}

var _ csi.NodeServer = &nodeService{}

func newNodeService(nodeID string) (*nodeService, error) {
	// Initialize Kubernetes client
	kubernetesNamespace := os.Getenv("KUBERNETES_NAMESPACE")
	if kubernetesNamespace == "" {
		kubernetesNamespace = "curvine-system"
	}
	k8sClient, err := NewK8sClient(kubernetesNamespace, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %v", err)
	}

	// Initialize mount state
	mountState, err := NewMountState(k8sClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create mount state: %v", err)
	}

	// Initialize FUSE manager
	fuseManager := NewFuseManager(mountState)

	return &nodeService{
		nodeID:      nodeID,
		fuseManager: fuseManager,
		mountState:  mountState,
		k8sClient:   k8sClient,
	}, nil
}

// Two ways use curvine
// 1. curvine path -> node staging path -> pod target path
// 2. curvine path -> pod target path
// NodeStageVolume is called by the CO when a workload that wants to use the specified volume is placed (scheduled) on a node.
func (n *nodeService) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeStageVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		klog.Errorf("RequestID: %s, Volume ID not provided in NodeStageVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// Get VolumeContext and PublishContext
	volumeContext := request.GetVolumeContext()
	if volumeContext == nil {
		volumeContext = make(map[string]string)
	}

	publishContext := request.GetPublishContext()
	if publishContext == nil {
		publishContext = make(map[string]string)
	}

	// Get required parameters
	masterAddrs := volumeContext["master-addrs"]
	if masterAddrs == "" {
		masterAddrs = publishContext["master-addrs"]
	}
	if masterAddrs == "" {
		klog.Errorf("RequestID: %s, master-addrs not found in volume context or publish context", requestID)
		return nil, status.Error(codes.InvalidArgument, "master-addrs parameter is required")
	}

	// Generate cluster-id from master-addrs
	clusterID := GenerateClusterID(masterAddrs)

	// Generate mnt-path based on cluster-id only
	// All PVs in same cluster share one FUSE process that mounts root path
	mntPath := fmt.Sprintf("/var/lib/kubelet/plugins/kubernetes.io/csi/curvine/%s/fuse-mount", clusterID)
	klog.Infof("RequestID: %s, Generated mnt-path: %s (cluster-id: %s, master-addrs: %s)",
		requestID, mntPath, clusterID, masterAddrs)

	// Collect FUSE parameters from VolumeContext or PublishContext
	fuseParams := make(map[string]string)
	fuseParamKeys := []string{
		"io-threads", "worker-threads", "mnt-per-task", "clone-fd",
		"fuse-channel-size", "stream-channel-size", "auto-cache",
		"direct-io", "kernel-cache", "cache-readdir", "entry-timeout",
		"attr-timeout", "negative-timeout", "max-background",
		"congestion-threshold", "node-cache-size", "node-cache-timeout",
		"master-hostname", "master-rpc-port", "master-web-port", "mnt-number",
	}
	for _, key := range fuseParamKeys {
		// Try VolumeContext first, then PublishContext
		if value, ok := volumeContext[key]; ok && value != "" {
			fuseParams[key] = value
		} else if value, ok := publishContext[key]; ok && value != "" {
			fuseParams[key] = value
		}
	}

	// Check if shared mount point already exists
	mountInfo, exists := n.mountState.GetMount(mntPath, clusterID)
	if exists && mountInfo.FusePID > 0 {
		// Verify process is still running
		if proc, procExists := n.fuseManager.GetFuseProcess(mntPath, clusterID); procExists {
			klog.Infof("RequestID: %s, Shared FUSE mount point already exists for %s@%s (PID: %d)",
				requestID, mntPath, clusterID, proc.PID)
			return &csi.NodeStageVolumeResponse{}, nil
		}
	}

	// Start FUSE process for shared mount point
	// Always mount root path "/" of curvine filesystem
	fsPathToMount := "/"
	klog.Infof("RequestID: %s, Starting FUSE process for cluster: %s, mounting curvine root path: %s to: %s",
		requestID, clusterID, fsPathToMount, mntPath)
	if err := n.fuseManager.StartFuseProcess(ctx, mntPath, clusterID, masterAddrs, fsPathToMount, fuseParams); err != nil {
		klog.Errorf("RequestID: %s, Failed to start FUSE process: %v", requestID, err)
		return nil, status.Errorf(codes.Internal, "Failed to start FUSE process: %v", err)
	}

	// Create staging directory
	stagingPath := request.GetStagingTargetPath()
	if err := os.MkdirAll(stagingPath, 0750); err != nil {
		klog.Errorf("RequestID: %s, Failed to create staging directory at %s: %v", requestID, stagingPath, err)
		return nil, status.Errorf(codes.Internal, "Failed to create staging directory at %s: %v", stagingPath, err)
	}

	klog.Infof("RequestID: %s, Successfully staged volume: %s", requestID, volumeID)
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume is called by the CO when a workload that was using the specified volume is being moved to a different node.
func (n *nodeService) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodeUnstageVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		klog.Errorf("RequestID: %s, Volume ID not provided in NodeUnstageVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// Try to parse VolumeHandle to get cluster-id
	components, parseErr := ParseVolumeHandle(volumeID)
	var clusterID string

	if parseErr == nil {
		clusterID = components.ClusterID
	} else {
		// Non-structured volumeHandle: generate cluster-id from volumeContext
		// (This should not happen for NodeUnstageVolume as volume should have been staged first)
		klog.Warningf("RequestID: %s, Failed to parse volumeHandle, cannot determine cluster-id: %v", requestID, parseErr)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Get mnt-path from mount state by cluster-id
	mountInfo, exists := n.mountState.GetMountByClusterID(clusterID)
	if !exists {
		klog.Warningf("RequestID: %s, Mount info not found for cluster-id %s, skipping cleanup", requestID, components.ClusterID)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	mntPath := mountInfo.MntPath
	if mntPath == "" {
		klog.Warningf("RequestID: %s, mnt-path not found in mount state, skipping cleanup", requestID)
		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	// Remove volume from mount state
	if err := n.mountState.RemoveVolume(mntPath, clusterID, volumeID); err != nil {
		klog.Warningf("RequestID: %s, Failed to remove volume from mount state: %v", requestID, err)
	}

	// Check reference count
	mountInfo, exists = n.mountState.GetMount(mntPath, clusterID)
	if exists && mountInfo.RefCount == 0 {
		// No more volumes using this mount, stop FUSE process
		klog.Infof("RequestID: %s, No more volumes using mount point %s@%s, stopping FUSE process",
			requestID, mntPath, clusterID)
		if err := n.fuseManager.StopFuseProcess(mntPath, clusterID); err != nil {
			klog.Warningf("RequestID: %s, Failed to stop FUSE process: %v", requestID, err)
		}
	}

	klog.Infof("RequestID: %s, Successfully unstaged volume: %s", requestID, volumeID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume mounts the volume on the node.
func (n *nodeService) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, NodePublishVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		klog.Errorf("RequestID: %s, Volume ID not provided in NodePublishVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// Validate basic request parameters
	targetPath := request.GetTargetPath()
	if len(targetPath) == 0 {
		klog.Errorf("RequestID: %s, Target path not provided for volume ID: %s", requestID, volumeID)
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	if request.GetVolumeCapability() == nil {
		klog.Errorf("RequestID: %s, Volume capability not provided for volume ID: %s", requestID, volumeID)
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if !isValidVolumeCapabilities([]*csi.VolumeCapability{request.GetVolumeCapability()}) {
		klog.Errorf("RequestID: %s, Volume capability not supported for volume ID: %s, capability: %+v",
			requestID, volumeID, request.GetVolumeCapability())
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	// Get VolumeContext and PublishContext
	volumeContext := request.GetVolumeContext()
	if volumeContext == nil {
		volumeContext = make(map[string]string)
	}

	publishContext := request.GetPublishContext()
	if publishContext == nil {
		publishContext = make(map[string]string)
	}

	// Get required parameters
	masterAddrs := volumeContext["master-addrs"]
	if masterAddrs == "" {
		masterAddrs = publishContext["master-addrs"]
	}
	if masterAddrs == "" {
		klog.Errorf("RequestID: %s, master-addrs not found in volume context or publish context", requestID)
		return nil, status.Error(codes.InvalidArgument, "master-addrs parameter is required")
	}

	// Get curvine-path
	curvinePath := volumeContext["curvine-path"]
	if curvinePath == "" {
		curvinePath = publishContext["curvine-path"]
	}
	if curvinePath == "" {
		klog.Errorf("RequestID: %s, curvine-path not found in volume context or publish context", requestID)
		return nil, status.Error(codes.InvalidArgument, "curvine-path parameter is required")
	}

	// Generate cluster-id and mnt-path (same as NodeStageVolume)
	clusterID := GenerateClusterID(masterAddrs)
	mntPath := fmt.Sprintf("/var/lib/kubelet/plugins/kubernetes.io/csi/curvine/%s/fuse-mount", clusterID)

	// Ensure shared mount point is ready (call NodeStageVolume logic if needed)
	klog.Infof("RequestID: %s, Checking mount state for mntPath: %s, clusterID: %s, curvine-path: %s",
		requestID, mntPath, clusterID, curvinePath)
	mountInfo, exists := n.mountState.GetMount(mntPath, clusterID)
	if !exists || mountInfo.FusePID == 0 {
		klog.Infof("RequestID: %s, Mount state not found or FUSE PID is 0, need to stage volume", requestID)
		// Need to stage the volume first
		// Merge volumeContext into publishContext for NodeStageVolume
		mergedPublishContext := make(map[string]string)
		for k, v := range publishContext {
			mergedPublishContext[k] = v
		}
		for k, v := range volumeContext {
			if _, exists := mergedPublishContext[k]; !exists {
				mergedPublishContext[k] = v
			}
		}
		stageReq := &csi.NodeStageVolumeRequest{
			VolumeId:          volumeID,
			StagingTargetPath: mntPath,
			PublishContext:    mergedPublishContext,
			VolumeContext:     volumeContext,
		}
		klog.Infof("RequestID: %s, Calling NodeStageVolume from NodePublishVolume", requestID)
		if _, err := n.NodeStageVolume(ctx, stageReq); err != nil {
			klog.Errorf("RequestID: %s, Failed to stage volume: %v", requestID, err)
			return nil, status.Errorf(codes.Internal, "Failed to stage volume: %v", err)
		}
		klog.Infof("RequestID: %s, NodeStageVolume completed successfully", requestID)
	} else {
		klog.Infof("RequestID: %s, Mount state exists, FUSE PID: %d", requestID, mountInfo.FusePID)
	}

	// Calculate host sub-path: mnt-path + curvine-path
	// FUSE mounts curvine root "/" to mnt-path, so curvine-path is accessible at mnt-path + curvine-path
	// For example: mnt-path="/xyz", curvine-path="/pvc-abc" → hostSubPath="/xyz/pvc-abc"
	// Need to strip leading "/" from curvine-path to avoid double slash
	curvineSubPath := strings.TrimPrefix(curvinePath, "/")
	hostSubPath := mntPath
	if curvineSubPath != "" {
		hostSubPath = mntPath + "/" + curvineSubPath
	}
	klog.Infof("RequestID: %s, Calculated host sub-path: %s (mnt-path: %s, curvine-path: %s)",
		requestID, hostSubPath, mntPath, curvinePath)

	// Check if sub-path exists (should be created by Controller in remote storage)
	klog.Infof("RequestID: %s, Checking if sub-path exists: %s", requestID, hostSubPath)
	if _, err := os.Stat(hostSubPath); os.IsNotExist(err) {
		// Sub-path not found - Controller may have failed to create it, or FUSE hasn't synced yet
		klog.Warningf("RequestID: %s, Sub-path %s not found in FUSE mount point. "+
			"This may indicate Controller CreateVolume failed. "+
			"Attempting to create through FUSE as fallback...", requestID, hostSubPath)

		// Try to create through FUSE (will create in remote storage)
		if err := os.MkdirAll(hostSubPath, 0750); err != nil {
			klog.Errorf("RequestID: %s, Failed to create sub-path %s through FUSE: %v",
				requestID, hostSubPath, err)
			return nil, status.Errorf(codes.Internal,
				"Sub-path not found and failed to create through FUSE: %v. "+
					"Please check if Controller CreateVolume succeeded.", err)
		}
		klog.Infof("RequestID: %s, Sub-path created through FUSE (fallback): %s", requestID, hostSubPath)
	} else if err != nil {
		// Other error (permission, etc.)
		klog.Errorf("RequestID: %s, Failed to stat sub-path %s: %v", requestID, hostSubPath, err)
		return nil, status.Errorf(codes.Internal, "Failed to access sub-path: %v", err)
	} else {
		// Sub-path exists (created by Controller as expected)
		klog.Infof("RequestID: %s, Sub-path already exists (created by Controller): %s",
			requestID, hostSubPath)
	}

	// Ensure target path exists
	klog.Infof("RequestID: %s, Creating target path: %s", requestID, targetPath)
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		klog.Errorf("RequestID: %s, Failed to create target path %s: %v", requestID, targetPath, err)
		return nil, status.Errorf(codes.Internal, "Failed to create target path: %v", err)
	}
	klog.Infof("RequestID: %s, Target path created successfully", requestID)

	// Bind mount sub-path to target path
	klog.Infof("RequestID: %s, Bind mounting %s to %s", requestID, hostSubPath, targetPath)
	cmd := exec.Command("mount", "--bind", hostSubPath, targetPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to bind mount %s to %s: %v, output: %s",
			requestID, hostSubPath, targetPath, err, string(output))
		return nil, status.Errorf(codes.Internal, "Failed to bind mount: %v", err)
	}

	// Add volume to mount state and increment reference count (async to avoid blocking)
	klog.Infof("RequestID: %s, Updating mount state for volume", requestID)
	go func() {
		if err := n.mountState.AddVolume(mntPath, clusterID, volumeID); err != nil {
			klog.Warningf("RequestID: %s, Failed to update mount state: %v", requestID, err)
		} else {
			klog.Infof("RequestID: %s, Mount state updated successfully", requestID)
		}
	}()

	klog.Infof("RequestID: %s, Successfully published volume %s at %s", requestID, volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmount the volume from the target path
func (n *nodeService) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
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

	// Check if target path exists and is a mount point before unmounting
	if _, err := os.Stat(target); os.IsNotExist(err) {
		klog.Infof("RequestID: %s, Target path %s does not exist, treating as already unmounted", requestID, target)
		// Path doesn't exist, consider it already unmounted
	} else {
		// Check if it's a mount point
		cmdCheck := exec.Command("mountpoint", "-q", target)
		if err := cmdCheck.Run(); err != nil {
			// Not a mount point, treat as success
			klog.Infof("RequestID: %s, Target path %s is not a mount point, treating as already unmounted", requestID, target)
		} else {
			// It is a mount point, proceed with unmount
			klog.Infof("RequestID: %s, Unmounting volume %s from %s", requestID, volumeID, target)
			cmd := exec.Command("umount", target)

			// Execute command with retry and timeout
			retryCount := 3
			retryInterval := 2 * time.Second
			commandTimeout := 30 * time.Second
			output, err := ExecuteWithRetry(
				cmd,
				retryCount,
				retryInterval,
				commandTimeout,
				[]string{"not mounted", "no mount point specified", "not found"},
			)

			if err != nil {
				klog.Errorf("RequestID: %s, Failed to unmount volume %s from %s: %v, output: %s",
					requestID, volumeID, target, err, string(output))
				return nil, status.Errorf(codes.Internal, "Failed to unmount volume %s from %s: %v, output: %s",
					volumeID, target, err, string(output))
			}
		}
	}

	// Update mount state: remove volume and decrement reference count
	// Iterate through all mounts to find which one contains this volume
	klog.Infof("RequestID: %s, Looking for volume %s in mount state", requestID, volumeID)
	allMounts := n.mountState.GetAllMounts()
	for _, mountInfo := range allMounts {
		// Check if this volume is in the mount's volume list
		for _, volID := range mountInfo.Volumes {
			if volID == volumeID {
				// Found the mount, remove volume from it
				klog.Infof("RequestID: %s, Found volume in mount %s@%s, removing",
					requestID, mountInfo.MntPath, mountInfo.ClusterID)
				if err := n.mountState.RemoveVolume(mountInfo.MntPath, mountInfo.ClusterID, volumeID); err != nil {
					klog.Warningf("RequestID: %s, Failed to update mount state: %v", requestID, err)
				}
				break
			}
		}
	}

	klog.Infof("RequestID: %s, Successfully unpublished volume %s from %s", requestID, volumeID, target)
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

// getCurvinePath determines the curvine filesystem path to mount
func getCurvinePath(publishContext, volumeContext map[string]string, volumeID, requestID string) string {
	// First try to get curvinePath from publishContext
	if curvinePath := publishContext["curvinePath"]; curvinePath != "" {
		klog.Infof("RequestID: %s, Using curvinePath from publishContext: %s", requestID, curvinePath)
		return curvinePath
	}

	// If not in publishContext, try to get from volumeContext
	if curvinePath := volumeContext["curvinePath"]; curvinePath != "" {
		klog.Infof("RequestID: %s, Using curvinePath from volumeContext: %s", requestID, curvinePath)
		return curvinePath
	}

	// If neither exists, use volumeID as curvinePath
	klog.Infof("RequestID: %s, curvinePath not found in contexts, using volumeID as curvinePath: %s", requestID, volumeID)
	return volumeID
}

// buildMountOptions constructs mount options from the request
func buildMountOptions(request *csi.NodePublishVolumeRequest, requestID string) string {
	var mountFlags []string
	volCap := request.GetVolumeCapability()

	// Add mount flags from volume capability
	if m := volCap.GetMount(); m != nil {
		mountFlags = append(mountFlags, m.MountFlags...)
	}

	// Check if volume should be mounted as read-only
	readOnly := request.GetReadonly() ||
		volCap.AccessMode.GetMode() == csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY

	if readOnly {
		mountFlags = append(mountFlags, "ro")
		klog.Infof("RequestID: %s, Volume will be mounted as read-only", requestID)
	} else {
		klog.Infof("RequestID: %s, Volume will be mounted as read-write", requestID)
	}

	// Format mount options as command line arguments
	if len(mountFlags) > 0 {
		return "-o " + strings.Join(mountFlags, ",")
	}
	return ""
}

// appendFuseParameters adds all supported fuse parameters from publishContext
func appendFuseParameters(args []string, publishContext map[string]string, requestID string) []string {
	// Define supported fuse parameters and their types
	fuseParams := map[string]string{
		"master-hostname":      "string",
		"master-rpc-port":      "uint16",
		"mnt-number":           "usize",
		"master-web-port":      "uint16",
		"io-threads":           "usize",
		"worker-threads":       "usize",
		"mnt-per-task":         "usize",
		"clone-fd":             "bool",
		"fuse-channel-size":    "usize",
		"stream-channel-size":  "usize",
		"auto-cache":           "bool",
		"direct-io":            "bool",
		"kernel-cache":         "bool",
		"cache-readdir":        "bool",
		"entry-timeout":        "f64",
		"attr-timeout":         "f64",
		"negative-timeout":     "f64",
		"max-background":       "uint16",
		"congestion-threshold": "uint16",
		"node-cache-size":      "uint64",
		"node-cache-timeout":   "string",
	}

	// Process parameters from publishContext
	for key, value := range publishContext {
		// Skip parameters that are handled elsewhere
		if key == "curvinePath" {
			continue
		}

		// Check if this is a supported fuse parameter
		paramType, isSupported := fuseParams[key]
		if !isSupported {
			klog.Warningf("RequestID: %s, Unsupported fuse parameter: %s", requestID, key)
			continue
		}

		// Validate parameter value
		if !isValidParameterValue(value, paramType, requestID, key) {
			continue
		}

		// Add parameter to command args
		args = append(args, "--"+key, value)
		klog.Infof("RequestID: %s, Added fuse parameter: --%s=%s", requestID, key, value)
	}

	return args
}

func isValidParameterValue(value, paramType, requestID, paramName string) bool {
	if value == "" {
		klog.Warningf("RequestID: %s, Empty value for parameter %s", requestID, paramName)
		return false
	}

	switch paramType {
	case "bool":
		if value != "true" && value != "false" {
			klog.Warningf("RequestID: %s, Invalid boolean value '%s' for parameter %s", requestID, value, paramName)
			return false
		}
	case "string":
		return true
	case "uint16", "usize", "uint64", "f64":
		return true
	default:
		klog.Warningf("RequestID: %s, Unknown parameter type %s for parameter %s", requestID, paramType, paramName)
		return false
	}

	return true
}
