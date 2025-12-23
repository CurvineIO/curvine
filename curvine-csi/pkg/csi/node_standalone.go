/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

// nodeServiceStandalone implements CSI NodeServer using Standalone for FUSE processes
type nodeServiceStandalone struct {
	nodeID            string
	standaloneManager StandaloneMountManager
	k8sClient         *K8sClient
}

var _ csi.NodeServer = &nodeServiceStandalone{}

// newNodeServiceStandalone creates a new nodeServiceStandalone
func newNodeServiceStandalone(nodeID string) (*nodeServiceStandalone, error) {
	// Initialize Kubernetes client
	kubernetesNamespace := os.Getenv("KUBERNETES_NAMESPACE")
	if kubernetesNamespace == "" {
		kubernetesNamespace = "curvine"
	}

	k8sClient, err := NewK8sClient(kubernetesNamespace, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %v", err)
	}

	// Get Standalone image from environment
	standaloneImage := os.Getenv("STANDALONE_IMAGE")
	if standaloneImage == "" {
		standaloneImage = StandaloneImage
	}

	// Get Standalone ServiceAccount from environment (required, no default)
	standaloneServiceAccount := os.Getenv(EnvStandaloneServiceAccount)
	if standaloneServiceAccount == "" {
		return nil, fmt.Errorf("environment variable %s is required for standalone mode but not set", EnvStandaloneServiceAccount)
	}

	// Initialize Standalone manager
	standaloneManager := NewStandaloneManager(k8sClient.clientset, kubernetesNamespace, nodeID, standaloneImage, standaloneServiceAccount)

	// Recover state from ConfigMap
	ctx := context.Background()
	if err := standaloneManager.RecoverState(ctx); err != nil {
		klog.Warningf("Failed to recover Standalone state: %v", err)
	}

	return &nodeServiceStandalone{
		nodeID:            nodeID,
		standaloneManager: standaloneManager,
		k8sClient:         k8sClient,
	}, nil
}

// NodeStageVolume stages the volume
func (n *nodeServiceStandalone) NodeStageVolume(ctx context.Context, request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, [Standalone] NodeStageVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
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
		return nil, status.Error(codes.InvalidArgument, "master-addrs parameter is required")
	}

	// Generate cluster-id from master-addrs
	clusterID := GenerateClusterID(masterAddrs)

	// Ensure Standalone exists and is ready
	opts := &StandaloneOptions{
		ClusterID:   clusterID,
		MasterAddrs: masterAddrs,
		FSPath:      "/", // Always mount root
		NodeName:    n.nodeID,
		Namespace:   n.k8sClient.namespace,
	}

	hostMountPath, err := n.standaloneManager.EnsureStandalone(ctx, opts)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to ensure Standalone: %v", requestID, err)
		return nil, status.Errorf(codes.Internal, "Failed to ensure Standalone: %v", err)
	}

	klog.Infof("RequestID: %s, Standalone ready, hostMountPath: %s", requestID, hostMountPath)
	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unstages the volume
func (n *nodeServiceStandalone) NodeUnstageVolume(ctx context.Context, request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, [Standalone] NodeUnstageVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	// Extract cluster-id from volumeHandle
	clusterID := ExtractClusterIDFromVolumeID(volumeID)
	if clusterID == "" {
		// Try to parse as structured volumeHandle
		components, err := ParseVolumeHandle(volumeID)
		if err != nil {
			klog.Warningf("RequestID: %s, Failed to parse volumeHandle: %v", requestID, err)
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
		clusterID = components.ClusterID
	}

	// Remove volume reference
	shouldDelete, err := n.standaloneManager.RemoveVolumeRef(ctx, clusterID, volumeID)
	if err != nil {
		klog.Warningf("RequestID: %s, Failed to remove volume ref: %v", requestID, err)
	}

	// Delete Standalone if no more references
	if shouldDelete {
		klog.Infof("RequestID: %s, No more volume refs, deleting Standalone for cluster %s", requestID, clusterID)
		if err := n.standaloneManager.DeleteStandalone(ctx, clusterID); err != nil {
			klog.Warningf("RequestID: %s, Failed to delete Standalone: %v", requestID, err)
		}
	}

	klog.Infof("RequestID: %s, Successfully unstaged volume: %s", requestID, volumeID)
	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume mounts the volume on the node
func (n *nodeServiceStandalone) NodePublishVolume(ctx context.Context, request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, [Standalone] NodePublishVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	targetPath := request.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	if request.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
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
		return nil, status.Error(codes.InvalidArgument, "master-addrs parameter is required")
	}

	curvinePath := volumeContext["curvine-path"]
	if curvinePath == "" {
		curvinePath = publishContext["curvine-path"]
	}
	if curvinePath == "" {
		return nil, status.Error(codes.InvalidArgument, "curvine-path parameter is required")
	}

	// Generate cluster-id
	clusterID := GenerateClusterID(masterAddrs)

	// Ensure Standalone exists and is ready
	opts := &StandaloneOptions{
		ClusterID:   clusterID,
		MasterAddrs: masterAddrs,
		FSPath:      "/",
		NodeName:    n.nodeID,
		Namespace:   n.k8sClient.namespace,
	}

	hostMountPath, err := n.standaloneManager.EnsureStandalone(ctx, opts)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to ensure Standalone: %v", requestID, err)
		return nil, status.Errorf(codes.Internal, "Failed to ensure Standalone: %v", err)
	}

	// Calculate host sub-path
	curvineSubPath := strings.TrimPrefix(curvinePath, "/")
	hostSubPath := hostMountPath
	if curvineSubPath != "" {
		hostSubPath = hostMountPath + "/" + curvineSubPath
	}
	klog.Infof("RequestID: %s, Host sub-path: %s", requestID, hostSubPath)

	// Check if sub-path exists
	if _, err := os.Stat(hostSubPath); os.IsNotExist(err) {
		klog.Warningf("RequestID: %s, Sub-path %s not found, creating...", requestID, hostSubPath)
		if err := os.MkdirAll(hostSubPath, 0750); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to create sub-path: %v", err)
		}
	}

	// Ensure target path exists
	if err := os.MkdirAll(targetPath, 0750); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create target path: %v", err)
	}

	// Check if already mounted
	cmdCheck := exec.Command("mountpoint", "-q", targetPath)
	if err := cmdCheck.Run(); err == nil {
		klog.Infof("RequestID: %s, Target path %s already mounted", requestID, targetPath)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Bind mount sub-path to target path
	klog.Infof("RequestID: %s, Bind mounting %s to %s", requestID, hostSubPath, targetPath)
	cmd := exec.Command("mount", "--bind", hostSubPath, targetPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to bind mount: %v, output: %s", requestID, err, string(output))
		return nil, status.Errorf(codes.Internal, "Failed to bind mount: %v", err)
	}

	// Add volume reference
	if err := n.standaloneManager.AddVolumeRef(ctx, clusterID, volumeID); err != nil {
		klog.Warningf("RequestID: %s, Failed to add volume ref: %v", requestID, err)
	}

	klog.Infof("RequestID: %s, Successfully published volume %s at %s", requestID, volumeID, targetPath)
	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the target path
func (n *nodeServiceStandalone) NodeUnpublishVolume(ctx context.Context, request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, [Standalone] NodeUnpublishVolume called with request: %+v", requestID, request)

	volumeID := request.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	target := request.GetTargetPath()
	if len(target) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	// Check if target path exists
	if _, err := os.Stat(target); os.IsNotExist(err) {
		klog.Infof("RequestID: %s, Target path %s does not exist", requestID, target)
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	// Check if it's a mount point
	cmdCheck := exec.Command("mountpoint", "-q", target)
	if err := cmdCheck.Run(); err != nil {
		klog.Infof("RequestID: %s, Target path %s is not a mount point", requestID, target)
	} else {
		// Unmount
		klog.Infof("RequestID: %s, Unmounting %s", requestID, target)
		cmd := exec.Command("umount", target)
		output, err := cmd.CombinedOutput()
		if err != nil {
			// Try lazy unmount
			klog.Warningf("RequestID: %s, Normal unmount failed, trying lazy unmount: %v", requestID, err)
			cmd = exec.Command("umount", "-l", target)
			output, err = cmd.CombinedOutput()
			if err != nil {
				klog.Errorf("RequestID: %s, Failed to unmount: %v, output: %s", requestID, err, string(output))
				return nil, status.Errorf(codes.Internal, "Failed to unmount: %v", err)
			}
		}
	}

	// Extract cluster-id and remove volume reference
	clusterID := ExtractClusterIDFromVolumeID(volumeID)
	if clusterID == "" {
		components, err := ParseVolumeHandle(volumeID)
		if err == nil {
			clusterID = components.ClusterID
		}
	}

	if clusterID != "" {
		shouldDelete, err := n.standaloneManager.RemoveVolumeRef(ctx, clusterID, volumeID)
		if err != nil {
			klog.Warningf("RequestID: %s, Failed to remove volume ref: %v", requestID, err)
		}

		// Delete Standalone if no more references
		if shouldDelete {
			klog.Infof("RequestID: %s, No more volume refs, deleting Standalone for cluster %s", requestID, clusterID)
			if err := n.standaloneManager.DeleteStandalone(ctx, clusterID); err != nil {
				klog.Warningf("RequestID: %s, Failed to delete Standalone: %v", requestID, err)
			}
		}
	}

	klog.Infof("RequestID: %s, Successfully unpublished volume %s", requestID, volumeID)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetVolumeStats gets the volume stats
func (n *nodeServiceStandalone) NodeGetVolumeStats(ctx context.Context, request *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats not implemented")
}

// NodeExpandVolume expands the volume
func (n *nodeServiceStandalone) NodeExpandVolume(ctx context.Context, request *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume not implemented")
}

// NodeGetCapabilities gets the node capabilities
func (n *nodeServiceStandalone) NodeGetCapabilities(ctx context.Context, request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	capabilities := []*csi.NodeServiceCapability{
		{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
				},
			},
		},
	}

	return &csi.NodeGetCapabilitiesResponse{Capabilities: capabilities}, nil
}

// NodeGetInfo gets the node info
func (n *nodeServiceStandalone) NodeGetInfo(ctx context.Context, request *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.nodeID}, nil
}
