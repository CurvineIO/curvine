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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
)

// nodeServiceStandalone implements CSI NodeServer using Standalone for FUSE processes
type nodeServiceStandalone struct {
	nodeID            string
	standaloneManager StandaloneMountManager
	k8sClient         *K8sClient
	pvInformer        cache.SharedIndexInformer
	stopCh            chan struct{}
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

	// Get Standalone image from environment (optional)
	// If not set, NewStandaloneManager will automatically detect current pod image
	standaloneImage := os.Getenv(EnvStandaloneImage)

	// Get Standalone ServiceAccount from environment (required, no default)
	standaloneServiceAccount := os.Getenv(EnvStandaloneServiceAccount)
	if standaloneServiceAccount == "" {
		return nil, fmt.Errorf("environment variable %s is required for standalone mode but not set", EnvStandaloneServiceAccount)
	}

	// Initialize Standalone manager
	// If standaloneImage is empty, NewStandaloneManager will automatically detect current pod image
	// and fallback to StandaloneImage default if detection fails
	standaloneManager := NewStandaloneManager(k8sClient.clientset, kubernetesNamespace, nodeID, standaloneImage, standaloneServiceAccount)

	// Recover state from ConfigMap
	ctx := context.Background()
	if err := standaloneManager.RecoverState(ctx); err != nil {
		klog.Warningf("Failed to recover Standalone state: %v", err)
	}

	// Create node service
	nodeService := &nodeServiceStandalone{
		nodeID:            nodeID,
		standaloneManager: standaloneManager,
		k8sClient:         k8sClient,
		stopCh:            make(chan struct{}),
	}

	// Start PV watcher for automatic cleanup
	if err := nodeService.startPVWatcher(); err != nil {
		klog.Warningf("Failed to start PV watcher: %v (cleanup may be delayed)", err)
	}

	// Start garbage collector for orphaned Standalone pods
	nodeService.startGarbageCollector()

	return nodeService, nil
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

	// Get fs-path from VolumeContext or PublishContext (used for FUSE mount)
	// If not specified, default to root path "/"
	fsPathToMount := volumeContext["fs-path"]
	if fsPathToMount == "" {
		fsPathToMount = publishContext["fs-path"]
	}
	if fsPathToMount == "" {
		fsPathToMount = "/"
		klog.Infof("RequestID: %s, fs-path not specified, using default root path: /", requestID)
	} else {
		klog.Infof("RequestID: %s, Using fs-path from context: %s", requestID, fsPathToMount)
	}

	// Generate cluster-id from master-addrs (for logging)
	clusterID := GenerateClusterID(masterAddrs)

	// Generate mount-key from master-addrs + fs-path (for standalone pod sharing)
	mountKey := GenerateMountKey(masterAddrs, fsPathToMount)

	// Ensure Standalone exists and is ready
	opts := &StandaloneOptions{
		ClusterID:   clusterID,
		MountKey:    mountKey,
		MasterAddrs: masterAddrs,
		FSPath:      fsPathToMount,
		NodeName:    n.nodeID,
		Namespace:   n.k8sClient.namespace,
	}

	hostMountPath, ensureErr := n.standaloneManager.EnsureStandalone(ctx, opts)

	// Add volume reference immediately, even if EnsureStandalone failed
	// This ensures the Standalone Pod can be cleaned up when PV is deleted
	// Note: AddVolumeRef will auto-create state entry if not exists
	if refErr := n.standaloneManager.AddVolumeRef(ctx, mountKey, volumeID); refErr != nil {
		klog.Errorf("RequestID: %s, Failed to add volume ref: %v", requestID, refErr)
		// If AddVolumeRef fails, return error immediately
		return nil, status.Errorf(codes.Internal, "Failed to add volume ref: %v", refErr)
	}

	// Now check if EnsureStandalone succeeded
	if ensureErr != nil {
		klog.Errorf("RequestID: %s, Failed to ensure Standalone (volume ref added for cleanup): %v", requestID, ensureErr)
		// Return error but volume ref is already added, so NodeUnstageVolume can clean up
		return nil, status.Errorf(codes.Internal, "Failed to ensure Standalone: %v", ensureErr)
	}

	klog.Infof("RequestID: %s, Standalone ready, hostMountPath: %s, volume ref added", requestID, hostMountPath)
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

	// Extract mount-key from volumeHandle
	// Try to parse as structured volumeHandle: clusterID@fsPath@pvcName
	var mountKey string
	_, err := ParseVolumeHandle(volumeID)
	if err == nil {
		// Structured volumeHandle: generate mount-key from master-addrs + fs-path
		// Note: We need master-addrs to generate mount-key, but we can get it from volumeHandle's clusterID
		// For now, use FindMountKeyByVolumeID to lookup from state
		foundMountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
		if found {
			mountKey = foundMountKey
			klog.Infof("RequestID: %s, Found mountKey %s for volumeID %s via state lookup", requestID, mountKey, volumeID)
		} else {
			klog.Warningf("RequestID: %s, Cannot find mountKey for volumeID %s", requestID, volumeID)
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
	} else {
		// For static PVs or non-structured volumeHandle, search by volumeID
		foundMountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
		if found {
			mountKey = foundMountKey
			klog.Infof("RequestID: %s, Found mountKey %s for volumeID %s via state lookup", requestID, mountKey, volumeID)
		} else {
			klog.Warningf("RequestID: %s, Cannot find mountKey for volumeID %s", requestID, volumeID)
			return &csi.NodeUnstageVolumeResponse{}, nil
		}
	}

	// Remove volume reference
	shouldDelete, err := n.standaloneManager.RemoveVolumeRef(ctx, mountKey, volumeID)
	if err != nil {
		klog.Warningf("RequestID: %s, Failed to remove volume ref: %v", requestID, err)
	}

	// Delete Standalone if no more references
	if shouldDelete {
		klog.Infof("RequestID: %s, No more volume refs, deleting Standalone for mountKey %s", requestID, mountKey)
		if err := n.standaloneManager.DeleteStandalone(ctx, mountKey); err != nil {
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

	// Get fs-path from VolumeContext or PublishContext (used for FUSE mount)
	// If not specified, default to root path "/"
	fsPath := volumeContext["fs-path"]
	if fsPath == "" {
		fsPath = publishContext["fs-path"]
	}
	if fsPath == "" {
		fsPath = "/"
		klog.Infof("RequestID: %s, fs-path not specified, using default root path: /", requestID)
	} else {
		klog.Infof("RequestID: %s, Using fs-path from context: %s", requestID, fsPath)
	}

	// Generate cluster-id from master-addrs (for logging)
	clusterID := GenerateClusterID(masterAddrs)

	// Generate mount-key from master-addrs + fs-path (for standalone pod sharing)
	mountKey := GenerateMountKey(masterAddrs, fsPath)

	// Ensure Standalone exists and is ready
	opts := &StandaloneOptions{
		ClusterID:   clusterID,
		MountKey:    mountKey,
		MasterAddrs: masterAddrs,
		FSPath:      fsPath,
		NodeName:    n.nodeID,
		Namespace:   n.k8sClient.namespace,
	}

	hostMountPath, err := n.standaloneManager.EnsureStandalone(ctx, opts)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to ensure Standalone: %v", requestID, err)
		return nil, status.Errorf(codes.Internal, "Failed to ensure Standalone: %v", err)
	}

	// Calculate host sub-path based on fs-path
	// If fs-path is "/", curvine-path is relative to root
	// If fs-path is not "/", need to calculate relative path from curvine-path
	var hostSubPath string
	if fsPath == "/" {
		// FUSE mounts root, curvine-path is relative to root
		curvineSubPath := strings.TrimPrefix(curvinePath, "/")
		hostSubPath = hostMountPath
		if curvineSubPath != "" {
			hostSubPath = hostMountPath + "/" + curvineSubPath
		}
	} else {
		// FUSE mounts fs-path, need to calculate relative path from curvine-path
		// curvine-path should start with fs-path, remove fs-path prefix to get relative path
		if !strings.HasPrefix(curvinePath, fsPath) {
			klog.Errorf("RequestID: %s, curvine-path %s does not start with fs-path %s", requestID, curvinePath, fsPath)
			return nil, status.Errorf(codes.Internal, "curvine-path %s does not match fs-path %s", curvinePath, fsPath)
		}
		// Remove fs-path prefix from curvine-path
		relativePath := strings.TrimPrefix(curvinePath, fsPath)
		// Remove leading "/" if present
		relativePath = strings.TrimPrefix(relativePath, "/")
		hostSubPath = hostMountPath
		if relativePath != "" {
			hostSubPath = hostMountPath + "/" + relativePath
		}
	}
	klog.Infof("RequestID: %s, Host sub-path: %s (fs-path: %s, curvine-path: %s)", requestID, hostSubPath, fsPath, curvinePath)

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

	// Note: Volume reference was already added in NodeStageVolume
	// No need to add it again here

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

	// For static PVs, volumeID may not contain mountKey, search by volumeID
	var mountKey string
	if clusterID == "" || clusterID == volumeID {
		foundMountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
		if found {
			klog.Infof("RequestID: %s, Found mountKey %s for volumeID %s via state lookup", requestID, foundMountKey, volumeID)
			mountKey = foundMountKey
		} else {
			klog.Warningf("RequestID: %s, Cannot find mountKey for volumeID %s", requestID, volumeID)
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}
	} else {
		// For structured volumeHandle, we need to get mountKey from volumeHandle
		// Parse volumeHandle to get fs-path, then generate mountKey
		_, err := ParseVolumeHandle(volumeID)
		if err == nil {
			// We need master-addrs to generate mountKey, but we don't have it here
			// Use FindMountKeyByVolumeID instead
			foundMountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
			if found {
				mountKey = foundMountKey
			} else {
				klog.Warningf("RequestID: %s, Cannot find mountKey for volumeID %s", requestID, volumeID)
				return &csi.NodeUnpublishVolumeResponse{}, nil
			}
		} else {
			foundMountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
			if found {
				mountKey = foundMountKey
			} else {
				klog.Warningf("RequestID: %s, Cannot find mountKey for volumeID %s", requestID, volumeID)
				return &csi.NodeUnpublishVolumeResponse{}, nil
			}
		}
	}

	if mountKey != "" {
		shouldDelete, err := n.standaloneManager.RemoveVolumeRef(ctx, mountKey, volumeID)
		if err != nil {
			klog.Warningf("RequestID: %s, Failed to remove volume ref: %v", requestID, err)
		}

		// Delete Standalone if no more references
		if shouldDelete {
			klog.Infof("RequestID: %s, No more volume refs, deleting Standalone for mountKey %s", requestID, mountKey)
			if err := n.standaloneManager.DeleteStandalone(ctx, mountKey); err != nil {
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

// startPVWatcher starts watching PV deletion events for automatic cleanup
func (n *nodeServiceStandalone) startPVWatcher() error {
	klog.Info("Starting PV watcher for Standalone pod cleanup")

	// Create informer factory with field selector to only watch PVs on this node
	// Note: We can't filter by node in PV list, so we filter in the event handler
	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		n.k8sClient.clientset,
		30*time.Second, // Resync period
	)

	// Create PV informer
	n.pvInformer = informerFactory.Core().V1().PersistentVolumes().Informer()

	// Add event handler for PV deletion
	n.pvInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pv, ok := obj.(*corev1.PersistentVolume)
			if !ok {
				// Handle DeletedFinalStateUnknown
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					klog.Warningf("Failed to decode deleted PV object")
					return
				}
				pv, ok = tombstone.Obj.(*corev1.PersistentVolume)
				if !ok {
					klog.Warningf("Tombstone contained object that is not a PV")
					return
				}
			}

			// Only handle PVs for this CSI driver
			if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == DriverName {
				n.handlePVDeletion(pv)
			}
		},
	})

	// Start informer
	go n.pvInformer.Run(n.stopCh)

	// Wait for cache sync
	if !cache.WaitForCacheSync(n.stopCh, n.pvInformer.HasSynced) {
		return fmt.Errorf("failed to sync PV informer cache")
	}

	klog.Info("PV watcher started successfully")
	return nil
}

// handlePVDeletion handles PV deletion events by cleaning up associated Standalone pods
func (n *nodeServiceStandalone) handlePVDeletion(pv *corev1.PersistentVolume) {
	klog.Infof("PV %s deleted, checking for Standalone cleanup", pv.Name)

	// Extract volumeID from PV
	if pv.Spec.CSI == nil {
		klog.V(4).Infof("PV %s is not a CSI volume, skipping", pv.Name)
		return
	}

	volumeID := pv.Spec.CSI.VolumeHandle
	if volumeID == "" {
		klog.Warningf("PV %s has empty volumeHandle", pv.Name)
		return
	}

	// Try to find mountKey from state
	mountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
	if !found {
		klog.V(4).Infof("No Standalone found for volumeID %s", volumeID)
		return
	}

	klog.Infof("Found mountKey %s for deleted PV %s (volumeID: %s)", mountKey, pv.Name, volumeID)

	// Remove volume reference
	ctx := context.Background()
	shouldDelete, err := n.standaloneManager.RemoveVolumeRef(ctx, mountKey, volumeID)
	if err != nil {
		klog.Warningf("Failed to remove volume ref for PV %s: %v", pv.Name, err)
		return
	}

	// Delete Standalone if no more references
	if shouldDelete {
		klog.Infof("No more volume refs for mountKey %s, deleting Standalone", mountKey)
		if err := n.standaloneManager.DeleteStandalone(ctx, mountKey); err != nil {
			klog.Warningf("Failed to delete Standalone for mountKey %s: %v", mountKey, err)
		} else {
			klog.Infof("Successfully deleted Standalone for mountKey %s", mountKey)
		}
	} else {
		klog.Infof("Standalone for mountKey %s still has volume references, not deleting", mountKey)
	}
}

// startGarbageCollector starts a periodic garbage collector for orphaned Standalone pods
// This serves as a fallback mechanism in case PV watch events are missed
func (n *nodeServiceStandalone) startGarbageCollector() {
	klog.Info("Starting garbage collector for orphaned Standalone pods")

	go func() {
		// Initial cleanup after startup
		time.Sleep(2 * time.Minute)
		n.cleanupOrphanedStandalonePods()

		// Periodic cleanup every 10 minutes
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.cleanupOrphanedStandalonePods()
			case <-n.stopCh:
				klog.Info("Garbage collector stopped")
				return
			}
		}
	}()

	klog.Info("Garbage collector started (interval: 10 minutes)")
}

// cleanupOrphanedStandalonePods cleans up Standalone pods that have no corresponding PVs
func (n *nodeServiceStandalone) cleanupOrphanedStandalonePods() {
	klog.Info("Running garbage collection for orphaned Standalone pods")
	ctx := context.Background()

	// Get all Standalone pods on this node
	listOptions := metav1.ListOptions{
		LabelSelector: "app=curvine-standalone",
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", n.nodeID).String(),
	}

	pods, err := n.k8sClient.clientset.CoreV1().Pods(n.k8sClient.namespace).List(ctx, listOptions)
	if err != nil {
		klog.Warningf("Failed to list Standalone pods: %v", err)
		return
	}

	klog.Infof("Found %d Standalone pods on node %s", len(pods.Items), n.nodeID)

	// Get all PVs to check which ones still exist
	pvList, err := n.k8sClient.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Warningf("Failed to list PVs: %v", err)
		return
	}

	// Build a map of existing volumeIDs
	existingVolumeIDs := make(map[string]bool)
	for _, pv := range pvList.Items {
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == DriverName {
			existingVolumeIDs[pv.Spec.CSI.VolumeHandle] = true
		}
	}

	// Check each Standalone pod
	// Iterate through state to find pods that match
	state := n.standaloneManager.GetState()
	for mountKey, entry := range state.Mounts {
		// Check if pod exists for this mountKey
		podName := entry.PodName
		podExists := false
		for _, pod := range pods.Items {
			if pod.Name == podName {
				podExists = true
				break
			}
		}

		if !podExists {
			// Pod doesn't exist but state has entry, skip (will be cleaned up by other mechanisms)
			continue
		}

		// Check if entry has volumes
		if len(entry.Volumes) == 0 {
			klog.Infof("Standalone pod %s has no volume references, deleting", podName)
			if err := n.standaloneManager.DeleteStandalone(ctx, mountKey); err != nil {
				klog.Warningf("Failed to delete orphaned Standalone %s: %v", podName, err)
			}
			continue
		}

		// Check if any of the volumes still exist
		hasExistingVolume := false
		for _, volumeID := range entry.Volumes {
			if existingVolumeIDs[volumeID] {
				hasExistingVolume = true
				break
			}
		}

		if !hasExistingVolume {
			klog.Infof("Standalone pod %s has no existing PVs, deleting", podName)
			if err := n.standaloneManager.DeleteStandalone(ctx, mountKey); err != nil {
				klog.Warningf("Failed to delete orphaned Standalone %s: %v", podName, err)
			}
		}
	}

	klog.Info("Garbage collection completed")
}

// Stop stops the PV watcher and garbage collector
func (n *nodeServiceStandalone) Stop() {
	klog.Info("Stopping PV watcher and garbage collector")
	close(n.stopCh)
}
