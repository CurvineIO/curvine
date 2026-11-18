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
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	mountCleanup      *mountCleanup
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
		return nil, fmt.Errorf("failed to recover Standalone state: %w", err)
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

	if err := RejectDisallowedVolumeParameters(volumeContext, publishContext, requestID); err != nil {
		return nil, err
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

	// Collect FUSE parameters from VolumeContext or PublishContext
	fuseParams := CollectPassthroughParams(volumeContext, publishContext)

	if IsStaticVolumeID(volumeID) {
		validateCtx, cancel := context.WithTimeout(ctx, fuseValidateTimeout())
		defer cancel()
		if err := ValidateFuseParameters(validateCtx, masterAddrs, fsPathToMount, fuseParams); err != nil {
			klog.Errorf("RequestID: %s, validate-config failed for static PV: %v", requestID, err)
			return nil, StatusFromValidateConfigError(err)
		}
	}

	// Generate mount-key from master-addrs + fs-path + fuse-params
	// Including fuse params ensures that StorageClasses with the same cluster endpoint
	// and fs-path but different FUSE parameters each receive their own standalone pod.
	mountKey := GenerateMountKeyWithFuseParams(masterAddrs, fsPathToMount, fuseParams)

	// Ensure Standalone exists and is ready
	opts := &StandaloneOptions{
		ClusterID:   clusterID,
		MountKey:    mountKey,
		MasterAddrs: masterAddrs,
		FSPath:      fsPathToMount,
		NodeName:    n.nodeID,
		Namespace:   n.k8sClient.namespace,
		FuseParams:  fuseParams,
	}

	hostMountPath, err := n.standaloneManager.StageVolume(ctx, opts, volumeID)
	if err != nil {
		return nil, cleanupStatus(ctx, err)
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

	// Kubelet calls unstage after all targets for this volume are unpublished.
	// Keep the last reference discoverable until deletion and persistence succeed.
	cleanupCtx, cancel := context.WithTimeout(ctx, standaloneCleanupTimeout)
	defer cancel()
	if err := cleanupCtx.Err(); err != nil {
		return nil, cleanupStatus(cleanupCtx, err)
	}
	mountKey, found := n.standaloneManager.FindMountKeyByVolumeID(volumeID)
	if !found {
		return &csi.NodeUnstageVolumeResponse{}, nil
	}
	if err := n.standaloneManager.ReleaseVolume(cleanupCtx, mountKey, volumeID); err != nil {
		return nil, cleanupStatus(cleanupCtx, err)
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
	// Static PVs bypass StorageClass parameter validation, so validate the
	// path before it is turned into a host path.
	curvinePath, err := NormalizeCurvinePath(curvinePath)
	if err != nil {
		klog.Errorf("RequestID: %s, Invalid curvine-path: %v", requestID, err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid curvine-path: %v", err)
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
	fsPath, err = NormalizeCurvinePath(fsPath)
	if err != nil {
		klog.Errorf("RequestID: %s, Invalid fs-path: %v", requestID, err)
		return nil, status.Errorf(codes.InvalidArgument, "invalid fs-path: %v", err)
	}

	// Generate cluster-id from master-addrs (for logging)
	clusterID := GenerateClusterID(masterAddrs)

	// Collect FUSE parameters from VolumeContext or PublishContext
	fuseParams := CollectPassthroughParams(volumeContext, publishContext)

	// Generate mount-key from master-addrs + fs-path + fuse-params
	// Including fuse params ensures that StorageClasses with the same cluster endpoint
	// and fs-path but different FUSE parameters each receive their own standalone pod.
	mountKey := GenerateMountKeyWithFuseParams(masterAddrs, fsPath, fuseParams)

	// Ensure Standalone exists and is ready
	opts := &StandaloneOptions{
		ClusterID:   clusterID,
		MountKey:    mountKey,
		MasterAddrs: masterAddrs,
		FSPath:      fsPath,
		NodeName:    n.nodeID,
		Namespace:   n.k8sClient.namespace,
		FuseParams:  fuseParams,
	}

	hostMountPath, err := n.standaloneManager.EnsureStandalone(ctx, opts)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to ensure Standalone: %v", requestID, err)
		return nil, status.Errorf(codes.Internal, "Failed to ensure Standalone: %v", err)
	}

	// Calculate host sub-path based on fs-path. The validated paths keep the
	// derived host path inside the FUSE mount root.
	relativePath, err := CurvineSubPath(fsPath, curvinePath)
	if err != nil {
		klog.Errorf("RequestID: %s, curvine-path %s is not within fs-path %s: %v",
			requestID, curvinePath, fsPath, err)
		return nil, status.Errorf(codes.InvalidArgument,
			"curvine-path %s does not match fs-path %s: %v", curvinePath, fsPath, err)
	}
	hostSubPath, err := JoinMountSubPath(hostMountPath, relativePath)
	if err != nil {
		klog.Errorf("RequestID: %s, Refusing mount sub-path: %v", requestID, err)
		return nil, status.Errorf(codes.Internal, "invalid mount sub-path: %v", err)
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
	readOnly := IsReadOnlyRequest(request)
	cmdCheck := exec.Command("mountpoint", "-q", targetPath)
	if err := cmdCheck.Run(); err == nil {
		klog.Infof("RequestID: %s, Target path %s already mounted", requestID, targetPath)
		if readOnly {
			if output, err := exec.Command("mount", "-o", "remount,bind,ro", targetPath).CombinedOutput(); err != nil {
				return nil, status.Errorf(codes.Internal,
					"failed to remount %s read-only: %v, output: %s", targetPath, err, string(output))
			}
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Bind mount sub-path to target path
	klog.Infof("RequestID: %s, Bind mounting %s to %s (readOnly=%v)", requestID, hostSubPath, targetPath, readOnly)
	if err := BindMount(hostSubPath, targetPath, readOnly); err != nil {
		klog.Errorf("RequestID: %s, %v", requestID, err)
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

	cleanup := n.mountCleanup
	if cleanup == nil {
		cleanup = newMountCleanup()
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, volumeCleanupTimeout)
	defer cancel()
	if err := cleanup.unmount(cleanupCtx, target); err != nil {
		return nil, cleanupStatus(cleanupCtx, err)
	}
	// A volume may still be published to other consumers on this node. Its
	// reference belongs to NodeUnstageVolume, not an individual target.

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

	ctx, cancel := context.WithTimeout(context.Background(), standaloneCleanupTimeout)
	defer cancel()
	if err := n.standaloneManager.ReleaseVolume(ctx, mountKey, volumeID); err != nil {
		klog.Warningf("Failed to release deleted PV %s: %v", pv.Name, err)
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

	// Snapshot references first so a volume staged after the PV list cannot be
	// mistaken for a deleted PV during this pass.
	state := n.standaloneManager.GetState()

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
	for mountKey, entry := range state.Mounts {
		podName := entry.PodName

		// Check if entry has volumes
		if len(entry.Volumes) == 0 {
			klog.Infof("Standalone pod %s has no volume references, deleting", podName)
			if err := n.standaloneManager.DeleteStandalone(ctx, mountKey); err != nil {
				klog.Warningf("Failed to delete orphaned Standalone %s: %v", podName, err)
			}
			continue
		}

		// A live shared consumer must not suppress retries for a deleted PV.
		for _, volumeID := range entry.Volumes {
			if existingVolumeIDs[volumeID] {
				continue
			}
			cleanupCtx, cancel := context.WithTimeout(ctx, standaloneCleanupTimeout)
			err := n.standaloneManager.ReleaseVolume(cleanupCtx, mountKey, volumeID)
			cancel()
			if err != nil {
				klog.Warningf("Failed to release orphaned volume %s: %v", volumeID, err)
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
