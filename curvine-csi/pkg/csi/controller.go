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
	"os/exec"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)


var (
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	}
)

type controllerService struct {
	config *Config
}

var _ csi.ControllerServer = &controllerService{}

func newControllerService(config *Config) *controllerService {
	return &controllerService{config: config}
}

// CreateVolume creates a volume
func (d *controllerService) CreateVolume(ctx context.Context, request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, CreateVolume called with request: %+v", requestID, request)

	if len(request.Name) == 0 {
		klog.Errorf("RequestID: %s, Volume name not provided in CreateVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}
	if request.VolumeCapabilities == nil {
		klog.Errorf("RequestID: %s, Volume capabilities not provided for volume: %s", requestID, request.Name)
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	requiredCap := request.CapacityRange.GetRequiredBytes()
	klog.Infof("RequestID: %s, Creating volume: %s, size: %d bytes, parameters: %v",
		requestID, request.Name, requiredCap, request.Parameters)

	volCtx := make(map[string]string)
	for k, v := range request.Parameters {
		volCtx[k] = v
	}
	volCtx["author"] = "curvine.io"
	// Get "curvinePath" from volCtx
	curvinePath, ok := volCtx["curvinePath"]
	if !ok {
		klog.Errorf("RequestID: %s, Parameter 'curvinePath' not found in volume context", requestID)
		return nil, status.Error(codes.InvalidArgument, "Parameter 'curvinePath' not found")
	}

	// Determine if type is DirectoryOrCreate or Directory
	volType, ok := volCtx["type"]
	if ok {
		klog.Infof("RequestID: %s, Volume type specified: %s", requestID, volType)
	}
	if ok && volType == "DirectoryOrCreate" {
		// Check if directory exists
		klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)
		err := d.isCurvinePathExists(ctx, requestID, curvinePath)
		if err != nil {
			// Directory doesn't exist, create directory
			klog.Infof("RequestID: %s, Curvine path does not exist, creating: %s", requestID, curvinePath)
			err := d.CreateCurinveDir(ctx, requestID, curvinePath)
			if err != nil {
				klog.Errorf("RequestID: %s, Failed to create curvine directory %s: %v",
					requestID, curvinePath, err)
				return nil, status.Errorf(codes.Internal, "Failed to create volume %s: %v",
					curvinePath, err)
			}
		}
	} else { // Default to Directory type, only check if directory exists, don't create
		// Check if directory exists
		klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)
		err := d.isCurvinePathExists(ctx, requestID, curvinePath)
		if err != nil {
			klog.Errorf("RequestID: %s, Directory type requires existing path, but path %s does not exist: %v",
				requestID, curvinePath, err)
			return nil, status.Errorf(codes.Internal, "Directory type requires existing path, but path %s does not exist: %v",
				curvinePath, err)
		}
	}

	// Use curvinePath as VolumeId, so in DeleteVolume we can directly use VolumeId as curvinePath
	volume := csi.Volume{
		VolumeId:      curvinePath,
		CapacityBytes: requiredCap,
		VolumeContext: volCtx,
	}

	klog.Infof("RequestID: %s, Successfully created volume: %s", requestID, curvinePath)
	return &csi.CreateVolumeResponse{Volume: &volume}, nil
}

// DeleteVolume deletes a volume
func (d *controllerService) DeleteVolume(ctx context.Context, request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, DeleteVolume called with request: %+v", requestID, request)

	if len(request.VolumeId) == 0 {
		klog.Errorf("RequestID: %s, Volume ID not provided in DeleteVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	klog.Infof("RequestID: %s, Deleting volume: %s", requestID, request.VolumeId)

	// In CreateVolume, we use curvinePath as VolumeId
	// So here we can directly use VolumeId as curvinePath
	curvinePath := request.VolumeId
	klog.Infof("RequestID: %s, Using curvinePath: %s for deletion", requestID, curvinePath)

	// Build delete command
	cmd := exec.Command(d.config.CurvineCliPath, "fs", "rm", "-r", curvinePath)
	klog.Infof("RequestID: %s, Executing command: %s %s %s %s %s", requestID, d.config.CurvineCliPath, "fs", "rm", "-r", curvinePath)

	// Execute command with retry and timeout
	output, err := ExecuteWithRetry(
		cmd,
		d.config.RetryCount,
		time.Duration(d.config.RetryInterval)*time.Second,
		time.Duration(d.config.CommandTimeout)*time.Second,
	)

	if err != nil {
		klog.Errorf("RequestID: %s, Failed to delete volume %s: %v, output: %s",
			requestID, request.VolumeId, err, string(output))
		return nil, status.Errorf(codes.Internal, "Failed to delete volume %s: %v, output: %s",
			request.VolumeId, err, string(output))
	}

	klog.Infof("RequestID: %s, Successfully deleted volume: %s", requestID, request.VolumeId)
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerGetCapabilities get controller capabilities
func (d *controllerService) ControllerGetCapabilities(ctx context.Context, request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// ControllerPublishVolume publish a volume
// In Kubernetes, this function is used to attach volume to node
func (d *controllerService) ControllerPublishVolume(ctx context.Context, request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, ControllerPublishVolume called with request: %v", requestID, request)

	if len(request.VolumeId) == 0 {
		klog.Errorf("RequestID: %s, Volume ID cannot be empty", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	if len(request.NodeId) == 0 {
		klog.Errorf("RequestID: %s, Node ID cannot be empty", requestID)
		return nil, status.Error(codes.InvalidArgument, "Node ID cannot be empty")
	}

	// For filesystem-based CSI driver, we don't need to perform actual attach operation
	// We only need to verify if volume exists, then return success
	// Actual mount operation is handled in NodePublishVolume

	// Get curvinePath parameter from VolumeContext
	curvinePath, ok := request.VolumeContext["curvinePath"]
	if !ok {
		klog.Errorf("RequestID: %s, Parameter 'curvinePath' not found in volume context", requestID)
		return nil, status.Error(codes.InvalidArgument, "Parameter 'curvinePath' not found in volume context")
	}

	// Check if curvinePath exists
	klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)
	err := d.isCurvinePathExists(ctx, requestID, curvinePath)
	if err != nil {
		klog.Errorf("RequestID: %s, Curvine path does not exist: %s, error: %v", requestID, curvinePath, err)
		return nil, status.Errorf(codes.NotFound, "Volume %s does not exist: %v", curvinePath, err)
	}

	// Build publish context, this will be used in NodeStageVolume and NodePublishVolume
	publishContext := map[string]string{
		"curvinePath": curvinePath,
	}

	// Record node ID, indicating volume has been attached to this node
	klog.Infof("RequestID: %s, Volume %s attached to node %s", requestID, curvinePath, request.NodeId)

	klog.Infof("RequestID: %s, ControllerPublishVolume completed successfully", requestID)
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: publishContext,
	}, nil
}

// ControllerUnpublishVolume unpublish a volume
func (d *controllerService) ControllerUnpublishVolume(ctx context.Context, request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, ControllerUnpublishVolume called with request: %v", requestID, request)

	if len(request.VolumeId) == 0 {
		klog.Errorf("RequestID: %s, Volume ID cannot be empty", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume ID cannot be empty")
	}

	// Here we don't need to perform actual operations, because Curvine CSI driver is filesystem-based
	// Actual mount/unmount operations are handled in NodePublishVolume/NodeUnpublishVolume
	// Here we just need to return success

	klog.Infof("RequestID: %s, ControllerUnpublishVolume completed successfully", requestID)
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

// ValidateVolumeCapabilities validate volume capabilities
func (d *controllerService) ValidateVolumeCapabilities(ctx context.Context, request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListVolumes list volumes
func (d *controllerService) ListVolumes(ctx context.Context, request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// GetCapacity get capacity
func (d *controllerService) GetCapacity(ctx context.Context, request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// CreateSnapshot create a snapshot
func (d *controllerService) CreateSnapshot(ctx context.Context, request *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// DeleteSnapshot delete a snapshot
func (d *controllerService) DeleteSnapshot(ctx context.Context, request *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ListSnapshots list snapshots
func (d *controllerService) ListSnapshots(ctx context.Context, request *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerExpandVolume expand a volume
func (d *controllerService) ControllerExpandVolume(ctx context.Context, request *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetVolume get a volume
func (d *controllerService) ControllerGetVolume(ctx context.Context, request *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) CreateCurinveDir(ctx context.Context, requestID string, curvinePath string) error {
	klog.Infof("RequestID: %s, Creating curvine directory: %s", requestID, curvinePath)

	// Validate path security
	if err := ValidatePath(curvinePath); err != nil {
		klog.Errorf("RequestID: %s, Invalid curvine path %s: %v", requestID, curvinePath, err)
		return status.Errorf(codes.InvalidArgument, "Invalid curvine path: %v", err)
	}

	cmd := exec.Command(d.config.CurvineCliPath, "fs", "mkdir", "-p", curvinePath)

	// Execute command with retry and timeout
	output, err := ExecuteWithRetry(
		cmd,
		d.config.RetryCount,
		time.Duration(d.config.RetryInterval)*time.Second,
		time.Duration(d.config.CommandTimeout)*time.Second,
	)

	if err != nil {
		klog.Errorf("RequestID: %s, Failed to create curvine directory %s: %v, output: %s",
			requestID, curvinePath, err, string(output))
		return status.Errorf(codes.Internal, "Failed to create directory: %v", err)
	}

	klog.Infof("RequestID: %s, Successfully created curvine directory: %s", requestID, curvinePath)
	return nil
}

func (d *controllerService) isCurvinePathExists(ctx context.Context, requestID string, curvinePath string) error {
	klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)

	// Validate path security
	if err := ValidatePath(curvinePath); err != nil {
		klog.Errorf("RequestID: %s, Invalid curvine path %s: %v", requestID, curvinePath, err)
		return status.Errorf(codes.InvalidArgument, "Invalid curvine path: %v", err)
	}

	cmd := exec.Command(d.config.CurvineCliPath, "fs", "ls", curvinePath)

	// Execute command with retry and timeout
	output, err := ExecuteWithRetry(
		cmd,
		d.config.RetryCount,
		time.Duration(d.config.RetryInterval)*time.Second,
		time.Duration(d.config.CommandTimeout)*time.Second,
	)

	if err != nil {
		klog.Infof("RequestID: %s, Curvine path %s does not exist: %v, output: %s",
			requestID, curvinePath, err, string(output))
		return err
	}

	klog.Infof("RequestID: %s, Curvine path exists: %s", requestID, curvinePath)
	return nil
}
