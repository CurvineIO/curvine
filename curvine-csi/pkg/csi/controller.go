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
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

var (
	// Only capabilities that are actually implemented are advertised.
	// ControllerPublishVolume/ControllerUnpublishVolume return Unimplemented,
	// so PUBLISH_UNPUBLISH_VOLUME must not be declared.
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	}
)

type controllerService struct {
	pvClient *PVClient
}

var _ csi.ControllerServer = &controllerService{}

func newControllerService() *controllerService {
	// Initialize PV client for querying PV volumeAttributes in DeleteVolume
	pvClient, err := NewPVClient()
	if err != nil {
		klog.Warningf("Failed to create PV client: %v. DeleteVolume will rely on structured volumeHandle.", err)
		pvClient = nil
	}

	return &controllerService{
		pvClient: pvClient,
	}
}

// - check if the driver has ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME capability
// - check request argument, e.g. req.VolumeCapabilities is not nil.

// CreateVolume creates a volume
func (d *controllerService) CreateVolume(ctx context.Context, request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, CreateVolume called with request: %+v", requestID, request)

	volumeID := request.GetName()
	if len(volumeID) == 0 {
		klog.Errorf("RequestID: %s, Volume name not provided in CreateVolume request", requestID)
		return nil, status.Error(codes.InvalidArgument, "Volume Name cannot be empty")
	}

	// FIXME: check if the driver has create volume capability.
	if request.VolumeCapabilities == nil {
		klog.Errorf("RequestID: %s, Volume capabilities not provided for volume: %s", requestID, volumeID)
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities cannot be empty")
	}

	requiredCap := request.CapacityRange.GetRequiredBytes()
	klog.Infof("RequestID: %s, Creating volume: %s, size: %d bytes, parameters: %v",
		requestID, request.Name, requiredCap, request.Parameters)

	// Validate StorageClass parameters
	params, err := ValidateStorageClassParams(request.Parameters, requestID)
	if err != nil {
		return nil, err
	}

	validateCtx, cancel := context.WithTimeout(ctx, fuseValidateTimeout())
	defer cancel()
	if err := ValidateFuseParameters(validateCtx, params.MasterAddrs, params.FSPath, params.Passthrough); err != nil {
		klog.Errorf("RequestID: %s, validate-config failed: %v", requestID, err)
		return nil, StatusFromValidateConfigError(err)
	}

	// Generate VolumeHandle: {cluster-id}@{fs-path}@{pv-name}
	pvName := request.Name
	volumeHandle := GenerateVolumeHandle(params.MasterAddrs, params.FSPath, pvName)

	// Calculate curvine filesystem path: {fs-path}/{pv-name}
	curvinePath := GetCurvinePath(params.FSPath, pvName)

	// Ensure fs-path exists before creating curvinePath
	// This ensures the parent directory exists before creating the volume path
	// fs-path creation also follows path-type setting
	if params.FSPath != "/" {
		klog.Infof("RequestID: %s, Checking if fs-path exists: %s", requestID, params.FSPath)
		exists, err := d.isCurvinePathExists(ctx, requestID, params.FSPath, params.MasterAddrs)
		if err != nil {
			return nil, backendStatusError(fmt.Sprintf("Failed to check fs-path %s", params.FSPath), err)
		}
		if !exists {
			// fs-path doesn't exist
			if params.PathType == "DirectoryOrCreate" {
				// Create fs-path if DirectoryOrCreate
				klog.Infof("RequestID: %s, fs-path does not exist, creating: %s (path-type: DirectoryOrCreate)", requestID, params.FSPath)
				err := d.CreateCurinveDir(ctx, requestID, params.FSPath, params.MasterAddrs)
				if err != nil {
					klog.Errorf("RequestID: %s, Failed to create fs-path %s: %v",
						requestID, params.FSPath, err)
					return nil, backendStatusError(fmt.Sprintf("Failed to create fs-path %s", params.FSPath), err)
				}
				klog.Infof("RequestID: %s, Successfully created fs-path: %s", requestID, params.FSPath)
			} else {
				// Directory type requires existing fs-path
				klog.Errorf("RequestID: %s, Directory type requires existing fs-path, but fs-path %s does not exist: %v",
					requestID, params.FSPath, err)
				return nil, status.Errorf(codes.InvalidArgument, "Directory type requires existing fs-path, but fs-path %s does not exist", params.FSPath)
			}
		}
	}

	// Create or verify curvine path based on path-type
	if params.PathType == "DirectoryOrCreate" {
		// Check if directory exists
		klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)
		exists, err := d.isCurvinePathExists(ctx, requestID, curvinePath, params.MasterAddrs)
		if err != nil {
			return nil, backendStatusError(fmt.Sprintf("Failed to check curvine path %s", curvinePath), err)
		}
		if !exists {
			// Directory doesn't exist, create directory
			klog.Infof("RequestID: %s, Curvine path does not exist, creating: %s", requestID, curvinePath)
			err := d.CreateCurinveDir(ctx, requestID, curvinePath, params.MasterAddrs)
			if err != nil {
				klog.Errorf("RequestID: %s, Failed to create curvine directory %s: %v",
					requestID, curvinePath, err)
				return nil, backendStatusError(fmt.Sprintf("Failed to create volume %s", curvinePath), err)
			}
		}
	} else { // Directory type, only check if directory exists
		// Check if directory exists
		klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)
		exists, err := d.isCurvinePathExists(ctx, requestID, curvinePath, params.MasterAddrs)
		if err != nil {
			return nil, backendStatusError(fmt.Sprintf("Failed to check curvine path %s", curvinePath), err)
		}
		if !exists {
			klog.Errorf("RequestID: %s, Directory type requires existing path, but path %s does not exist",
				requestID, curvinePath)
			return nil, status.Errorf(codes.InvalidArgument, "Directory type requires existing path, but path %s does not exist", curvinePath)
		}
	}

	// Build volume context with all parameters for later use
	volCtx := make(map[string]string)
	for k, v := range request.Parameters {
		volCtx[k] = v
	}
	volCtx["author"] = "curvine.io"
	// Store master-addrs in volume context for Node operations
	volCtx["master-addrs"] = params.MasterAddrs
	// Store fs-path in volume context for Node operations (used for FUSE mount)
	volCtx["fs-path"] = params.FSPath
	// Store curvine-path (complete path in curvine filesystem)
	volCtx["curvine-path"] = curvinePath
	// Note: mnt-path will be auto-generated in NodeStageVolume based on volumeHandle

	// Use VolumeHandle as VolumeId
	volume := csi.Volume{
		VolumeId:      volumeHandle,
		CapacityBytes: requiredCap,
		VolumeContext: volCtx,
	}

	klog.Infof("RequestID: %s, Successfully created volume: %s, VolumeHandle: %s, curvinePath: %s",
		requestID, pvName, volumeHandle, curvinePath)
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

	// Query PV to get volumeAttributes
	var masterAddrs string
	var curvinePath string

	if d.pvClient != nil {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		pv, pvErr := d.pvClient.GetPVByVolumeHandle(queryCtx, request.VolumeId)
		if pvErr == nil && pv.Spec.CSI != nil && pv.Spec.CSI.VolumeAttributes != nil {
			// Get master-addrs from volumeAttributes
			masterAddrs = pv.Spec.CSI.VolumeAttributes["master-addrs"]

			// Get curvine-path: try direct path first, then construct from fs-path + pv-name
			if curvinePath = pv.Spec.CSI.VolumeAttributes["curvine-path"]; curvinePath == "" {
				// Try to parse structured volumeHandle
				components, parseErr := ParseVolumeHandle(request.VolumeId)
				if parseErr == nil {
					// Structured volumeHandle: construct path from fs-path + pv-name
					curvinePath = GetCurvinePath(components.FSPath, components.PVName)
					klog.Infof("RequestID: %s, Constructed curvine-path from volumeHandle: %s", requestID, curvinePath)
				} else {
					// Non-structured volumeHandle: construct from fs-path + PV name
					fsPath := pv.Spec.CSI.VolumeAttributes["fs-path"]
					if fsPath == "" {
						fsPath = "/"
					}
					curvinePath = GetCurvinePath(fsPath, pv.Name)
					klog.Infof("RequestID: %s, Constructed curvine-path from PV name: %s", requestID, curvinePath)
				}
			}

			klog.Infof("RequestID: %s, Retrieved from PV volumeAttributes: master-addrs=%s, curvine-path=%s",
				requestID, masterAddrs, curvinePath)
		} else {
			klog.Warningf("RequestID: %s, Failed to query PV: %v", requestID, pvErr)
		}
	} else {
		klog.Warningf("RequestID: %s, PV client not available, cannot query PV", requestID)
	}

	if masterAddrs == "" {
		// Without a reachable cluster we cannot confirm that the directory is
		// gone. Returning success would silently leak the data directory, so
		// surface a retryable error instead.
		klog.Errorf("RequestID: %s, Cannot determine master-addrs for volumeId %s, refusing to report deletion success",
			requestID, request.VolumeId)
		return nil, status.Errorf(codes.Unavailable,
			"cannot determine master-addrs for volumeId %s, cannot verify deletion", request.VolumeId)
	}

	// Delete the path (but not root path)
	if curvinePath == "/" {
		// A root path means the volume attributes are wrong (or the handle is
		// corrupt). Reporting success here would drop the PV without deleting
		// anything and hide the misconfiguration.
		klog.Errorf("RequestID: %s, Refusing to delete root path for volume %s", requestID, request.VolumeId)
		return nil, status.Errorf(codes.FailedPrecondition,
			"refusing to delete root path for volume %s: invalid curvine-path", request.VolumeId)
	}

	// Try to delete the path directly
	// If path doesn't exist, deletion is considered successful (idempotent)
	klog.Infof("RequestID: %s, Deleting curvine path: %s", requestID, curvinePath)
	deleteErr := d.DeleteCurvineDir(ctx, requestID, curvinePath, masterAddrs)
	if deleteErr != nil {
		// Check if path exists - if not, deletion is successful (idempotent)
		exists, checkErr := d.isCurvinePathExists(ctx, requestID, curvinePath, masterAddrs)
		if checkErr != nil {
			// The backend could not be queried (master unreachable, timeout,
			// permissions). Reporting success here would let Kubernetes delete the
			// PV while the data directory still exists, so return a retryable error.
			klog.Errorf("RequestID: %s, Cannot verify deletion of %s: %v", requestID, curvinePath, checkErr)
			return nil, status.Errorf(codes.Unavailable,
				"cannot verify deletion of %s, will retry: %v", curvinePath, checkErr)
		}
		if !exists {
			// Path doesn't exist, deletion considered successful (idempotent)
			klog.Infof("RequestID: %s, Curvine path %s does not exist after delete attempt, deletion considered successful", requestID, curvinePath)
			return &csi.DeleteVolumeResponse{}, nil
		}
		// Path still exists, deletion failed
		klog.Errorf("RequestID: %s, Failed to delete curvine directory %s: %v, path still exists", requestID, curvinePath, deleteErr)
		return nil, status.Errorf(codes.Internal, "Failed to delete curvine directory %s: %v", curvinePath, deleteErr)
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
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerUnpublishVolume unpublish a volume
func (d *controllerService) ControllerUnpublishVolume(ctx context.Context, request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
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

func (d *controllerService) CreateCurinveDir(ctx context.Context, requestID string, curvinePath string, masterAddrs string) error {
	klog.Infof("RequestID: %s, Creating curvine directory: %s", requestID, curvinePath)

	// Validate path security
	if err := ValidatePath(curvinePath); err != nil {
		klog.Errorf("RequestID: %s, Invalid curvine path %s: %v", requestID, curvinePath, err)
		return status.Errorf(codes.InvalidArgument, "Invalid curvine path: %v", err)
	}

	// Build command with master-addrs if provided
	curvineCliPath := "/opt/curvine/curvine-cli"
	args := []string{"fs", "mkdir", "-p", curvinePath}
	// Add --master-addrs parameter if provided
	if masterAddrs != "" {
		args = append(args, "--master-addrs", masterAddrs)
	}
	cmd := exec.Command(curvineCliPath, args...)

	// Execute command with retry and timeout
	retryCount := 3
	retryInterval := 2 * time.Second
	commandTimeout := 30 * time.Second
	output, err := ExecuteWithRetry(
		ctx,
		cmd,
		retryCount,
		retryInterval,
		commandTimeout,
		nil,
	)

	if err != nil {
		klog.Errorf("RequestID: %s, Failed to create curvine directory %s: %v, output: %s",
			requestID, curvinePath, err, string(output))
		return status.Errorf(codes.Internal, "Failed to create directory: %v", err)
	}

	klog.Infof("RequestID: %s, Successfully created curvine directory: %s", requestID, curvinePath)
	return nil
}

// isCurvineNotFound reports whether the CLI ran and reported that the
// requested path does not exist.
//
// A command that never started (missing or non-executable binary, unreadable
// path), a timeout or a cancelled context never proves absence: those errors
// carry the same POSIX wording ("no such file or directory") as a genuine
// "path missing" message, so they are rejected before any text matching.
func isCurvineNotFound(output []byte, err error, curvinePath string) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	// Require the requested path and the marker on the same line, and strip the
	// path before looking for the marker so a path that itself contains
	// "not found" cannot match an unrelated permission error.
	path := strings.ToLower(curvinePath)
	for _, line := range strings.Split(strings.ToLower(string(output)+"\n"+err.Error()), "\n") {
		if !strings.Contains(line, path) {
			continue
		}
		rest := strings.ReplaceAll(line, path, "")
		if strings.Contains(rest, "not found") ||
			strings.Contains(rest, "does not exist") ||
			strings.Contains(rest, "no such file or directory") {
			return true
		}
	}
	return false
}

// isCurvinePathExists returns (exists, error). A missing path is reported as
// (false, nil); transport/permission failures are returned as errors so callers
// can distinguish "not there" from "cannot tell".
func (d *controllerService) isCurvinePathExists(ctx context.Context, requestID string, curvinePath string, masterAddrs string) (bool, error) {
	klog.Infof("RequestID: %s, Checking if curvine path exists: %s", requestID, curvinePath)

	// Validate path security
	if err := ValidatePath(curvinePath); err != nil {
		klog.Errorf("RequestID: %s, Invalid curvine path %s: %v", requestID, curvinePath, err)
		return false, status.Errorf(codes.InvalidArgument, "Invalid curvine path: %v", err)
	}

	curvineCliPath := "/opt/curvine/curvine-cli"
	args := []string{"fs", "ls", curvinePath}
	// Add --master-addrs parameter if provided
	if masterAddrs != "" {
		args = append(args, "--master-addrs", masterAddrs)
	}
	cmd := exec.Command(curvineCliPath, args...)

	// Execute command with retry and timeout
	retryCount := 3
	retryInterval := 2 * time.Second
	commandTimeout := 30 * time.Second
	output, err := ExecuteWithRetry(
		ctx,
		cmd,
		retryCount,
		retryInterval,
		commandTimeout,
		nil,
	)

	if err != nil {
		if isCurvineNotFound(output, err, curvinePath) {
			klog.Infof("RequestID: %s, Curvine path %s does not exist: %v, output: %s",
				requestID, curvinePath, err, string(output))
			return false, nil
		}
		klog.Errorf("RequestID: %s, Failed to check curvine path %s: %v, output: %s",
			requestID, curvinePath, err, string(output))
		return false, fmt.Errorf("failed to check curvine path %s: %w", curvinePath, err)
	}

	klog.Infof("RequestID: %s, Curvine path exists: %s", requestID, curvinePath)
	return true, nil
}

// DeleteCurvineDir deletes a directory in curvine filesystem
func (d *controllerService) DeleteCurvineDir(ctx context.Context, requestID string, curvinePath string, masterAddrs string) error {
	klog.Infof("RequestID: %s, Deleting curvine directory: %s", requestID, curvinePath)

	// Validate path security
	if err := ValidatePath(curvinePath); err != nil {
		klog.Errorf("RequestID: %s, Invalid curvine path %s: %v", requestID, curvinePath, err)
		return status.Errorf(codes.InvalidArgument, "Invalid curvine path: %v", err)
	}

	curvineCliPath := "/opt/curvine/curvine-cli"
	args := []string{"fs", "rm", "-r", curvinePath}
	// Add --master-addrs parameter if provided
	if masterAddrs != "" {
		args = append(args, "--master-addrs", masterAddrs)
	}
	cmd := exec.Command(curvineCliPath, args...)

	// Execute command with retry and timeout
	// For delete operations, use shorter timeout and fewer retries to avoid blocking
	retryCount := 1
	retryInterval := 1 * time.Second
	commandTimeout := 15 * time.Second
	output, err := ExecuteWithRetry(
		ctx,
		cmd,
		retryCount,
		retryInterval,
		commandTimeout,
		nil,
	)

	if err != nil {
		klog.Errorf("RequestID: %s, Failed to delete curvine directory %s: %v, output: %s",
			requestID, curvinePath, err, string(output))
		return status.Errorf(codes.Internal, "Failed to delete directory: %v", err)
	}

	klog.Infof("RequestID: %s, Successfully deleted curvine directory: %s", requestID, curvinePath)
	return nil
}
