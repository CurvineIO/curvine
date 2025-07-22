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
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/klog"
)


const (
	// DriverName to be registered
	DriverName = "curvine"
)

type Driver struct {
	*controllerService
	*nodeService

	srv      *grpc.Server
	endpoint string
	config   *Config
}

// NewDriver creates a new driver
func NewDriver(endpoint string, nodeID string, configPath string) *Driver {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, Initializing driver: %v version %v commit %v date %v",
		requestID, DriverName, driverVersion, gitCommit, buildDate)

	// Load configuration
	config, err := LoadConfig(configPath)
	if err != nil {
		klog.Warningf("RequestID: %s, Failed to load config from %s: %v, using default config",
			requestID, configPath, err)
		config = DefaultConfig()
	}

	klog.Infof("RequestID: %s, Driver config: %+v", requestID, config)

	return &Driver{
		endpoint:          endpoint,
		config:            config,
		controllerService: newControllerService(config),
		nodeService:       newNodeService(nodeID, config),
	}
}

// NodeStageVolume is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return d.nodeService.NodeStageVolume(ctx, req)
}

// NodeUnstageVolume is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return d.nodeService.NodeUnstageVolume(ctx, req)
}

// NodePublishVolume is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	return d.nodeService.NodePublishVolume(ctx, req)
}

// NodeUnpublishVolume is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	return d.nodeService.NodeUnpublishVolume(ctx, req)
}

// NodeGetVolumeStats is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return d.nodeService.NodeGetVolumeStats(ctx, req)
}

// NodeExpandVolume is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return d.nodeService.NodeExpandVolume(ctx, req)
}

// NodeGetCapabilities is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return d.nodeService.NodeGetCapabilities(ctx, req)
}

// NodeGetInfo is a proxy method that delegates to the embedded nodeService
func (d *Driver) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return d.nodeService.NodeGetInfo(ctx, req)
}

// CreateVolume is a proxy method that delegates to the embedded controllerService
func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return d.controllerService.CreateVolume(ctx, req)
}

// DeleteVolume is a proxy method that delegates to the embedded controllerService
func (d *Driver) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return d.controllerService.DeleteVolume(ctx, req)
}

// ControllerPublishVolume is a proxy method that delegates to the embedded controllerService
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return d.controllerService.ControllerPublishVolume(ctx, req)
}

// ControllerUnpublishVolume is a proxy method that delegates to the embedded controllerService
func (d *Driver) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return d.controllerService.ControllerUnpublishVolume(ctx, req)
}

// ValidateVolumeCapabilities is a proxy method that delegates to the embedded controllerService
func (d *Driver) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return d.controllerService.ValidateVolumeCapabilities(ctx, req)
}

// ListVolumes is a proxy method that delegates to the embedded controllerService
func (d *Driver) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return d.controllerService.ListVolumes(ctx, req)
}

// GetCapacity is a proxy method that delegates to the embedded controllerService
func (d *Driver) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return d.controllerService.GetCapacity(ctx, req)
}

// ControllerGetCapabilities is a proxy method that delegates to the embedded controllerService
func (d *Driver) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return d.controllerService.ControllerGetCapabilities(ctx, req)
}

// CreateSnapshot is a proxy method that delegates to the embedded controllerService
func (d *Driver) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return d.controllerService.CreateSnapshot(ctx, req)
}

// DeleteSnapshot is a proxy method that delegates to the embedded controllerService
func (d *Driver) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return d.controllerService.DeleteSnapshot(ctx, req)
}

// ListSnapshots is a proxy method that delegates to the embedded controllerService
func (d *Driver) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return d.controllerService.ListSnapshots(ctx, req)
}

// ControllerExpandVolume is a proxy method that delegates to the embedded controllerService
func (d *Driver) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return d.controllerService.ControllerExpandVolume(ctx, req)
}

// ControllerGetVolume is a proxy method that delegates to the embedded controllerService
func (d *Driver) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return d.controllerService.ControllerGetVolume(ctx, req)
}

func (d *Driver) Run() error {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, Starting driver at endpoint: %s", requestID, d.endpoint)

	scheme, addr, err := ParseEndpoint(d.endpoint)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to parse endpoint: %v", requestID, err)
		return err
	}

	listener, err := net.Listen(scheme, addr)
	if err != nil {
		klog.Errorf("RequestID: %s, Failed to listen: %v", requestID, err)
		return err
	}

	logErr := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Generate unique request ID for each gRPC request
		reqID := generateRequestID()

		// Add request start log
		klog.Infof("RequestID: %s, GRPC call: %s, Request: %+v", reqID, info.FullMethod, req)

		// Record request processing time
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start)

		if err != nil {
			klog.Errorf("RequestID: %s, GRPC error: Method: %s, Error: %v, Elapsed: %v",
				reqID, info.FullMethod, err, elapsed)
		} else {
			klog.Infof("RequestID: %s, GRPC success: %s, Elapsed: %v",
				reqID, info.FullMethod, elapsed)
		}

		return resp, err
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logErr),
	}
	d.srv = grpc.NewServer(opts...)

	csi.RegisterIdentityServer(d.srv, d)
	csi.RegisterControllerServer(d.srv, d)
	csi.RegisterNodeServer(d.srv, d)

	klog.Infof("RequestID: %s, Listening for connections on address: %#v", requestID, listener.Addr())
	return d.srv.Serve(listener)
}

func ParseEndpoint(endpoint string) (string, string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("could not parse endpoint: %v", err)
	}

	addr := path.Join(u.Host, filepath.FromSlash(u.Path))

	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "tcp":
	case "unix":
		addr = path.Join("/", addr)
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return "", "", fmt.Errorf("could not remove unix domain socket %q: %v", addr, err)
		}
	default:
		return "", "", fmt.Errorf("unsupported protocol: %s", scheme)
	}

	return scheme, addr, nil
}
