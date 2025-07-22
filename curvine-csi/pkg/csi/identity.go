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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"k8s.io/klog"
)


// GetPluginInfo returns the name and version of the plugin
func (d *Driver) GetPluginInfo(ctx context.Context, request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, GetPluginInfo called", requestID)

	// According to CSI spec v1.8.0, need to return plugin name and version info
	// Name must be in DNS subdomain format, max length 63 characters
	// Version is optional but recommended
	resp := &csi.GetPluginInfoResponse{
		Name:          DriverName,
		VendorVersion: driverVersion,
		// Can add additional metadata information
		Manifest: map[string]string{
			"built-by":   "curvine",
			"commit":     gitCommit,
			"build-date": buildDate,
		},
	}

	klog.Infof("RequestID: %s, GetPluginInfo returning: %+v", requestID, resp)
	return resp, nil
}

// GetPluginCapabilities returns the capabilities of the plugin
func (d *Driver) GetPluginCapabilities(ctx context.Context, request *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, GetPluginCapabilities called", requestID)

	// According to CSI spec v1.8.0, need to return plugin capabilities
	capabilities := []*csi.PluginCapability{
		{
			// Declare support for CONTROLLER_SERVICE capability
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		},
		{
			// Declare support for volume expansion capability
			Type: &csi.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
					Type: csi.PluginCapability_VolumeExpansion_OFFLINE,
				},
			},
		},
	}

	resp := &csi.GetPluginCapabilitiesResponse{
		Capabilities: capabilities,
	}

	klog.Infof("RequestID: %s, GetPluginCapabilities returning capabilities: %+v", requestID, resp)
	return resp, nil
}

// Probe returns the health and readiness of the plugin
func (d *Driver) Probe(ctx context.Context, request *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	// Generate request ID for log tracking
	requestID := generateRequestID()
	klog.Infof("RequestID: %s, Probe called", requestID)

	klog.Infof("RequestID: %s, Probe returning success", requestID)
	return &csi.ProbeResponse{}, nil
}
