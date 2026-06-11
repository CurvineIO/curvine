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
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

const fuseKubeletPluginBase = "/var/lib/kubelet/plugins/kubernetes.io/csi/curvine"

// reservedVolumeParameterKeys are handled explicitly by the CSI driver and must not
// be forwarded as generic curvine-fuse CLI flags.
var reservedVolumeParameterKeys = map[string]struct{}{
	"master-addrs":  {},
	"fs-path":       {},
	"path-type":     {},
	"curvine-path":  {},
	"author":        {},
	"volume-handle": {},
}

// rejectedVolumeParameterKeys must not appear in StorageClass / PV parameters.
var rejectedVolumeParameterKeys = map[string]struct{}{
	"mnt-path": {},
}

// volumeParameterDenylist blocks keys that are CSI-internal or not valid fuse flags.
var volumeParameterDenylist = map[string]struct{}{
	"csi.storage.k8s.io/ephemeral": {},
	"csi.storage.k8s.io/pod.name":  {},
	"csi.storage.k8s.io/pod.namespace": {},
	"csi.storage.k8s.io/pod.uid":   {},
	"csi.storage.k8s.io/serviceAccount.name": {},
}

// IsReservedVolumeParameterKey reports whether the key is owned by the CSI driver.
func IsReservedVolumeParameterKey(key string) bool {
	_, ok := reservedVolumeParameterKeys[key]
	return ok
}

// IsRejectedVolumeParameterKey reports whether the key must be rejected at provision time.
func IsRejectedVolumeParameterKey(key string) bool {
	_, ok := rejectedVolumeParameterKeys[key]
	return ok
}

// IsDeniedVolumeParameterKey reports whether the key must never be passed to curvine-fuse.
func IsDeniedVolumeParameterKey(key string) bool {
	if _, ok := volumeParameterDenylist[key]; ok {
		return true
	}
	// Kubernetes-injected volume context keys (e.g. storage.kubernetes.io/csiProvisionerIdentity).
	if strings.HasPrefix(key, "csi.storage.k8s.io/") || strings.HasPrefix(key, "storage.kubernetes.io/") {
		return true
	}
	return false
}

// RejectDisallowedVolumeParameters rejects user-supplied keys such as mnt-path on PVs
// and in node volume/publish context before mount-key generation.
func RejectDisallowedVolumeParameters(volumeContext, publishContext map[string]string, requestID string) error {
	for key := range volumeContext {
		if IsRejectedVolumeParameterKey(key) {
			klog.Errorf("RequestID: %s, Parameter %q is not allowed on StorageClass or PV", requestID, key)
			return status.Errorf(codes.InvalidArgument, "Parameter %q is not allowed on StorageClass or PV", key)
		}
	}
	for key := range publishContext {
		if IsRejectedVolumeParameterKey(key) {
			klog.Errorf("RequestID: %s, Parameter %q is not allowed on StorageClass or PV", requestID, key)
			return status.Errorf(codes.InvalidArgument, "Parameter %q is not allowed on StorageClass or PV", key)
		}
	}
	return nil
}

// MergeVolumeParameters merges volumeContext and publishContext with volumeContext winning.
func MergeVolumeParameters(volumeContext, publishContext map[string]string) map[string]string {
	merged := make(map[string]string)
	for k, v := range publishContext {
		if v != "" {
			merged[k] = v
		}
	}
	for k, v := range volumeContext {
		if v != "" {
			merged[k] = v
		}
	}
	return merged
}

// CollectPassthroughParams returns StorageClass/PV keys that should be forwarded to
// curvine-fuse as --key value pairs. The returned map is unordered; deterministic argv
// ordering is applied in appendPassthroughArgs when building CLI flags.
func CollectPassthroughParams(volumeContext, publishContext map[string]string) map[string]string {
	merged := MergeVolumeParameters(volumeContext, publishContext)
	out := make(map[string]string)
	for key, value := range merged {
		if value == "" {
			continue
		}
		if IsReservedVolumeParameterKey(key) || IsRejectedVolumeParameterKey(key) || IsDeniedVolumeParameterKey(key) {
			continue
		}
		out[key] = value
	}
	return out
}

// ComputeFuseMntPath returns the host path where curvine-fuse mounts for a mount key.
func ComputeFuseMntPath(mountKey string) string {
	return fmt.Sprintf("%s/%s/fuse-mount", fuseKubeletPluginBase, mountKey)
}

// FuseExecArgsInput describes inputs for building a curvine-fuse argv slice.
type FuseExecArgsInput struct {
	// Subcommand is optional (e.g. "validate-config"). Empty means default mount.
	Subcommand  string
	ConfPath    string
	MasterAddrs string
	FSPath      string
	MountKey    string
	// MntPath, when set, is used as --mnt-path instead of ComputeFuseMntPath(MountKey).
	MntPath     string
	Passthrough map[string]string
}

// BuildFuseExecArgs builds argv for curvine-fuse mount or validate-config.
func BuildFuseExecArgs(in FuseExecArgsInput) []string {
	args := make([]string, 0, 8+len(in.Passthrough)*2)
	if in.Subcommand != "" {
		args = append(args, in.Subcommand)
	}
	if in.ConfPath != "" {
		args = append(args, "--conf", in.ConfPath)
	}
	if in.MasterAddrs != "" {
		args = append(args, "--master-addrs", in.MasterAddrs)
	}
	if in.FSPath != "" {
		args = append(args, "--fs-path", in.FSPath)
	}
	switch {
	case in.MntPath != "":
		args = append(args, "--mnt-path", in.MntPath)
	case in.MountKey != "":
		args = append(args, "--mnt-path", ComputeFuseMntPath(in.MountKey))
	}
	args = appendPassthroughArgs(args, in.Passthrough)
	return args
}

func appendPassthroughArgs(args []string, passthrough map[string]string) []string {
	if len(passthrough) == 0 {
		return args
	}
	keys := make([]string, 0, len(passthrough))
	for key := range passthrough {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		flag := passthroughCLIKey(key)
		args = append(args, flag, passthrough[key])
	}
	return args
}

// passthroughCLIKey maps a volume parameter key to a curvine-fuse CLI flag name.
func passthroughCLIKey(key string) string {
	return "--" + key
}
