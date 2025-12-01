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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog"
)

const (
	// SecretDataKeyMounts is the key for mount state data in Secret
	SecretDataKeyMounts = "mounts.json"
)

// MountInfo represents information about a FUSE mount point
type MountInfo struct {
	MntPath     string   `json:"mnt-path"`
	FSPath      string   `json:"fs-path"`
	MasterAddrs string   `json:"master-addrs"`
	ClusterID   string   `json:"cluster-id"`
	FusePID     int      `json:"fuse-pid"`
	RefCount    int      `json:"ref-count"`
	Volumes     []string `json:"volumes"`
}

// MountState manages mount point state persisted in Kubernetes Secret
type MountState struct {
	client *K8sClient
	mounts map[string]*MountInfo // key: mnt-path@cluster-id
	mutex  sync.RWMutex
	ctx    context.Context
}

// NewMountState creates a new MountState manager
func NewMountState(client *K8sClient) (*MountState, error) {
	ms := &MountState{
		client: client,
		mounts: make(map[string]*MountInfo),
		ctx:    context.Background(),
	}

	// Load existing state from Secret
	if err := ms.loadFromSecret(); err != nil {
		klog.Warningf("Failed to load mount state from Secret: %v, starting with empty state", err)
	}

	return ms, nil
}

// getMountKey generates a key for mount map
func getMountKey(mntPath, clusterID string) string {
	return fmt.Sprintf("%s@%s", mntPath, clusterID)
}

// loadFromSecret loads mount state from Kubernetes Secret
func (ms *MountState) loadFromSecret() error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	data, err := ms.client.GetSecretData(ms.ctx, SecretDataKeyMounts)
	if err != nil {
		return err
	}

	if data == "" {
		klog.V(5).Infof("Secret data is empty, starting with empty mount state")
		return nil
	}

	var mountsData struct {
		Mounts []*MountInfo `json:"mounts"`
	}

	if err := json.Unmarshal([]byte(data), &mountsData); err != nil {
		return fmt.Errorf("failed to unmarshal mount state: %v", err)
	}

	ms.mounts = make(map[string]*MountInfo)
	for _, mount := range mountsData.Mounts {
		key := getMountKey(mount.MntPath, mount.ClusterID)
		ms.mounts[key] = mount
	}

	klog.Infof("Loaded %d mount(s) from Secret", len(ms.mounts))
	return nil
}

// saveToSecret saves mount state to Kubernetes Secret
func (ms *MountState) saveToSecret() error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	mountsList := make([]*MountInfo, 0, len(ms.mounts))
	for _, mount := range ms.mounts {
		mountsList = append(mountsList, mount)
	}

	mountsData := struct {
		Mounts []*MountInfo `json:"mounts"`
	}{
		Mounts: mountsList,
	}

	data, err := json.Marshal(mountsData)
	if err != nil {
		return fmt.Errorf("failed to marshal mount state: %v", err)
	}

	// Use context with timeout to avoid blocking indefinitely
	ctx, cancel := context.WithTimeout(ms.ctx, 5*time.Second)
	defer cancel()

	klog.Infof("Saving mount state to Secret (timeout: 5s)")
	err = ms.client.UpdateSecretData(ctx, SecretDataKeyMounts, string(data))
	if err != nil {
		klog.Errorf("Failed to save mount state to Secret: %v", err)
		return err
	}
	klog.Infof("Mount state saved to Secret successfully")
	return nil
}

// GetMount retrieves mount information
func (ms *MountState) GetMount(mntPath, clusterID string) (*MountInfo, bool) {
	klog.Infof("GetMount: acquiring RLock for mntPath: %s, clusterID: %s", mntPath, clusterID)
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()
	klog.Infof("GetMount: RLock acquired, checking mounts map")

	key := getMountKey(mntPath, clusterID)
	klog.Infof("GetMount: looking for key: %s", key)
	mount, exists := ms.mounts[key]
	if exists {
		klog.Infof("GetMount: mount found, FusePID: %d", mount.FusePID)
		// Return a copy to avoid race conditions
		mountCopy := *mount
		return &mountCopy, true
	}
	klog.Infof("GetMount: mount not found for key: %s", key)
	return nil, false
}

// GetMountByClusterID finds mount information by cluster-id (returns first match)
func (ms *MountState) GetMountByClusterID(clusterID string) (*MountInfo, bool) {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	for _, mount := range ms.mounts {
		if mount.ClusterID == clusterID {
			// Return a copy to avoid race conditions
			mountCopy := *mount
			return &mountCopy, true
		}
	}
	return nil, false
}

// GetAllMounts returns all mount information
func (ms *MountState) GetAllMounts() []*MountInfo {
	ms.mutex.RLock()
	defer ms.mutex.RUnlock()

	mounts := make([]*MountInfo, 0, len(ms.mounts))
	for _, mount := range ms.mounts {
		// Return a copy to avoid race conditions
		mountCopy := *mount
		mounts = append(mounts, &mountCopy)
	}
	return mounts
}

// AddMount adds or updates a mount point
func (ms *MountState) AddMount(mount *MountInfo) error {
	ms.mutex.Lock()
	key := getMountKey(mount.MntPath, mount.ClusterID)
	ms.mounts[key] = mount
	ms.mutex.Unlock()

	// Save to secret without holding the lock to avoid blocking
	return ms.saveToSecret()
}

// RemoveMount removes a mount point
func (ms *MountState) RemoveMount(mntPath, clusterID string) error {
	ms.mutex.Lock()
	key := getMountKey(mntPath, clusterID)
	delete(ms.mounts, key)
	ms.mutex.Unlock()

	// Save to secret without holding the lock to avoid blocking
	return ms.saveToSecret()
}

// IncrementRefCount increments reference count for a mount
func (ms *MountState) IncrementRefCount(mntPath, clusterID string) error {
	ms.mutex.Lock()
	key := getMountKey(mntPath, clusterID)
	mount, exists := ms.mounts[key]
	if !exists {
		ms.mutex.Unlock()
		return fmt.Errorf("mount not found: %s", key)
	}

	mount.RefCount++
	ms.mutex.Unlock()

	// Save to secret without holding the lock to avoid blocking
	return ms.saveToSecret()
}

// DecrementRefCount decrements reference count for a mount
func (ms *MountState) DecrementRefCount(mntPath, clusterID string) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	key := getMountKey(mntPath, clusterID)
	mount, exists := ms.mounts[key]
	if !exists {
		return fmt.Errorf("mount not found: %s", key)
	}

	if mount.RefCount > 0 {
		mount.RefCount--
	}
	return ms.saveToSecret()
}

// AddVolume adds a volume to a mount's volume list
func (ms *MountState) AddVolume(mntPath, clusterID, volumeID string) error {
	ms.mutex.Lock()
	key := getMountKey(mntPath, clusterID)
	mount, exists := ms.mounts[key]
	if !exists {
		ms.mutex.Unlock()
		return fmt.Errorf("mount not found: %s", key)
	}

	// Check if volume already exists
	for _, v := range mount.Volumes {
		if v == volumeID {
			ms.mutex.Unlock()
			return nil // Already exists
		}
	}

	mount.Volumes = append(mount.Volumes, volumeID)
	mount.RefCount++
	ms.mutex.Unlock()

	// Save to secret without holding the lock to avoid blocking
	return ms.saveToSecret()
}

// RemoveVolume removes a volume from a mount's volume list
func (ms *MountState) RemoveVolume(mntPath, clusterID, volumeID string) error {
	ms.mutex.Lock()
	key := getMountKey(mntPath, clusterID)
	mount, exists := ms.mounts[key]
	if !exists {
		ms.mutex.Unlock()
		return fmt.Errorf("mount not found: %s", key)
	}

	// Remove volume from list
	newVolumes := make([]string, 0, len(mount.Volumes))
	for _, v := range mount.Volumes {
		if v != volumeID {
			newVolumes = append(newVolumes, v)
		}
	}
	mount.Volumes = newVolumes

	if mount.RefCount > 0 {
		mount.RefCount--
	}
	ms.mutex.Unlock()

	// Save to secret without holding the lock to avoid blocking
	return ms.saveToSecret()
}

// UpdateFusePID updates FUSE process ID for a mount
func (ms *MountState) UpdateFusePID(mntPath, clusterID string, pid int) error {
	ms.mutex.Lock()
	defer ms.mutex.Unlock()

	key := getMountKey(mntPath, clusterID)
	mount, exists := ms.mounts[key]
	if !exists {
		return fmt.Errorf("mount not found: %s", key)
	}

	mount.FusePID = pid
	return ms.saveToSecret()
}
