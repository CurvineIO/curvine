/*
Copyright 2024 Curvine Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package csi

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// StandaloneNamePrefix is the prefix for Standalone names
	StandaloneNamePrefix = "curvine-csi-standalone-"

	// StandaloneLabel is the label for Standalone
	StandaloneLabelApp       = "app"
	StandaloneLabelAppValue  = "curvine-standalone"
	StandaloneLabelClusterID = "curvine.io/cluster-id"
	StandaloneLabelNode      = "curvine.io/node"

	// StandaloneImage is the default image for Standalone
	// This will be overridden by getCurrentPodImage() if available
	StandaloneImage = "ghcr.io/curvineio/curvine-csi:latest"

	// StandaloneMountPath is the mount path inside Standalone
	StandaloneMountPath = "/mnt/curvine"

	// HostMountBaseDir is the base directory for host mounts
	HostMountBaseDir = "/var/lib/kubelet/plugins/curvine"

	// StateConfigMapPrefix is the prefix for state ConfigMap
	StateConfigMapPrefix = "curvine-standalone-state-"

	// StandaloneReadyTimeout is the timeout for Standalone to be ready
	StandaloneReadyTimeout = 60 * time.Second

	// StandaloneCheckInterval is the interval to check Standalone status
	StandaloneCheckInterval = 2 * time.Second

	// StandaloneTerminationGracePeriod is the graceful shutdown period for Standalone
	// This gives FUSE enough time to unmount and cleanup
	StandaloneTerminationGracePeriod = 30

	// StandalonePreStopSleepSeconds is the sleep time in preStop hook
	// This ensures in-flight I/O operations complete before unmount
	StandalonePreStopSleepSeconds = 5

	// EnvStandaloneImage is the environment variable for Standalone pod image
	EnvStandaloneImage = "STANDALONE_IMAGE"

	// EnvStandaloneServiceAccount is the environment variable for Standalone pod ServiceAccount
	EnvStandaloneServiceAccount = "STANDALONE_SERVICE_ACCOUNT"

	// EnvStandaloneCPURequest is the environment variable for Standalone CPU request
	EnvStandaloneCPURequest = "STANDALONE_CPU_REQUEST"

	// EnvStandaloneCPULimit is the environment variable for Standalone CPU limit
	EnvStandaloneCPULimit = "STANDALONE_CPU_LIMIT"

	// EnvStandaloneMemoryRequest is the environment variable for Standalone Memory request
	EnvStandaloneMemoryRequest = "STANDALONE_MEMORY_REQUEST"

	// EnvStandaloneMemoryLimit is the environment variable for Standalone Memory limit
	EnvStandaloneMemoryLimit = "STANDALONE_MEMORY_LIMIT"
)

// StandaloneOptions contains options for creating a Standalone
type StandaloneOptions struct {
	ClusterID   string // Used for logging
	MountKey    string // Used as key for pod name and mount path (master-addrs + fs-path)
	MasterAddrs string
	FSPath      string
	NodeName    string
	Namespace   string
	Image       string
}

// StandaloneStatus represents the status of a Standalone
type StandaloneStatus struct {
	Phase     corev1.PodPhase
	Ready     bool
	MountPath string
	PodName   string
}

// StandaloneInfo contains information about a Standalone
type StandaloneInfo struct {
	ClusterID string
	PodName   string
	RefCount  int
	Volumes   []string
	CreatedAt time.Time
}

// StandaloneState is the state stored in ConfigMap
type StandaloneState struct {
	Mounts map[string]*StandaloneStateEntry `json:"mounts"`
}

// StandaloneStateEntry is a single mount entry in state
type StandaloneStateEntry struct {
	PodName   string    `json:"pod-name"`
	RefCount  int       `json:"ref-count"`
	Volumes   []string  `json:"volumes"`
	CreatedAt time.Time `json:"created-at"`
}

// StandaloneMountManager manages Standalones for FUSE mounts
type StandaloneMountManager interface {
	// EnsureStandalone ensures a Standalone exists for the given cluster
	// Returns the host mount path where FUSE is mounted
	EnsureStandalone(ctx context.Context, opts *StandaloneOptions) (string, error)

	// DeleteStandalone deletes the Standalone for the given mount key on this node
	DeleteStandalone(ctx context.Context, mountKey string) error

	// GetStandaloneStatus returns the status of the Standalone
	GetStandaloneStatus(ctx context.Context, mountKey string) (*StandaloneStatus, error)

	// WaitForStandaloneReady waits for the Standalone to be ready
	WaitForStandaloneReady(ctx context.Context, mountKey string, timeout time.Duration) error

	// AddVolumeRef adds a volume reference to the Standalone
	AddVolumeRef(ctx context.Context, mountKey, volumeID string) error

	// RemoveVolumeRef removes a volume reference from the Standalone
	// Returns true if the Standalone should be deleted (ref count = 0)
	RemoveVolumeRef(ctx context.Context, mountKey, volumeID string) (bool, error)

	// RecoverState recovers state from ConfigMap on startup
	RecoverState(ctx context.Context) error

	// GetHostMountPath returns the host mount path for a mount key
	GetHostMountPath(mountKey string) string

	// FindMountKeyByVolumeID finds the mountKey that contains the given volumeID
	FindMountKeyByVolumeID(volumeID string) (string, bool)

	// GetState returns a copy of the current state (for garbage collection)
	GetState() *StandaloneState
}

// standaloneManagerImpl implements StandaloneMountManager
type standaloneMountManagerImpl struct {
	client             kubernetes.Interface
	namespace          string
	nodeName           string
	image              string
	serviceAccountName string

	mu    sync.RWMutex
	state *StandaloneState
}

// getCurrentPodImage attempts to get the image of the current CSI node pod
// Returns empty string if unable to determine (fallback to default)
func getCurrentPodImage(client kubernetes.Interface, namespace string) string {
	// Get pod name from HOSTNAME environment variable (set by Kubernetes)
	podName := os.Getenv("HOSTNAME")
	if podName == "" {
		klog.V(5).Infof("HOSTNAME not set, cannot determine current pod image")
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query current pod to get its image
	pod, err := client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		klog.V(5).Infof("Failed to get current pod %s/%s: %v", namespace, podName, err)
		return ""
	}

	// Find the csi-plugin container image
	for _, container := range pod.Spec.Containers {
		if container.Name == "csi-plugin" {
			klog.Infof("Found current pod image: %s (from pod %s/%s)", container.Image, namespace, podName)
			return container.Image
		}
	}
	klog.Warningf("No csi-plugin container found in pod %s/%s, using default image", namespace, podName)
	return ""
}

// NewStandaloneMountManager creates a new StandaloneMountManager
func NewStandaloneManager(client kubernetes.Interface, namespace, nodeName, image, serviceAccountName string) StandaloneMountManager {
	if image == "" {
		// Try to get current pod image first
		currentImage := getCurrentPodImage(client, namespace)
		if currentImage != "" {
			image = currentImage
			klog.Infof("Using current pod image as Standalone image: %s", image)
		} else {
			// Fallback to default
			image = StandaloneImage
			klog.Infof("Using default Standalone image: %s", image)
		}
	}
	return &standaloneMountManagerImpl{
		client:             client,
		namespace:          namespace,
		nodeName:           nodeName,
		image:              image,
		serviceAccountName: serviceAccountName,
		state: &StandaloneState{
			Mounts: make(map[string]*StandaloneStateEntry),
		},
	}
}

// GetHostMountPath returns the host mount path for a mount key
func (m *standaloneMountManagerImpl) GetHostMountPath(mountKey string) string {
	return filepath.Join(HostMountBaseDir, mountKey, "fuse-mount")
}

// getStandaloneName returns the name of the Standalone for a mount key
// mountKey is generated using UUID v5 algorithm, which always produces 32 hex characters (128 bits)
// We use first 8 characters (32 bits) for Pod name to keep it short and follow Kubernetes naming conventions
func (m *standaloneMountManagerImpl) getStandaloneName(mountKey string) string {
	// UUID v5 mountKey is always 32 characters, use first 8 chars for Pod name
	mountKeyShort := mountKey
	if len(mountKey) < 8 {
		// If mountKey is too short (shouldn't happen with UUID v5), pad with hash
		hash := hashString(mountKey)
		mountKeyShort = fmt.Sprintf("%08x", hash)
	} else {
		// Use first 8 characters (32 bits) - sufficient for uniqueness per node
		mountKeyShort = mountKey[:8]
	}
	
	// Generate nodeHash (uint32 hash produces 8 hex chars, use first 5 for shorter Pod name)
	// Kubernetes typically uses 5-char suffixes for Pod names (e.g., ReplicaSet pods)
	nodeHashFull := fmt.Sprintf("%08x", hashString(m.nodeName))
	nodeHash := nodeHashFull[:5]
	
	// Pod name format: curvine-csi-standalone-{mountKey8chars}-{nodeHash5chars}
	// Total length: 23 + 8 + 1 + 5 = 37 characters (follows Kubernetes naming best practices)
	// Combined uniqueness: 2^32 (mountKey) * 2^20 (nodeHash) = 2^52 per node (still very high)
	return fmt.Sprintf("%s%s-%s", StandaloneNamePrefix, mountKeyShort, nodeHash)
}

// hashString returns a simple hash of a string
func hashString(s string) uint32 {
	h := uint32(0)
	for _, c := range s {
		h = h*31 + uint32(c)
	}
	return h
}

// EnsureStandalone ensures a Standalone exists for the given cluster
// Implements self-healing: if pod is unhealthy, it will be deleted and recreated
func (m *standaloneMountManagerImpl) EnsureStandalone(ctx context.Context, opts *StandaloneOptions) (string, error) {
	podName := m.getStandaloneName(opts.MountKey)
	hostMountPath := m.GetHostMountPath(opts.MountKey)

	klog.Infof("EnsureStandalone: clusterID=%s, mountKey=%s, fsPath=%s, podName=%s, hostMountPath=%s", opts.ClusterID, opts.MountKey, opts.FSPath, podName, hostMountPath)

	// Check if Standalone already exists
	existingPod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err == nil {
		// Pod exists, check its health status
		klog.Infof("Standalone %s already exists, phase=%s", podName, existingPod.Status.Phase)

		// Self-healing: check if pod needs to be recreated
		needRecreate, reason := m.needsRecreation(existingPod)
		if needRecreate {
			klog.Warningf("Standalone %s needs recreation: %s", podName, reason)
			if err := m.deleteStandaloneAndWait(ctx, podName); err != nil {
				klog.Errorf("Failed to delete unhealthy Standalone %s: %v", podName, err)
				return "", fmt.Errorf("failed to delete unhealthy Standalone: %v", err)
			}
			// Fall through to create new pod
		} else if existingPod.Status.Phase == corev1.PodRunning && isPodReady(existingPod) {
			return hostMountPath, nil
		} else {
			// Pod exists but not ready, wait for it
			if err := m.WaitForStandaloneReady(ctx, opts.MountKey, StandaloneReadyTimeout); err != nil {
				// If waiting times out, try self-healing
				klog.Warningf("Standalone %s not ready after waiting, attempting self-healing", podName)
				if delErr := m.deleteStandaloneAndWait(ctx, podName); delErr != nil {
					return "", fmt.Errorf("Standalone %s not ready and failed to delete: %v", podName, delErr)
				}
				// Fall through to create new pod
			} else {
				return hostMountPath, nil
			}
		}
	} else if !errors.IsNotFound(err) {
		return "", fmt.Errorf("failed to get Standalone %s: %v", podName, err)
	}

	// Ensure mount path is clean and ready (handles stale mount points)
	if err := m.ensureCleanMountPath(ctx, hostMountPath, podName); err != nil {
		return "", fmt.Errorf("failed to ensure clean mount path %s: %v", hostMountPath, err)
	}

	// Create host mount directory if it doesn't exist
	if err := os.MkdirAll(hostMountPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create host mount directory %s: %v", hostMountPath, err)
	}

	// Create Standalone
	pod := m.buildStandalone(opts, podName, hostMountPath)
	klog.Infof("Creating Standalone %s for mountKey %s (clusterID: %s, fsPath: %s)", podName, opts.MountKey, opts.ClusterID, opts.FSPath)

	_, err = m.client.CoreV1().Pods(m.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			klog.Infof("Standalone %s already exists (race condition)", podName)
		} else {
			return "", fmt.Errorf("failed to create Standalone %s: %v", podName, err)
		}
	}

	// Wait for pod to be ready
	if err := m.WaitForStandaloneReady(ctx, opts.MountKey, StandaloneReadyTimeout); err != nil {
		return "", fmt.Errorf("Standalone %s not ready after creation: %v", podName, err)
	}

	// Update state (use mountKey as key)
	m.mu.Lock()
	m.state.Mounts[opts.MountKey] = &StandaloneStateEntry{
		PodName:   podName,
		RefCount:  0,
		Volumes:   []string{},
		CreatedAt: time.Now(),
	}
	m.mu.Unlock()

	if err := m.saveState(ctx); err != nil {
		klog.Warningf("Failed to save state after creating Standalone: %v", err)
	}

	klog.Infof("Standalone %s created and ready, hostMountPath=%s", podName, hostMountPath)
	return hostMountPath, nil
}

// needsRecreation checks if a Standalone needs to be deleted and recreated
// Returns (needRecreate, reason)
func (m *standaloneMountManagerImpl) needsRecreation(pod *corev1.Pod) (bool, string) {
	// Failed pods need recreation
	if pod.Status.Phase == corev1.PodFailed {
		return true, fmt.Sprintf("pod is in Failed phase: %s", pod.Status.Reason)
	}

	// Pods being deleted need recreation
	if pod.DeletionTimestamp != nil {
		return true, "pod is being deleted"
	}

	// Check container statuses for crash loops or terminated containers
	for _, cs := range pod.Status.ContainerStatuses {
		// CrashLoopBackOff detection
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			return true, fmt.Sprintf("container %s is in CrashLoopBackOff", cs.Name)
		}

		// Too many restarts (threshold: 5)
		if cs.RestartCount > 5 {
			return true, fmt.Sprintf("container %s has too many restarts (%d)", cs.Name, cs.RestartCount)
		}

		// Terminated with error
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return true, fmt.Sprintf("container %s terminated with exit code %d", cs.Name, cs.State.Terminated.ExitCode)
		}
	}

	return false, ""
}

// deleteStandaloneAndWait deletes the Standalone and waits for it to be fully deleted
func (m *standaloneMountManagerImpl) deleteStandaloneAndWait(ctx context.Context, podName string) error {
	klog.Infof("Deleting Standalone %s and waiting for deletion", podName)

	// Use grace period 0 for immediate deletion in self-healing scenarios
	gracePeriod := int64(0)
	deletePolicy := metav1.DeletePropagationForeground
	err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
		PropagationPolicy:  &deletePolicy,
	})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Standalone %s: %v", podName, err)
	}

	// Wait for pod to be fully deleted
	return wait.PollUntilContextTimeout(ctx, time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, nil
	})
}

// buildStandalone builds a Standalone spec with graceful shutdown and health checks
func (m *standaloneMountManagerImpl) buildStandalone(opts *StandaloneOptions, podName, hostMountPath string) *corev1.Pod {
	privileged := true
	hostPathType := corev1.HostPathDirectoryOrCreate
	charDeviceType := corev1.HostPathCharDev
	terminationGracePeriod := int64(StandaloneTerminationGracePeriod)

	image := opts.Image
	if image == "" {
		image = m.image
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: m.namespace,
			Labels: map[string]string{
				StandaloneLabelApp:       StandaloneLabelAppValue,
				StandaloneLabelClusterID: opts.ClusterID,
				StandaloneLabelNode:      m.nodeName,
			},
		},
		Spec: corev1.PodSpec{
			HostNetwork:                   true,
			HostPID:                       true,
			DNSPolicy:                     corev1.DNSClusterFirstWithHostNet, // Use cluster DNS first, then host DNS
			NodeName:                      m.nodeName,
			RestartPolicy:                 corev1.RestartPolicyAlways,
			ServiceAccountName:            m.serviceAccountName,
			TerminationGracePeriodSeconds: &terminationGracePeriod,
			Containers: []corev1.Container{
				{
					Name:            "curvine-fuse",
					Image:           image,
					ImagePullPolicy: corev1.PullNever,
					Command: []string{
						"/opt/curvine/curvine-fuse",
					},
					Args: []string{
						"--master-addrs", opts.MasterAddrs,
						"--fs-path", opts.FSPath,
						"--mnt-path", StandaloneMountPath,
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
					Resources: buildResourceRequirements(),
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:             "fuse-mount",
							MountPath:        StandaloneMountPath,
							MountPropagation: mountPropagationBidirectionalPtr(),
						},
						{
							Name:      "fuse-device",
							MountPath: "/dev/fuse",
						},
					},
					// Lifecycle hooks for graceful shutdown
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								// Graceful unmount: wait for in-flight I/O, then unmount FUSE
								Command: []string{
									"/bin/sh", "-c",
									fmt.Sprintf(
										"echo 'PreStop: starting graceful shutdown'; "+
											"sleep %d; "+
											"echo 'PreStop: unmounting FUSE at %s'; "+
											"fusermount -u %s || umount -l %s || true; "+
											"echo 'PreStop: unmount completed'",
										StandalonePreStopSleepSeconds,
										StandaloneMountPath,
										StandaloneMountPath,
										StandaloneMountPath,
									),
								},
							},
						},
					},
					// StartupProbe: allows longer startup time for FUSE initialization
					StartupProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"mountpoint", "-q", StandaloneMountPath},
							},
						},
						InitialDelaySeconds: 1,
						PeriodSeconds:       2,
						TimeoutSeconds:      3,
						FailureThreshold:    30, // Allow up to 60s for startup
						SuccessThreshold:    1,
					},
					// ReadinessProbe: checks if FUSE mount is ready for traffic
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"mountpoint", "-q", StandaloneMountPath},
							},
						},
						InitialDelaySeconds: 0,
						PeriodSeconds:       5,
						TimeoutSeconds:      3,
						FailureThreshold:    3,
						SuccessThreshold:    1,
					},
					// LivenessProbe: restarts container if FUSE mount becomes unhealthy
					LivenessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{
								Command: []string{"mountpoint", "-q", StandaloneMountPath},
							},
						},
						InitialDelaySeconds: 0, // StartupProbe handles initial delay
						PeriodSeconds:       10,
						TimeoutSeconds:      3,
						FailureThreshold:    3,
						SuccessThreshold:    1,
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "fuse-mount",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: hostMountPath,
							Type: &hostPathType,
						},
					},
				},
				{
					Name: "fuse-device",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/dev/fuse",
							Type: &charDeviceType,
						},
					},
				},
			},
			Tolerations: []corev1.Toleration{
				{
					Operator: corev1.TolerationOpExists,
				},
			},
		},
	}
}

// buildResourceRequirements builds resource requirements from environment variables
func buildResourceRequirements() corev1.ResourceRequirements {
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{},
		Limits:   corev1.ResourceList{},
	}

	// Parse CPU request
	if cpuRequest := os.Getenv(EnvStandaloneCPURequest); cpuRequest != "" {
		if quantity, err := resource.ParseQuantity(cpuRequest); err == nil {
			resources.Requests[corev1.ResourceCPU] = quantity
		} else {
			klog.Warningf("Failed to parse CPU request %s: %v", cpuRequest, err)
		}
	}

	// Parse CPU limit
	if cpuLimit := os.Getenv(EnvStandaloneCPULimit); cpuLimit != "" {
		if quantity, err := resource.ParseQuantity(cpuLimit); err == nil {
			resources.Limits[corev1.ResourceCPU] = quantity
		} else {
			klog.Warningf("Failed to parse CPU limit %s: %v", cpuLimit, err)
		}
	}

	// Parse Memory request
	if memRequest := os.Getenv(EnvStandaloneMemoryRequest); memRequest != "" {
		if quantity, err := resource.ParseQuantity(memRequest); err == nil {
			resources.Requests[corev1.ResourceMemory] = quantity
		} else {
			klog.Warningf("Failed to parse Memory request %s: %v", memRequest, err)
		}
	}

	// Parse Memory limit
	if memLimit := os.Getenv(EnvStandaloneMemoryLimit); memLimit != "" {
		if quantity, err := resource.ParseQuantity(memLimit); err == nil {
			resources.Limits[corev1.ResourceMemory] = quantity
		} else {
			klog.Warningf("Failed to parse Memory limit %s: %v", memLimit, err)
		}
	}

	// Log the configured resources
	if len(resources.Requests) > 0 || len(resources.Limits) > 0 {
		klog.V(4).Infof("Standalone Pod resources configured: requests=%v, limits=%v",
			resources.Requests, resources.Limits)
	}

	return resources
}

// mountPropagationBidirectionalPtr returns a pointer to Bidirectional mount propagation
func mountPropagationBidirectionalPtr() *corev1.MountPropagationMode {
	mode := corev1.MountPropagationBidirectional
	return &mode
}

// DeleteStandalone deletes the Standalone for the given mount key on this node
func (m *standaloneMountManagerImpl) DeleteStandalone(ctx context.Context, mountKey string) error {
	podName := m.getStandaloneName(mountKey)
	klog.Infof("Deleting Standalone %s for mountKey %s", podName, mountKey)

	err := m.client.CoreV1().Pods(m.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Standalone %s: %v", podName, err)
	}

	// Update state
	m.mu.Lock()
	delete(m.state.Mounts, mountKey)
	m.mu.Unlock()

	if err := m.saveState(ctx); err != nil {
		klog.Warningf("Failed to save state after deleting Standalone: %v", err)
	}

	klog.Infof("Standalone %s deleted", podName)
	return nil
}

// GetStandaloneStatus returns the status of the Standalone
func (m *standaloneMountManagerImpl) GetStandaloneStatus(ctx context.Context, mountKey string) (*StandaloneStatus, error) {
	podName := m.getStandaloneName(mountKey)

	pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get Standalone %s: %v", podName, err)
	}

	return &StandaloneStatus{
		Phase:     pod.Status.Phase,
		Ready:     isPodReady(pod),
		MountPath: m.GetHostMountPath(mountKey),
		PodName:   podName,
	}, nil
}

// WaitForStandaloneReady waits for the Standalone to be ready
func (m *standaloneMountManagerImpl) WaitForStandaloneReady(ctx context.Context, mountKey string, timeout time.Duration) error {
	podName := m.getStandaloneName(mountKey)

	return wait.PollUntilContextTimeout(ctx, StandaloneCheckInterval, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil // Keep waiting
			}
			return false, err
		}

		if pod.Status.Phase == corev1.PodFailed {
			return false, fmt.Errorf("Standalone %s failed", podName)
		}

		if isPodReady(pod) {
			return true, nil
		}

		klog.V(4).Infof("Waiting for Standalone %s to be ready, phase=%s", podName, pod.Status.Phase)
		return false, nil
	})
}

// isPodReady checks if a pod is ready
func isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// AddVolumeRef adds a volume reference to the Standalone
func (m *standaloneMountManagerImpl) AddVolumeRef(ctx context.Context, mountKey, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.state.Mounts[mountKey]
	if !ok {
		// Create new entry if not exists
		entry = &StandaloneStateEntry{
			PodName:   m.getStandaloneName(mountKey),
			RefCount:  0,
			Volumes:   []string{},
			CreatedAt: time.Now(),
		}
		m.state.Mounts[mountKey] = entry
	}

	// Check if volume already referenced
	for _, v := range entry.Volumes {
		if v == volumeID {
			klog.V(4).Infof("Volume %s already referenced for mountKey %s", volumeID, mountKey)
			return nil
		}
	}

	entry.Volumes = append(entry.Volumes, volumeID)
	entry.RefCount = len(entry.Volumes)

	klog.Infof("Added volume ref %s for mountKey %s, refCount=%d", volumeID, mountKey, entry.RefCount)

	return m.saveStateLocked(ctx)
}

// RemoveVolumeRef removes a volume reference from the Standalone
func (m *standaloneMountManagerImpl) RemoveVolumeRef(ctx context.Context, mountKey, volumeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.state.Mounts[mountKey]
	if !ok {
		klog.Warningf("No state found for mountKey %s when removing volume %s", mountKey, volumeID)
		return false, nil
	}

	// Remove volume from list
	newVolumes := make([]string, 0, len(entry.Volumes))
	for _, v := range entry.Volumes {
		if v != volumeID {
			newVolumes = append(newVolumes, v)
		}
	}
	entry.Volumes = newVolumes
	entry.RefCount = len(entry.Volumes)

	klog.Infof("Removed volume ref %s for mountKey %s, refCount=%d", volumeID, mountKey, entry.RefCount)

	if err := m.saveStateLocked(ctx); err != nil {
		return false, err
	}

	return entry.RefCount == 0, nil
}

// RecoverState recovers state from ConfigMap on startup
// Implements self-healing: verifies Standalones health and triggers recovery if needed
func (m *standaloneMountManagerImpl) RecoverState(ctx context.Context) error {
	klog.Info("Recovering Standalone state from ConfigMap")

	configMapName := StateConfigMapPrefix + m.nodeName
	cm, err := m.client.CoreV1().ConfigMaps(m.namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Info("No existing state ConfigMap found")
			return nil
		}
		return fmt.Errorf("failed to get state ConfigMap: %v", err)
	}

	stateJSON, ok := cm.Data["state.json"]
	if !ok {
		klog.Info("No state data in ConfigMap")
		return nil
	}

	var state StandaloneState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %v", err)
	}

	m.mu.Lock()
	m.state = &state
	m.mu.Unlock()

	klog.Infof("Recovered %d mount entries from state", len(state.Mounts))

	// Verify Standalones exist and are healthy
	for mountKey, entry := range state.Mounts {
		podName := m.getStandaloneName(mountKey)
		pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				klog.Warningf("Standalone %s for mountKey %s not found, but has %d volume refs",
					entry.PodName, mountKey, entry.RefCount)
				// Mark for potential recreation on next EnsureStandalone call
				continue
			}
			klog.Warningf("Failed to get Standalone %s: %v", entry.PodName, err)
			continue
		}

		// Check if Standalone needs recreation (self-healing)
		needRecreate, reason := m.needsRecreation(pod)
		if needRecreate {
			klog.Warningf("Standalone %s needs recreation during recovery: %s", podName, reason)
			// Delete unhealthy pod, it will be recreated on next EnsureStandalone call
			if delErr := m.deleteStandaloneAndWait(ctx, podName); delErr != nil {
				klog.Errorf("Failed to delete unhealthy Standalone %s during recovery: %v", podName, delErr)
			} else {
				klog.Infof("Deleted unhealthy Standalone %s, will be recreated on next volume mount", podName)
			}
		} else {
			klog.Infof("Standalone %s exists, phase=%s, ready=%v, refCount=%d",
				entry.PodName, pod.Status.Phase, isPodReady(pod), entry.RefCount)
		}
	}

	return nil
}

// saveState saves state to ConfigMap
func (m *standaloneMountManagerImpl) saveState(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.saveStateLocked(ctx)
}

// saveStateLocked saves state to ConfigMap (must hold lock)
func (m *standaloneMountManagerImpl) saveStateLocked(ctx context.Context) error {
	stateJSON, err := json.Marshal(m.state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}

	configMapName := StateConfigMapPrefix + m.nodeName
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: m.namespace,
			Labels: map[string]string{
				StandaloneLabelApp:  StandaloneLabelAppValue,
				StandaloneLabelNode: m.nodeName,
			},
		},
		Data: map[string]string{
			"state.json": string(stateJSON),
		},
	}

	_, err = m.client.CoreV1().ConfigMaps(m.namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = m.client.CoreV1().ConfigMaps(m.namespace).Create(ctx, cm, metav1.CreateOptions{})
		}
	}

	if err != nil {
		return fmt.Errorf("failed to save state ConfigMap: %v", err)
	}

	return nil
}

// Helper to extract cluster ID from volume ID
// Volume ID format: clusterID@fsPath@pvcName
func ExtractClusterIDFromVolumeID(volumeID string) string {
	// VolumeID format: clusterID@volumeName
	// For static PVs, volumeID is just the volume name without "@"
	if !strings.Contains(volumeID, "@") {
		return "" // No clusterID in volumeID, caller should use FindMountKeyByVolumeID
	}
	parts := strings.Split(volumeID, "@")
	if len(parts) >= 2 {
		return parts[0]
	}
	return ""
}

// FindMountKeyByVolumeID finds the mountKey that contains the given volumeID
// This is needed for static PVs where volumeID doesn't contain mountKey information
func (m *standaloneMountManagerImpl) FindMountKeyByVolumeID(volumeID string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for mountKey, entry := range m.state.Mounts {
		for _, v := range entry.Volumes {
			if v == volumeID {
				klog.V(4).Infof("Found mountKey %s for volumeID %s", mountKey, volumeID)
				return mountKey, true
			}
		}
	}
	klog.V(4).Infof("No mountKey found for volumeID %s", volumeID)
	return "", false
}

// GetState returns a copy of the current state for garbage collection
func (m *standaloneMountManagerImpl) GetState() *StandaloneState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a deep copy of the state
	stateCopy := &StandaloneState{
		Mounts: make(map[string]*StandaloneStateEntry),
	}

	for mountKey, entry := range m.state.Mounts {
		volumesCopy := make([]string, len(entry.Volumes))
		copy(volumesCopy, entry.Volumes)

		stateCopy.Mounts[mountKey] = &StandaloneStateEntry{
			PodName:   entry.PodName,
			RefCount:  entry.RefCount,
			Volumes:   volumesCopy,
			CreatedAt: entry.CreatedAt,
		}
	}

	return stateCopy
}

// isMountPoint checks if a path is a mount point
// Uses /proc/mounts for reliable detection without requiring special permissions
func isMountPoint(path string) (bool, error) {
	// First check if path exists
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil // Path doesn't exist, not a mount point
		}
		return false, err // Other stat error
	}
	
	// Read /proc/mounts to check if path is mounted
	// This is more reliable than mountpoint command in containers
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false, fmt.Errorf("failed to read /proc/mounts: %v", err)
	}
	
	// Normalize path for comparison
	cleanPath := filepath.Clean(path)
	
	// Check each mount point in /proc/mounts
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		mountPoint := fields[1]
		if mountPoint == cleanPath {
			return true, nil
		}
	}
	
	return false, nil // Not found in /proc/mounts, not a mount point
}

// cleanupStaleMountPoint safely cleans up a stale mount point
// Returns error if cleanup fails - caller should not proceed with Pod creation
func (m *standaloneMountManagerImpl) cleanupStaleMountPoint(hostMountPath string) error {
	klog.Warningf("Detected stale mount point at %s, attempting cleanup", hostMountPath)
	
	// Try lazy unmount first (handles broken/stale mounts)
	umountCmd := exec.Command("umount", "-l", hostMountPath)
	if output, err := umountCmd.CombinedOutput(); err != nil {
		// Check if already unmounted
		outputStr := string(output)
		if !strings.Contains(outputStr, "not mounted") && !strings.Contains(outputStr, "not found") {
			return fmt.Errorf("failed to unmount stale mount point %s: %v, output: %s", 
				hostMountPath, err, outputStr)
		}
		klog.Infof("Path %s already unmounted or not a mount point", hostMountPath)
	} else {
		klog.Infof("Successfully unmounted stale mount point: %s", hostMountPath)
	}
	
	// Remove the directory completely
	if err := os.RemoveAll(hostMountPath); err != nil {
		return fmt.Errorf("failed to remove directory %s after unmount: %v", hostMountPath, err)
	}
	
	klog.Infof("Successfully cleaned up stale mount point: %s", hostMountPath)
	return nil
}

// ensureCleanMountPath ensures the mount path is clean and ready to use
// It detects and cleans up stale mount points left by previous Standalone pods
// Returns error if path is unsafe or cleanup fails
func (m *standaloneMountManagerImpl) ensureCleanMountPath(ctx context.Context, hostMountPath, podName string) error {
	// Check directory status
	_, statErr := os.Stat(hostMountPath)
	
	// Case 1: Directory doesn't exist - this is normal, will be created later
	if os.IsNotExist(statErr) {
		klog.V(4).Infof("Mount path %s does not exist, will create", hostMountPath)
		return nil
	}
	
	// Case 2: Stat failed with error other than "not exist" 
	// This usually indicates a stale mount point (e.g., "transport endpoint is not connected")
	if statErr != nil {
		klog.Warningf("Stat failed for %s: %v - likely a stale mount point", hostMountPath, statErr)
		
		// Double-check: verify the corresponding Pod doesn't exist
		if _, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{}); err == nil {
			// Pod exists! This mount might be valid, don't clean it
			return fmt.Errorf("mount path %s has errors but Pod %s exists - manual intervention required", 
				hostMountPath, podName)
		}
		
		// Pod doesn't exist, safe to clean up stale mount
		return m.cleanupStaleMountPoint(hostMountPath)
	}
	
	// Case 3: Directory exists and Stat succeeded - check if it's a mount point
	klog.V(4).Infof("Mount path %s exists, checking if it's a mount point", hostMountPath)
	
	isMounted, err := isMountPoint(hostMountPath)
	if err != nil {
		return fmt.Errorf("failed to check if %s is a mount point: %v", hostMountPath, err)
	}
	
	if !isMounted {
		// Not a mount point, safe to reuse the directory
		klog.V(4).Infof("Mount path %s exists but is not a mount point, will reuse", hostMountPath)
		return nil
	}
	
	// It IS a mount point - verify if it's stale (Pod should not exist)
	klog.Infof("Mount path %s is a mount point, verifying if Pod %s exists", hostMountPath, podName)
	
	pod, err := m.client.CoreV1().Pods(m.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err == nil {
		// Pod exists - check if it's healthy
		if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
			// Pod is running/pending, this is likely a valid mount, don't touch it
			klog.Infof("Mount point %s is valid (Pod %s is %s), will reuse", 
				hostMountPath, podName, pod.Status.Phase)
			return nil
		}
		// Pod exists but in bad state (Failed, Unknown, etc.)
		klog.Warningf("Pod %s exists but in phase %s, treating mount as stale", podName, pod.Status.Phase)
	} else if !errors.IsNotFound(err) {
		// Error checking Pod (not NotFound)
		return fmt.Errorf("failed to check Pod %s status: %v", podName, err)
	}
	
	// Pod doesn't exist or is in bad state - mount point is stale, clean it up
	klog.Warningf("Mount point %s is stale (Pod %s not found or unhealthy), cleaning up", 
		hostMountPath, podName)
	return m.cleanupStaleMountPoint(hostMountPath)
}
