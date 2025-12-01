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
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

// PVClient wraps Kubernetes client for PersistentVolume operations
type PVClient struct {
	clientset *kubernetes.Clientset
}

// NewPVClient creates a new PV client
func NewPVClient() (*PVClient, error) {
	var config *rest.Config
	var err error

	// Try in-cluster config first
	config, err = rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig file
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create k8s config: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s clientset: %v", err)
	}

	client := &PVClient{
		clientset: clientset,
	}

	klog.V(5).Infof("Initialized PV client")
	return client, nil
}

// GetPVByVolumeHandle queries PV by volumeHandle
func (c *PVClient) GetPVByVolumeHandle(ctx context.Context, volumeHandle string) (*corev1.PersistentVolume, error) {
	pvList, err := c.clientset.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list PVs: %v", err)
	}

	for i := range pvList.Items {
		pv := &pvList.Items[i]
		if pv.Spec.CSI != nil && pv.Spec.CSI.Driver == DriverName && pv.Spec.CSI.VolumeHandle == volumeHandle {
			klog.V(5).Infof("Found PV %s with volumeHandle: %s", pv.Name, volumeHandle)
			return pv, nil
		}
	}

	return nil, fmt.Errorf("PV with volumeHandle %s not found", volumeHandle)
}
