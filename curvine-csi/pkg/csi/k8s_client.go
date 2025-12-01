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
	"encoding/base64"
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

// K8sClient wraps Kubernetes client for Secret operations
type K8sClient struct {
	clientset  *kubernetes.Clientset
	namespace  string
	nodeName   string
	secretName string
}

// NewK8sClient creates a new Kubernetes client
func NewK8sClient(namespace, nodeName string) (*K8sClient, error) {
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

	secretName := fmt.Sprintf("curvine-csi-mount-%s", nodeName)

	client := &K8sClient{
		clientset:  clientset,
		namespace:  namespace,
		nodeName:   nodeName,
		secretName: secretName,
	}

	klog.Infof("Initialized K8s client: namespace=%s, nodeName=%s, secretName=%s", namespace, nodeName, secretName)
	return client, nil
}

// GetSecret retrieves the mount state Secret
func (c *K8sClient) GetSecret(ctx context.Context) (*corev1.Secret, error) {
	secret, err := c.clientset.CoreV1().Secrets(c.namespace).Get(ctx, c.secretName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret, nil
}

// CreateOrUpdateSecret creates or updates the mount state Secret
func (c *K8sClient) CreateOrUpdateSecret(ctx context.Context, data map[string]string) error {
	secretData := make(map[string][]byte)
	for k, v := range data {
		secretData[k] = []byte(v)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.secretName,
			Namespace: c.namespace,
			Labels: map[string]string{
				"app":                  "curvine-csi",
				"component":            "node",
				"curvine.io/node-name": c.nodeName,
			},
		},
		Data: secretData,
	}

	// Try to get existing secret
	_, err := c.clientset.CoreV1().Secrets(c.namespace).Get(ctx, c.secretName, metav1.GetOptions{})
	if err != nil {
		// Secret doesn't exist, create it
		_, err = c.clientset.CoreV1().Secrets(c.namespace).Create(ctx, secret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create secret: %v", err)
		}
		klog.V(5).Infof("Created Secret: %s/%s", c.namespace, c.secretName)
	} else {
		// Secret exists, update it
		_, err = c.clientset.CoreV1().Secrets(c.namespace).Update(ctx, secret, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update secret: %v", err)
		}
		klog.V(5).Infof("Updated Secret: %s/%s", c.namespace, c.secretName)
	}

	return nil
}

// DeleteSecret deletes the mount state Secret
func (c *K8sClient) DeleteSecret(ctx context.Context) error {
	err := c.clientset.CoreV1().Secrets(c.namespace).Delete(ctx, c.secretName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete secret: %v", err)
	}
	klog.V(5).Infof("Deleted Secret: %s/%s", c.namespace, c.secretName)
	return nil
}

// GetSecretData retrieves data from Secret
func (c *K8sClient) GetSecretData(ctx context.Context, key string) (string, error) {
	secret, err := c.GetSecret(ctx)
	if err != nil {
		return "", err
	}

	if data, ok := secret.Data[key]; ok {
		return string(data), nil
	}

	return "", fmt.Errorf("key %s not found in secret", key)
}

// UpdateSecretData updates a specific key in Secret
func (c *K8sClient) UpdateSecretData(ctx context.Context, key, value string) error {
	klog.Infof("Getting Secret %s/%s", c.namespace, c.secretName)
	secret, err := c.GetSecret(ctx)
	if err != nil {
		// Secret doesn't exist, create it with this data
		klog.Infof("Secret %s/%s not found, creating it", c.namespace, c.secretName)
		return c.CreateOrUpdateSecret(ctx, map[string]string{key: value})
	}

	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[key] = []byte(value)

	klog.Infof("Updating Secret %s/%s", c.namespace, c.secretName)
	_, err = c.clientset.CoreV1().Secrets(c.namespace).Update(ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update secret data: %v", err)
		return fmt.Errorf("failed to update secret data: %v", err)
	}
	klog.Infof("Secret %s/%s updated successfully", c.namespace, c.secretName)

	return nil
}

// DecodeBase64Data decodes base64 encoded data from Secret
func DecodeBase64Data(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

// EncodeBase64Data encodes data to base64 for Secret storage
func EncodeBase64Data(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}
