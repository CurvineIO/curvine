#!/bin/bash
set -e

NAMESPACE="curvine-system"
TEST_POD="curvine-test-pod"
TEST_PVC="curvine-pvc"

echo "=== CSI Test Suite ==="

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    kubectl delete pod $TEST_POD -n $NAMESPACE --ignore-not-found=true
    kubectl delete pvc $TEST_PVC -n $NAMESPACE --ignore-not-found=true
}

trap cleanup EXIT

# Test 1: Create PVC
echo "Test 1: Creating PVC..."
kubectl apply -f curvine-csi/examples/pvc-curvine.yaml
kubectl wait --for=condition=Bound pvc/$TEST_PVC -n $NAMESPACE --timeout=60s
echo "✓ PVC created and bound"

# Test 2: Create Pod
echo "Test 2: Creating test pod..."
kubectl apply -f curvine-csi/examples/pod-curvine.yaml
kubectl wait --for=condition=Ready pod/$TEST_POD -n $NAMESPACE --timeout=60s
echo "✓ Pod created and ready"

# Test 3: Write data
echo "Test 3: Writing test data..."
kubectl exec $TEST_POD -n $NAMESPACE -- \
  sh -c 'echo "CSI Test: $(date)" > /usr/share/nginx/html/test.txt'
echo "✓ Data written"

# Test 4: Read data
echo "Test 4: Reading test data..."
DATA=$(kubectl exec $TEST_POD -n $NAMESPACE -- cat /usr/share/nginx/html/test.txt)
echo "✓ Data read: $DATA"

# Test 5: Restart CSI Node Pod (MountPod mode)
if kubectl get pods -n $NAMESPACE -l app=curvine-csi-node --no-headers | grep -q Running; then
    echo "Test 5: Restarting CSI Node Pod..."
    kubectl delete pod -l app=curvine-csi-node -n $NAMESPACE
    kubectl wait --for=condition=Ready pod -l app=curvine-csi-node -n $NAMESPACE --timeout=60s
    
    # Verify data still accessible
    DATA_AFTER=$(kubectl exec $TEST_POD -n $NAMESPACE -- cat /usr/share/nginx/html/test.txt)
    if [ "$DATA" == "$DATA_AFTER" ]; then
        echo "✓ Data persisted after CSI restart"
    else
        echo "✗ Data mismatch after restart"
        exit 1
    fi
fi

echo "=== All Tests Passed ==="

