#!/bin/bash
set -e

# curvine-csi/deploy and the examples all use the curvine namespace.
NAMESPACE="curvine"
TEST_POD="curvine-test-pod"
TEST_PVC="curvine-pvc"
# pod-curvine.yaml mounts the PVC at /data.
TEST_FILE="/data/test.txt"

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
# A PVC reports Bound in status.phase; it has no Bound condition.
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/$TEST_PVC -n $NAMESPACE --timeout=60s
echo "✓ PVC created and bound"

# Test 2: Create Pod
echo "Test 2: Creating test pod..."
kubectl apply -f curvine-csi/examples/pod-curvine.yaml
kubectl wait --for=condition=Ready pod/$TEST_POD -n $NAMESPACE --timeout=60s
echo "✓ Pod created and ready"

# Test 3: Write data
echo "Test 3: Writing test data..."
kubectl exec $TEST_POD -n $NAMESPACE -- \
  sh -c "echo \"CSI Test: \$(date)\" > $TEST_FILE"
echo "✓ Data written"

# Test 4: Read data
echo "Test 4: Reading test data..."
DATA=$(kubectl exec $TEST_POD -n $NAMESPACE -- cat $TEST_FILE)
echo "✓ Data read: $DATA"

# Test 5: Restart CSI Node Pod (standalone mode)
if kubectl get pods -n $NAMESPACE -l app=curvine-csi-node --no-headers | grep -q Running; then
    echo "Test 5: Restarting CSI Node Pod..."
    kubectl delete pod -l app=curvine-csi-node -n $NAMESPACE
    # Waits for the replacement pods; `kubectl wait -l` can run before they exist.
    kubectl rollout status daemonset/curvine-csi-node -n $NAMESPACE --timeout=120s

    # Verify data still accessible
    DATA_AFTER=$(kubectl exec $TEST_POD -n $NAMESPACE -- cat $TEST_FILE)
    if [ "$DATA" == "$DATA_AFTER" ]; then
        echo "✓ Data persisted after CSI restart"
    else
        echo "✗ Data mismatch after restart"
        exit 1
    fi
fi

echo "=== All Tests Passed ==="

