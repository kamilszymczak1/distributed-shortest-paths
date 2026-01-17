#!/bin/bash
# Script to clean up Kubernetes resources

set -e

echo "=== Cleaning up Kubernetes resources ==="

if [[ -e /tmp/leader-port-forward.pid ]]; then
    PORT_FORWARD_PID=$(cat /tmp/leader-port-forward.pid)
    echo "Stopping port-forwarding with PID: $PORT_FORWARD_PID"
    kill $PORT_FORWARD_PID || echo "Port-forward process already stopped."
    rm /tmp/leader-port-forward.pid
else
    echo "No port-forward PID file found. Skipping port-forward cleanup."
fi

# Delete all resources in the namespace
echo "Deleting leader job..."
kubectl delete job leader -n graph-dist --ignore-not-found

echo "Deleting workers..."
kubectl delete statefulset worker -n graph-dist --ignore-not-found
kubectl delete service worker -n graph-dist --ignore-not-found

echo "Deleting PVC and PV..."
kubectl delete pvc graph-data-pvc -n graph-dist --ignore-not-found
kubectl delete pv graph-data-pv --ignore-not-found

echo "Deleting ConfigMap..."
kubectl delete configmap graph-config -n graph-dist --ignore-not-found

echo "Deleting namespace..."
kubectl delete namespace graph-dist --ignore-not-found

echo ""
echo "=== Cleanup complete! ==="
