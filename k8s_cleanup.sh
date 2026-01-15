#!/bin/bash
# Script to clean up Kubernetes resources

set -e

echo "=== Cleaning up Kubernetes resources ==="

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
