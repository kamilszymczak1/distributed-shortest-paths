#!/bin/bash
# Script to deploy the application to Kubernetes (minikube)

set -e

echo "=== Deploying to Kubernetes ==="

# Check if minikube is running
if ! minikube status | grep -q "Running"; then
    echo "Error: minikube is not running. Start it with: minikube start"
    exit 1
fi

# Apply Kubernetes manifests
echo "Creating namespace..."
kubectl apply -f k8s/namespace.yaml

echo "Creating ConfigMap..."
kubectl apply -f k8s/configmap.yaml

echo "Creating leader service (DNS entry for leader)..."
kubectl apply -f k8s/leader-service.yaml

echo "Creating Persistent Volume and Claim..."
kubectl apply -f k8s/pvc.yaml

echo "Deploying workers..."
kubectl apply -f k8s/worker-statefulset.yaml

# Wait for workers to be ready
echo "Waiting for workers to be ready..."
kubectl wait --namespace graph-dist --for=condition=Ready pod -l app=graph-worker --timeout=120s

echo "Workers are ready. Starting leader job..."
kubectl apply -f k8s/leader-job.yaml

echo ""
echo "=== Deployment complete! ==="
echo ""
echo "Starting port-forwarding for the leader API..."

# Wait for the leader pod to be running
echo "Waiting for leader to start..."
kubectl wait --namespace graph-dist --for=condition=Ready pod -l app=graph-leader --timeout=120s

POD_NAME=$(kubectl get pods --namespace graph-dist -l "app=graph-leader" -o jsonpath="{.items[0].metadata.name}")
kubectl port-forward --namespace graph-dist $POD_NAME 8080:8080 > /dev/null 2>&1 &
PORT_FORWARD_PID=$!

echo "The leader API is now available at http://localhost:8080"
echo "Port-forwarding is running in the background with PID: $PORT_FORWARD_PID"
echo "You can stop it with: kill $PORT_FORWARD_PID"
echo ""
echo "Useful commands:"
echo "  kubectl get pods -n graph-dist              # View all pods"
echo "  kubectl logs -n graph-dist -l app=graph-leader -f  # Follow leader logs"
echo "  kubectl logs -n graph-dist worker-0         # View worker-0 logs"
echo "  ./k8s_cleanup.sh                            # Remove all resources"
