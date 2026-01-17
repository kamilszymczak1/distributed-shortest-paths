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

POD_NAME=$(kubectl get pods --namespace graph-dist \
  -l "app=graph-leader" \
  --sort-by=.metadata.creationTimestamp \
  -o jsonpath="{.items[-1].metadata.name}")

echo "Discovered leader pod: $POD_NAME"

MAX_RETRIES=10
COUNT=0

while [ $COUNT -lt $MAX_RETRIES ]; do
    # Start port-forward in background
    kubectl port-forward --namespace graph-dist $POD_NAME 8080:8080 >port-forward.log 2>port-forward.err &
    PORT_FORWARD_PID=$!

    # Give it a second to see if it crashes immediately
    sleep 2

    if ps -p $PORT_FORWARD_PID > /dev/null; then
        echo "Port-forwarding successful on PID $PORT_FORWARD_PID"
        echo $PORT_FORWARD_PID > /tmp/leader-port-forward.pid
        break
    else
        echo "Port-forward failed, retrying ($((COUNT+1))/$MAX_RETRIES)..."
        COUNT=$((COUNT + 1))
        echo "Check port-forward.err for details."
    fi
done

if [ $COUNT -eq $MAX_RETRIES ]; then
    echo "Failed to establish port-forward after $MAX_RETRIES attempts."
    cat port-forward.err
    exit 1
fi

echo "The leader API is now available at http://localhost:8080"
echo "Port-forwarding is running in the background with PID: $PORT_FORWARD_PID"
echo "You can stop it with: kill $PORT_FORWARD_PID"
echo ""
echo "Useful commands:"
echo "  kubectl get pods -n graph-dist              # View all pods"
echo "  kubectl logs -n graph-dist -l app=graph-leader -f  # Follow leader logs"
echo "  kubectl logs -n graph-dist worker-0         # View worker-0 logs"
echo "  ./k8s_cleanup.sh                            # Remove all resources"
