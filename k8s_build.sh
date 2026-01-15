#!/bin/bash
# Script to build Docker images directly in minikube's Docker daemon
# This avoids having to push images to a registry for local testing

set -e

echo "=== Building Docker images for Kubernetes (minikube) ==="

# Check if minikube is running
if ! minikube status | grep -q "Running"; then
    echo "Error: minikube is not running. Start it with: minikube start"
    exit 1
fi

# Use minikube's Docker daemon
echo "Configuring to use minikube's Docker daemon..."
eval $(minikube docker-env)

# Build the images
echo "Building graph-leader:v1 (with data files)..."
docker build -f Dockerfile.leader -t graph-leader:v1 .

echo "Building graph-worker:v1..."
docker build --build-arg MODULE_NAME=worker -t graph-worker:v1 .

echo ""
echo "=== Build complete! ==="
echo "Images are now available in minikube's Docker registry."
echo "Run ./k8s_deploy.sh to deploy to Kubernetes."
