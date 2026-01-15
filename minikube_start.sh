#!/bin/bash
# Quick script to start minikube with recommended settings

set -e

echo "=== Starting minikube ==="

# Check if minikube is already running
if minikube status 2>/dev/null | grep -q "Running"; then
    echo "minikube is already running."
    minikube status
    exit 0
fi

# Start minikube with Docker driver (recommended for Linux)
# Adjust memory and CPUs based on your system
echo "Starting minikube with Docker driver..."
minikube start \
    --driver=docker \
    --cpus=4 \
    --memory=4096 \
    --disk-size=20g \
    --force

echo ""
echo "=== minikube started successfully! ==="
echo ""
echo "Next steps:"
echo "  1. Build images:  ./k8s_build.sh"
echo "  2. Deploy:        ./k8s_deploy.sh"
echo ""
minikube status
