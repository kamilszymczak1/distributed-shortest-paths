#!/bin/bash
# Quick script to stop the minikube cluster

set -e

echo "=== Stopping minikube ==="
minikube stop
echo "=== minikube stopped. ==="
