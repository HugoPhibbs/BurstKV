#!/usr/bin/env bash
set -euo pipefail

if ! minikube status >/dev/null 2>&1; then
    minikube start --container-runtime=docker
fi

eval $(minikube docker-env)

minikube addons enable metrics-server

docker build -f docker/Dockerfile.db_pod     -t burstkv-dbpod:latest .
docker build -f docker/Dockerfile.controller -t burstkv-controller:latest .

kubectl apply -f deploy/deployment.yaml

kubectl rollout restart deploy/burstkv-controller # Refresh the Image rollout
kubectl rollout status deploy/burstkv-controller --timeout=120s # Wait for the images to be rolled out 

kubectl get pods -l 'burstkv/role in (controller,warm)'
kubectl logs deploy/burstkv-controller

kubectl port-forward deploy/burstkv-controller 6378:6378 6379:6379