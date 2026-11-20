#!/usr/bin/env bash
set -euo pipefail

if ! minikube status >/dev/null 2>&1; then
    minikube start --container-runtime=docker
fi

python ./scripts/start_dev_snapshot_server.py -d --name snapshot-dev

# Stops the snapshot server on EXIT (cleanup)
stop_snapshot_server() {
    eval $(minikube docker-env -u) # Point back at the devcontainer's Docker daemon
    docker stop snapshot-dev >/dev/null
}
trap stop_snapshot_server EXIT

eval $(minikube docker-env) # Point Docker daemon to Minikube's

minikube addons enable metrics-server

docker build -f docker/Dockerfile.db_pod     -t burstkv-dbpod:latest .
docker build -f docker/Dockerfile.controller -t burstkv-controller:latest .

kubectl apply -f deploy/deployment.yaml

kubectl rollout restart deploy/burstkv-controller # Refresh the Image rollout
kubectl rollout status deploy/burstkv-controller --timeout=120s # Wait for the images to be rolled out 

kubectl get pods -l 'burstkv/role in (controller,warm)'
kubectl logs deploy/burstkv-controller

kubectl port-forward deploy/burstkv-controller 6381:6381 6379:6379