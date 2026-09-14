#!/usr/bin/env bash
# minikube 싱글노드 K8s 통합 검증 환경 준비 (7.1.6)
#   1) minikube 기동  2) Chaos Mesh 설치  3) Testbed 이미지 빌드·적재  4) 메타 DB·StatefulSet 배포
# 사전 조건: docker, minikube, kubectl, helm, JDK 17 (JAVA_HOME)
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=batch-cluster-testbed:e2e
CHAOS_MESH_VERSION=2.8.4

minikube status >/dev/null 2>&1 || minikube start

helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null 2>&1 || true
helm repo update chaos-mesh >/dev/null
helm upgrade --install chaos-mesh chaos-mesh/chaos-mesh \
  --namespace chaos-mesh --create-namespace --version "${CHAOS_MESH_VERSION}" \
  --set chaosDaemon.runtime=containerd \
  --set chaosDaemon.socketPath=/run/containerd/containerd.sock \
  --set controllerManager.replicaCount=1 \
  --set dashboard.create=false \
  --set dnsServer.create=false \
  --wait --timeout 10m

./gradlew installDist
docker build -t "${IMAGE}" .
minikube image load --overwrite=true "${IMAGE}"

kubectl apply -f k8s/overlays/minikube/meta-db.yaml
kubectl -n ns-meta-db rollout status deployment/meta-db --timeout=180s
kubectl apply -k k8s/overlays/minikube
kubectl -n ns-batchservice rollout status statefulset/batch-scheduler --timeout=300s
