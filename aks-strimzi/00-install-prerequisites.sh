#!/bin/bash
set -euo pipefail

#####################################################################
# Step 0: Install Prerequisites
#   - Gateway API CRDs
#   - Envoy Gateway
#   - Cert-Manager
#   - Strimzi Kafka Operator
#####################################################################

echo "==> Installing Gateway API CRDs..."
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml

echo ""
echo "==> Installing Envoy Gateway (v1.2.1) in envoy-gateway-system namespace..."
helm upgrade --install envoy-gateway oci://docker.io/envoyproxy/gateway-helm \
  --version v1.2.1 \
  --namespace envoy-gateway-system \
  --create-namespace \
  --wait

echo ""
echo "==> Installing Cert-Manager (v1.16.2) in cert-manager namespace..."
helm repo add jetstack https://charts.jetstack.io --force-update
helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.2 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --wait

echo ""
echo "==> Installing Strimzi Kafka Operator (0.44.0) in strimzi namespace..."
helm repo add strimzi https://strimzi.io/charts/ --force-update
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.44.0 \
  --namespace strimzi \
  --create-namespace \
  --set watchNamespaces="{strimzi}" \
  --wait

echo ""
echo "==> All prerequisites installed successfully."
echo "    Verify with:"
echo "      kubectl get pods -n envoy-gateway-system"
echo "      kubectl get pods -n cert-manager"
echo "      kubectl get pods -n strimzi"
