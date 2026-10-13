#!/bin/bash
set -euo pipefail

###############################################################################
# Step 0: Install Prerequisites
#   - Cert-Manager  (TLS certificates for client-to-proxy)
#   - Strimzi Kafka Operator  (manages Kafka on Kubernetes)
#
# NOTE: Envoy Gateway is NOT needed — Kroxylicious replaces it.
###############################################################################

echo "==> Installing Cert-Manager (v1.16.2) in cert-manager namespace..."
helm repo add jetstack https://charts.jetstack.io --force-update
helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.2 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --wait

echo ""
echo "==> Installing Strimzi Kafka Operator (0.51.0) in strimzi namespace..."
helm repo add strimzi https://strimzi.io/charts/ --force-update
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.51.0 \
  --namespace strimzi \
  --create-namespace \
  --set watchNamespaces="{strimzi}" \
  --wait

echo ""
echo "==> Applying Strimzi CRDs (0.51.0) to ensure v1 API is available..."
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-crds-0.51.0.yaml --server-side --force-conflicts

echo ""
echo "==> All prerequisites installed successfully."
echo "    Verify with:"
echo "      kubectl get pods -n cert-manager"
echo "      kubectl get pods -n strimzi"
