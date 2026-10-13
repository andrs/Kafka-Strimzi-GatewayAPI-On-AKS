#!/bin/bash
set -euo pipefail

###############################################################################
# Step 1: Install Kroxylicious Operator (v0.25.0)
#
# The operator manages KafkaProxy, VirtualKafkaCluster, KafkaProxyIngress,
# KafkaService, and KafkaProtocolFilter CRDs.
#
# Installs into the "kroxylicious-operator" namespace.
###############################################################################

KROXY_VERSION="0.25.0"
KROXY_BASE_URL="https://github.com/kroxylicious/kroxylicious/releases/download/v${KROXY_VERSION}"

echo "==> Installing Kroxylicious CRDs (v${KROXY_VERSION})..."
kubectl apply -f "${KROXY_BASE_URL}/kroxylicious-operator-${KROXY_VERSION}-crds.yaml"
kubectl wait --for=condition=Established --timeout=60s crd kafkaproxies.kroxylicious.io

echo ""
echo "==> Installing Kroxylicious Operator (v${KROXY_VERSION})..."
kubectl apply -f "${KROXY_BASE_URL}/kroxylicious-operator-${KROXY_VERSION}-install.yaml"

echo ""
echo "==> Waiting for Kroxylicious Operator deployment..."
kubectl wait --for=condition=Available deployment/kroxylicious-operator \
  -n kroxylicious-operator --timeout=120s

echo ""
echo "==> Kroxylicious Operator installed successfully."
echo "    Verify with:"
echo "      kubectl get pods -n kroxylicious-operator"
echo "      kubectl get crd | grep kroxylicious"
