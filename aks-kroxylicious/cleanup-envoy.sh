#!/bin/bash
set -euo pipefail

###############################################################################
# Cleanup old Envoy Gateway resources
#
# Removes: TLSRoutes, Gateway, GatewayClass, EnvoyProxy config,
#          Envoy Gateway Helm release, and Gateway API CRDs.
#
# Run this BEFORE deploying Kroxylicious to avoid port conflicts.
###############################################################################

echo "==> Removing TLSRoutes..."
kubectl delete tlsroute kafka-bootstrap kafka-broker-10 kafka-broker-11 kafka-broker-12 \
  -n strimzi --ignore-not-found

echo ""
echo "==> Removing Gateway..."
kubectl delete gateway kafka-gateway -n strimzi --ignore-not-found

echo ""
echo "==> Removing GatewayClass..."
kubectl delete gatewayclass kafka-gateway-class --ignore-not-found

echo ""
echo "==> Removing EnvoyProxy config..."
kubectl delete envoyproxy kafka-proxy-config -n envoy-gateway-system --ignore-not-found

echo ""
echo "==> Uninstalling Envoy Gateway Helm release..."
helm uninstall envoy-gateway -n envoy-gateway-system 2>/dev/null || echo "  (not found)"

echo ""
echo "==> Removing Gateway API experimental CRDs..."
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml \
  --ignore-not-found 2>/dev/null || echo "  (some CRDs may still be in use)"

echo ""
echo "==> Cleaning up envoy-gateway-system namespace..."
kubectl delete namespace envoy-gateway-system --ignore-not-found 2>/dev/null || true

echo ""
echo "==> Envoy Gateway cleanup complete."
