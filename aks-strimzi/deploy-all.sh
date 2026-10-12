#!/bin/bash
set -euo pipefail

#####################################################################
# Full deployment pipeline — run from the aks-strimzi/ directory
#####################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "============================================================"
echo " Step 0: Install prerequisites (Envoy GW, Cert-Manager, Strimzi)"
echo "============================================================"
bash 00-install-prerequisites.sh

echo ""
echo "============================================================"
echo " Step 1: Create GatewayClass + EnvoyProxy config"
echo "============================================================"
kubectl apply -f 01-gateway-class.yaml
echo "Waiting for GatewayClass to be accepted..."
sleep 5

echo ""
echo "============================================================"
echo " Step 2: Create Gateway (TLS Passthrough on port 9092)"
echo "============================================================"
kubectl apply -f 02-gateway.yaml
echo "Waiting for Gateway to be programmed..."
sleep 10

echo ""
echo "============================================================"
echo " Step 3: Create self-signed ClusterIssuer + Certificate"
echo "============================================================"
kubectl apply -f 03-certificate.yaml
echo "Waiting for certificate to be issued..."
kubectl wait --for=condition=Ready certificate/kafka-certificate -n strimzi --timeout=120s

echo ""
echo "============================================================"
echo " Step 4: Create Kafka NodePools (Controller + Broker)"
echo "============================================================"
kubectl apply -f 04-kafka-node-pools.yaml

echo ""
echo "============================================================"
echo " Step 5: Deploy Kafka cluster (KRaft mode)"
echo "============================================================"
kubectl apply -f 05-kafka.yaml
echo "Waiting for Kafka cluster to become ready (this may take 3-5 minutes)..."
kubectl wait kafka/gateway-api-kafka --for=condition=Ready --timeout=600s -n strimzi

echo ""
echo "============================================================"
echo " Step 6: Create TLSRoutes (bootstrap + per-broker)"
echo "============================================================"
kubectl apply -f 06-tls-routes.yaml

echo ""
echo "============================================================"
echo " Step 7: Create KafkaUser + KafkaTopic"
echo "============================================================"
kubectl apply -f 07-user-and-topic.yaml
echo "Waiting for KafkaUser to be ready..."
kubectl wait kafkauser/kafka-test-user --for=condition=Ready --timeout=120s -n strimzi

echo ""
echo "============================================================"
echo " Step 8: Get the Azure Load Balancer external IP"
echo "============================================================"
echo "Waiting for LoadBalancer IP assignment..."
sleep 15
LB_IP=$(kubectl get svc -n envoy-gateway-system \
  -l gateway.envoyproxy.io/owning-gateway-name=kafka-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "pending")

echo ""
echo "============================================================"
echo " DEPLOYMENT COMPLETE"
echo "============================================================"
echo ""
echo "Azure Load Balancer IP: ${LB_IP}"
echo ""
echo "Add these DNS entries (or /etc/hosts) pointing to ${LB_IP}:"
echo "  ${LB_IP}  bootstrap.kafka.aks.mydomain.com"
echo "  ${LB_IP}  broker-10.kafka.aks.mydomain.com"
echo "  ${LB_IP}  broker-11.kafka.aks.mydomain.com"
echo "  ${LB_IP}  broker-12.kafka.aks.mydomain.com"
echo ""
echo "Then run: bash 08-create-client-config.sh"
echo "to generate the client properties file for external access."
