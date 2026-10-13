#!/bin/bash
set -euo pipefail

###############################################################################
# Full Kroxylicious deployment pipeline
#
# Deploys:
#   1. Prerequisites (Cert-Manager, Strimzi 0.51.0)
#   2. Kroxylicious Operator (0.25.0)
#   3. StorageClass (managed-csi-retain)
#   4. TLS Certificate (self-signed wildcard)
#   5. Kafka Cluster (KRaft mode, 1 controller + 3 brokers)
#   6. KafkaUser + KafkaTopics
#   7. Kroxylicious Proxy (KafkaProxy, Ingress, Service, VirtualCluster)
#
# Run from the aks-kroxylicious/ directory.
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "============================================================"
echo " Step 0: Install prerequisites (Cert-Manager, Strimzi)"
echo "============================================================"
bash 00-install-prerequisites.sh

echo ""
echo "============================================================"
echo " Step 1: Install Kroxylicious Operator"
echo "============================================================"
bash 01-install-kroxylicious.sh

echo ""
echo "============================================================"
echo " Step 2: Create StorageClass (managed-csi-retain)"
echo "============================================================"
kubectl apply -f 02-storage-class.yaml
echo "StorageClass created."

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
echo " Step 6: Create KafkaUser + KafkaTopics"
echo "============================================================"
kubectl apply -f 06-user-and-topics.yaml
echo "Waiting for KafkaUser to be ready..."
kubectl wait kafkauser/kafka-test-user --for=condition=Ready --timeout=120s -n strimzi

echo ""
echo "============================================================"
echo " Step 7: Deploy Kroxylicious Proxy"
echo "============================================================"
kubectl apply -f 07-kroxylicious.yaml
echo "Waiting for Kroxylicious proxy to be ready..."
sleep 10

echo ""
echo "==> Checking KafkaService status..."
kubectl get kafkaservice kafka-backend -n strimzi -o yaml 2>/dev/null | grep -A5 "status:" || echo "  (status pending)"

echo ""
echo "==> Checking VirtualKafkaCluster status..."
kubectl get virtualkafkacluster gateway-api-kafka -n strimzi -o yaml 2>/dev/null | grep -A10 "status:" || echo "  (status pending)"

echo ""
echo "============================================================"
echo " Step 8: Get the Azure Load Balancer external IP"
echo "============================================================"
echo "Waiting for LoadBalancer IP assignment (up to 60s)..."
for i in $(seq 1 12); do
  LB_IP=$(kubectl get svc -n strimzi \
    -l "app.kubernetes.io/managed-by=kroxylicious-operator" \
    -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  if [ -n "${LB_IP}" ] && [ "${LB_IP}" != "null" ]; then
    break
  fi
  echo "  ... waiting (${i}/12)"
  sleep 5
done

if [ -z "${LB_IP}" ] || [ "${LB_IP}" == "null" ]; then
  echo ""
  echo "WARNING: LoadBalancer IP not yet assigned."
  echo "Check manually with:"
  echo "  kubectl get svc -n strimzi -l app.kubernetes.io/managed-by=kroxylicious-operator"
  LB_IP="<PENDING>"
fi

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
echo "Then run: bash ../setup-credentials-kroxy.sh"
echo "to generate the client properties file for external access."
