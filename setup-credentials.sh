#!/bin/bash
set -euo pipefail

###############################################################################
# Extracts Kafka credentials from Kubernetes secrets and writes them to
# local files that the Python producer/consumer and CLI tools can use.
#
# Creates:
#   ./creds/ca.crt                  — CA certificate for TLS verification
#   ./creds/client.properties       — Java/CLI client properties file
#   ./creds/.env                    — Environment variables for Python scripts
#
# Prerequisites: kubectl, jq, base64
###############################################################################

NAMESPACE="strimzi"
CERT_SECRET="kafka-certificate"
USER_SECRET="kafka-test-user"
BOOTSTRAP="bootstrap.kafka.aks.mydomain.com:9092"
CREDS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/creds"

mkdir -p "${CREDS_DIR}"

echo "==> Extracting CA certificate from secret/${CERT_SECRET}..."
kubectl get secret "${CERT_SECRET}" -n "${NAMESPACE}" -o jsonpath='{.data.ca\.crt}' \
  | base64 --decode > "${CREDS_DIR}/ca.crt"

echo "==> Extracting SCRAM credentials from secret/${USER_SECRET}..."
SASL_USERNAME="kafka-test-user"
SASL_PASSWORD=$(kubectl get secret "${USER_SECRET}" -n "${NAMESPACE}" -o jsonpath='{.data.password}' \
  | base64 --decode)
JAAS_CONFIG=$(kubectl get secret "${USER_SECRET}" -n "${NAMESPACE}" -o jsonpath='{.data.sasl\.jaas\.config}' \
  | base64 --decode)

echo "==> Writing .env file..."
cat > "${CREDS_DIR}/.env" <<EOF
KAFKA_BOOTSTRAP=${BOOTSTRAP}
KAFKA_TOPIC=test-topic
KAFKA_USERNAME=${SASL_USERNAME}
KAFKA_PASSWORD=${SASL_PASSWORD}
KAFKA_CA_CERT=${CREDS_DIR}/ca.crt
KAFKA_GROUP_ID=test-consumer-group
EOF

echo "==> Writing client.properties (for Kafka CLI / Docker)..."
cat > "${CREDS_DIR}/client.properties" <<EOF
bootstrap.servers=${BOOTSTRAP}
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=${JAAS_CONFIG}
ssl.truststore.type=PEM
ssl.truststore.location=${CREDS_DIR}/ca.crt
EOF

echo "==> Getting Azure Load Balancer external IP..."
LB_IP=$(kubectl get svc -n envoy-gateway-system \
  -l gateway.envoyproxy.io/owning-gateway-name=kafka-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "UNKNOWN")

echo ""
echo "============================================================"
echo " Credentials written to: ${CREDS_DIR}/"
echo "============================================================"
echo ""
echo " Files:"
echo "   ca.crt              — CA certificate"
echo "   client.properties   — Kafka CLI config"
echo "   .env                — Python scripts config"
echo ""
echo " Load Balancer IP: ${LB_IP}"
echo ""
echo " Ensure /etc/hosts (or DNS) contains:"
echo "   ${LB_IP}  bootstrap.kafka.aks.mydomain.com"
echo "   ${LB_IP}  broker-10.kafka.aks.mydomain.com"
echo "   ${LB_IP}  broker-11.kafka.aks.mydomain.com"
echo "   ${LB_IP}  broker-12.kafka.aks.mydomain.com"
echo ""
