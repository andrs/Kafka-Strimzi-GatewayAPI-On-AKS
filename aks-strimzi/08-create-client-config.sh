#!/bin/bash
set -euo pipefail

#####################################################################
# Extracts the CA cert + SASL credentials from Kubernetes secrets
# and builds a client.properties file for external Kafka access.
#
# Prerequisites:
#   - kubectl configured for the AKS cluster
#   - jq installed
#   - keytool (from JDK) installed
#
# Usage:
#   ./08-create-client-config.sh
#   Then use /tmp/kafka-client.properties with your Kafka CLI tools.
#####################################################################

NAMESPACE="strimzi"
CERT_SECRET="kafka-certificate"
USER_SECRET="kafka-test-user"
TRUSTSTORE="/tmp/kafka-truststore.jks"
TRUSTSTORE_PASS="changeit"
PROPERTIES="/tmp/kafka-client.properties"

echo "==> Extracting CA certificate from secret/${CERT_SECRET}..."
kubectl get secret "${CERT_SECRET}" -n "${NAMESPACE}" -o json \
  | jq -r '.data."ca.crt"' | base64 --decode > /tmp/kafka-ca.crt

echo "==> Building JKS truststore at ${TRUSTSTORE}..."
rm -f "${TRUSTSTORE}"
keytool -importcert -alias kafka-ca \
  -file /tmp/kafka-ca.crt \
  -keystore "${TRUSTSTORE}" \
  -storepass "${TRUSTSTORE_PASS}" \
  -noprompt

echo "==> Extracting SASL/SCRAM credentials from secret/${USER_SECRET}..."
JAAS_CONFIG=$(kubectl get secret "${USER_SECRET}" -n "${NAMESPACE}" -o json \
  | jq -r '.data."sasl.jaas.config"' | base64 --decode)

echo "==> Writing client properties to ${PROPERTIES}..."
cat <<EOF > "${PROPERTIES}"
bootstrap.servers=bootstrap.kafka.aks.mydomain.com:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=${JAAS_CONFIG}
ssl.truststore.location=${TRUSTSTORE}
ssl.truststore.password=${TRUSTSTORE_PASS}
EOF

echo ""
echo "==> Done! Client config written to: ${PROPERTIES}"
echo ""
echo "==> To get the Azure LB public IP for DNS configuration:"
echo "    kubectl get svc -n envoy-gateway-system -l gateway.envoyproxy.io/owning-gateway-name=kafka-gateway"
echo ""
echo "==> Add these entries to /etc/hosts (or your DNS) pointing to that IP:"
echo "    <LB_IP>  bootstrap.kafka.aks.mydomain.com"
echo "    <LB_IP>  broker-10.kafka.aks.mydomain.com"
echo "    <LB_IP>  broker-11.kafka.aks.mydomain.com"
echo "    <LB_IP>  broker-12.kafka.aks.mydomain.com"
echo ""
echo "==> Test with:"
echo "    kafka-topics.sh --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 --list --command-config ${PROPERTIES}"
echo "    kafka-console-producer.sh --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 --topic test-topic --producer.config ${PROPERTIES}"
echo "    kafka-console-consumer.sh --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 --topic test-topic --from-beginning --consumer.config ${PROPERTIES}"
