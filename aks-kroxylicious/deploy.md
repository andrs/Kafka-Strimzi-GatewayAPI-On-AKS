# Deployment Commands: Kafka + Kroxylicious on AKS

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| **Azure CLI** | Latest | `curl -sL https://aka.ms/InstallAzureCLIDeb \| sudo bash` |
| **kubectl** | 1.35+ | Bundled with `az aks install-cli` |
| **Helm** | 3.x | `curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 \| bash` |
| **Python** | 3.10+ | `apt install python3 python3-pip` |

---

## Step 0: AKS Cluster Setup

### Login and connect

```bash
az login
az aks get-credentials --resource-group aks-cluster-andres --name aks
```

### Scale to 2 nodes (if needed)

```bash
az aks nodepool scale \
  --resource-group aks-cluster-andres \
  --cluster-name aks \
  --name aks \
  --node-count 2

kubectl get nodes
```

| Expected Output | |
|---|---|
| `aks-aks-*-vmss000000` | Ready |
| `aks-aks-*-vmss000001` | Ready |

---

## Step 1: Install Cert-Manager

```bash
helm repo add jetstack https://charts.jetstack.io --force-update

helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.2 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --wait
```

### Verify

```bash
kubectl get pods -n cert-manager
```

| Pod | Status |
|-----|--------|
| `cert-manager-*` | Running |
| `cert-manager-cainjector-*` | Running |
| `cert-manager-webhook-*` | Running |

---

## Step 2: Install Strimzi Kafka Operator

```bash
helm repo add strimzi https://strimzi.io/charts/ --force-update

helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.51.0 \
  --namespace strimzi \
  --create-namespace \
  --set watchNamespaces="{strimzi}" \
  --wait
```

### Apply CRDs (ensures v1 API is available)

```bash
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-crds-0.51.0.yaml \
  --server-side --force-conflicts
```

### Verify

```bash
kubectl get pods -n strimzi
```

| Pod | Status |
|-----|--------|
| `strimzi-cluster-operator-*` | Running |

### Known issue: stale RoleBindings

If Helm fails with `rolebindings already exists`, run:

```bash
kubectl delete rolebinding strimzi-cluster-operator \
  strimzi-cluster-operator-watched \
  strimzi-cluster-operator-entity-operator-delegation \
  -n strimzi --ignore-not-found
```

Then re-run the `helm upgrade --install` command.

---

## Step 3: Install Kroxylicious Operator

```bash
KROXY_VERSION="0.25.0"

# Install CRDs
kubectl apply -f \
  https://github.com/kroxylicious/kroxylicious/releases/download/v${KROXY_VERSION}/kroxylicious-operator-${KROXY_VERSION}-crds.yaml

kubectl wait --for=condition=Established --timeout=60s crd kafkaproxies.kroxylicious.io

# Install Operator
kubectl apply -f \
  https://github.com/kroxylicious/kroxylicious/releases/download/v${KROXY_VERSION}/kroxylicious-operator-${KROXY_VERSION}-install.yaml

kubectl wait --for=condition=Available deployment/kroxylicious-operator \
  -n kroxylicious-operator --timeout=120s
```

### Verify

```bash
kubectl get pods -n kroxylicious-operator
kubectl get crd | grep kroxylicious
```

| CRD | Purpose |
|-----|---------|
| `kafkaproxies.kroxylicious.io` | Proxy instance |
| `kafkaproxyingresses.kroxylicious.io` | Network exposure |
| `kafkaservices.kroxylicious.io` | Backend Kafka reference |
| `virtualkafkaclusters.kroxylicious.io` | Virtual cluster |
| `kafkaprotocolfilters.kroxylicious.io` | Protocol filters |

---

## Step 4: Create StorageClass

```bash
kubectl apply -f 02-storage-class.yaml
```

### Verify

```bash
kubectl get storageclass managed-csi-retain
```

| Field | Value |
|-------|-------|
| **Provisioner** | disk.csi.azure.com |
| **ReclaimPolicy** | Retain |
| **VolumeBindingMode** | WaitForFirstConsumer |

---

## Step 5: Create TLS Certificate

```bash
kubectl apply -f 03-certificate.yaml

kubectl wait --for=condition=Ready certificate/kafka-certificate \
  -n strimzi --timeout=120s
```

### Verify

```bash
kubectl get certificate kafka-certificate -n strimzi
kubectl get secret kafka-certificate -n strimzi
```

| Field | Value |
|-------|-------|
| **Secret** | `kafka-certificate` |
| **Issuer** | `kafka-selfsigned-issuer` (self-signed) |
| **DNS SANs** | `*.kafka.aks.mydomain.com` |
| **Keys in secret** | `tls.crt`, `tls.key`, `ca.crt` |

---

## Step 6: Deploy Kafka Node Pools

```bash
kubectl apply -f 04-kafka-node-pools.yaml
```

### Verify

```bash
kubectl get kafkanodepool -n strimzi
```

| Pool | Replicas | Roles | IDs |
|------|----------|-------|-----|
| `controller` | 1 | controller | 0-9 |
| `broker` | 3 | broker | 10-100 |

---

## Step 7: Deploy Kafka Cluster

```bash
kubectl apply -f 05-kafka.yaml

kubectl wait kafka/gateway-api-kafka --for=condition=Ready \
  --timeout=600s -n strimzi
```

This takes 3-5 minutes. The operator creates broker pods, configures KRaft, and waits for all brokers to join the cluster.

### Verify

```bash
kubectl get kafka -n strimzi
kubectl get pods -n strimzi
```

| Pod | Status |
|-----|--------|
| `gateway-api-kafka-controller-0` | Running |
| `gateway-api-kafka-broker-10` | Running |
| `gateway-api-kafka-broker-11` | Running |
| `gateway-api-kafka-broker-12` | Running |
| `gateway-api-kafka-entity-operator-*` | Running (2/2) |

### Known issue: Entity Operator CrashLoopBackOff

If the entity operator crashes with `404 Not Found` on `kafka.strimzi.io/v1`:

```bash
# Re-apply CRDs
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-crds-0.51.0.yaml

# Restart entity operator
kubectl delete pod -n strimzi -l strimzi.io/name=gateway-api-kafka-entity-operator
```

---

## Step 8: Create KafkaUser and Topics

```bash
kubectl apply -f 06-user-and-topics.yaml

kubectl wait kafkauser/kafka-test-user --for=condition=Ready \
  --timeout=120s -n strimzi
```

### Verify

```bash
kubectl get kafkauser -n strimzi
kubectl get kafkatopic -n strimzi
```

| Resource | Name | Details |
|----------|------|---------|
| **KafkaUser** | `kafka-test-user` | SCRAM-SHA-512, full ACLs |
| **KafkaTopic** | `test-topic` | 12 partitions, RF=3, segment 10 MB |
| **KafkaTopic** | `events` | 12 partitions, RF=3, segment 1 GB |
| **KafkaTopic** | `orders` | 12 partitions, RF=3, segment 1 GB |

---

## Step 9: Deploy Kroxylicious Proxy

```bash
kubectl apply -f 07-kroxylicious.yaml
```

### Verify KafkaService resolved

```bash
kubectl get kafkaservice kafka-backend -n strimzi -o yaml | grep -A10 "status:"
```

| Status Field | Expected Value |
|---|---|
| `bootstrapServers` | `gateway-api-kafka-kafka-bootstrap.strimzi.svc:9093` |
| `ResolvedRefs` | `True` |
| `tls.trustAnchorRef.name` | `gateway-api-kafka-cluster-ca-cert` |

### Verify VirtualKafkaCluster accepted

```bash
kubectl get virtualkafkacluster gateway-api-kafka -n strimzi -o yaml | grep -A15 "status:"
```

| Status Field | Expected Value |
|---|---|
| `ResolvedRefs` | `True` |
| `Accepted` | `True` |
| `bootstrapServer` | `bootstrap.kafka.aks.mydomain.com:9083` |
| `protocol` | `TLS` |

### Verify proxy pod running

```bash
kubectl get pods -n strimzi -l app.kubernetes.io/managed-by=kroxylicious-operator
```

| Pod | Status |
|-----|--------|
| `kafka-proxy-*` | Running (1/1) |

### Check proxy logs

```bash
kubectl logs -n strimzi -l app.kubernetes.io/managed-by=kroxylicious-operator --tail=10
```

Expected:
```
Kroxylicious version {version=kroxylicious: 0.25.0}
Virtual cluster lifecycle transition {from=Initializing, to=Serving, virtualCluster=gateway-api-kafka}
```

---

## Step 10: Get Load Balancer IP

```bash
kubectl get svc external-lb -n strimzi
```

| Field | Value |
|-------|-------|
| **Type** | LoadBalancer |
| **External IP** | `4.158.213.42` (yours will differ) |
| **Port** | `9083` |

### Alternate command (JSON)

```bash
kubectl get svc external-lb -n strimzi \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

---

## Step 11: Configure DNS

```bash
# Remove old entries
sudo sed -i '/kafka.aks.mydomain.com/d' /etc/hosts

# Add new entries (replace IP with yours)
LB_IP="4.158.213.42"
echo "${LB_IP}  bootstrap.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
echo "${LB_IP}  broker-10.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
echo "${LB_IP}  broker-11.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
echo "${LB_IP}  broker-12.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
```

### Verify

```bash
grep kafka.aks.mydomain.com /etc/hosts
nc -zv bootstrap.kafka.aks.mydomain.com 9083
```

---

## Step 12: Extract Client Credentials

```bash
cd /home/ubuntu/kafka-strimzi-gateway-api
bash setup-credentials-kroxy.sh
```

### Files created

| File | Purpose |
|------|---------|
| `creds/ca.crt` | CA certificate (client-to-proxy TLS) |
| `creds/.env` | Environment variables for Python scripts |
| `creds/client.properties` | Kafka CLI config (Java/Docker) |

### Verify .env contents

```bash
cat creds/.env
```

| Variable | Expected Value |
|----------|---------------|
| `KAFKA_BOOTSTRAP` | `bootstrap.kafka.aks.mydomain.com:9083` |
| `KAFKA_TOPIC` | `test-topic` |
| `KAFKA_USERNAME` | `kafka-test-user` |
| `KAFKA_PASSWORD` | (auto-extracted from K8s secret) |
| `KAFKA_CA_CERT` | `/home/ubuntu/kafka-strimzi-gateway-api/creds/ca.crt` |

---

## Step 13: Test Producer

```bash
pip install -r requirements.txt

python3 producer.py --rate 5 --count 20
```

### Expected output

```
Producer starting — topic=test-topic  rate=5.0/sec  count=20
Bootstrap: bootstrap.kafka.aks.mydomain.com:9083
------------------------------------------------------------
  SEND  #1       order=a833b100  customer=CUST-1736  amount=$318.00  status=completed
  ...
  ACK   partition=10  offset=1         [1 delivered]
  ...
Done — sent=20  delivered=20  failed=0
```

| Metric | Expected |
|--------|----------|
| **Sent** | 20 |
| **Delivered** | 20 |
| **Failed** | 0 |

---

## Step 14: Test Consumer

```bash
python3 consumer.py --from-beginning --group test-kroxy-consumer
```

### Expected output

```
Consumer starting — topic=test-topic  group=test-kroxy-consumer  offset=earliest
Bootstrap: bootstrap.kafka.aks.mydomain.com:9083
------------------------------------------------------------------------------------------------------------------------
      #       Part:Offset    Time       Order        Customer     Product                Qty     Amount  Status
------------------------------------------------------------------------------------------------------------------------
  [+] #1      P 9: 0         18:07:46  order=668ac777  customer=CUST-6678  Coffee Maker     qty=2  $   99.98  completed
  [!] #2      P 1: 2         18:07:47  order=b9505c7b  customer=CUST-8215  Running Shoes    qty=2  $  259.90  refunded
  [~] #3      P 0: 1         18:07:46  order=cde543ef  customer=CUST-2484  Cast Iron Skillet qty=2  $  89.98   pending
  ...
```

| Icon | Status |
|------|--------|
| `[+]` | completed |
| `[~]` | pending |
| `[!]` | refunded |

---

## Step 15: Verify Proxy Metrics

```bash
POD=$(kubectl get pods -n strimzi \
  -l app.kubernetes.io/managed-by=kroxylicious-operator \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec "$POD" -n strimzi -- \
  curl -s http://localhost:9190/metrics | grep "connections_total"
```

### Expected

| Metric | bootstrap | broker-10 | broker-11 | broker-12 |
|--------|-----------|-----------|-----------|-----------|
| `client_to_proxy_connections_total` | 2 | 2 | 2 | 3 |

---

## Automated Deployment (All Steps at Once)

```bash
cd /home/ubuntu/kafka-strimzi-gateway-api/aks-kroxylicious
bash deploy-all.sh
```

Then:

```bash
cd ..
bash setup-credentials-kroxy.sh

# Update /etc/hosts with the LB IP from the deploy output
# Test:
python3 producer.py --count 20
python3 consumer.py --from-beginning
```

---

## Cleanup Old Envoy Gateway (if migrating)

Run this **before** deploying Kroxylicious to avoid port conflicts:

```bash
cd /home/ubuntu/kafka-strimzi-gateway-api/aks-kroxylicious
bash cleanup-envoy.sh
```

### What it removes

| Resource | Name |
|----------|------|
| TLSRoute | `kafka-bootstrap`, `kafka-broker-10`, `kafka-broker-11`, `kafka-broker-12` |
| Gateway | `kafka-gateway` |
| GatewayClass | `kafka-gateway-class` |
| EnvoyProxy | `kafka-proxy-config` |
| Helm release | `envoy-gateway` |
| Gateway API CRDs | 10 CRDs (tlsroutes, gateways, httproutes, etc.) |
| Namespace | `envoy-gateway-system` |

---

## Full Cleanup (Remove Everything)

```bash
# 1. Remove Kroxylicious resources
kubectl delete -f 07-kroxylicious.yaml

# 2. Remove Kafka resources
kubectl delete -f 06-user-and-topics.yaml
kubectl delete -f 05-kafka.yaml
kubectl delete -f 04-kafka-node-pools.yaml
kubectl delete -f 03-certificate.yaml
kubectl delete -f 02-storage-class.yaml

# 3. Uninstall Kroxylicious Operator
kubectl delete -f \
  https://github.com/kroxylicious/kroxylicious/releases/download/v0.25.0/kroxylicious-operator-0.25.0-install.yaml

# 4. Uninstall Strimzi
helm uninstall strimzi-kafka-operator -n strimzi

# 5. Uninstall Cert-Manager
helm uninstall cert-manager -n cert-manager

# 6. Delete namespaces
kubectl delete namespace strimzi cert-manager kroxylicious-operator

# 7. Clean /etc/hosts
sudo sed -i '/kafka.aks.mydomain.com/d' /etc/hosts

# 8. Scale down AKS (optional, saves cost)
az aks nodepool scale \
  --resource-group aks-cluster-andres \
  --cluster-name aks \
  --name aks \
  --node-count 0
```

---

## Quick Reference: All Commands

| Step | Command | Duration |
|------|---------|----------|
| 0 | `az aks get-credentials ...` | ~5s |
| 1 | `helm upgrade --install cert-manager ...` | ~30s |
| 2 | `helm upgrade --install strimzi-kafka-operator ...` | ~30s |
| 3 | `kubectl apply -f kroxylicious-operator-install.yaml` | ~20s |
| 4 | `kubectl apply -f 02-storage-class.yaml` | ~1s |
| 5 | `kubectl apply -f 03-certificate.yaml` | ~5s |
| 6 | `kubectl apply -f 04-kafka-node-pools.yaml` | ~1s |
| 7 | `kubectl apply -f 05-kafka.yaml` + wait | ~3-5 min |
| 8 | `kubectl apply -f 06-user-and-topics.yaml` | ~10s |
| 9 | `kubectl apply -f 07-kroxylicious.yaml` | ~30s |
| 10 | `kubectl get svc external-lb -n strimzi` | ~1s |
| 11 | Update `/etc/hosts` | ~1s |
| 12 | `bash setup-credentials-kroxy.sh` | ~5s |
| 13 | `python3 producer.py --count 20` | ~10s |
| 14 | `python3 consumer.py --from-beginning` | ~10s |
| **Total** | | **~5-7 min** |
