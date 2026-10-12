# Apache Kafka on AKS with Strimzi + Gateway API (Envoy Gateway)

## Architecture Overview

This deployment exposes Kafka externally via the Kubernetes Gateway API using Envoy Gateway with TLS passthrough. Instead of traditional Ingress or NodePort, SNI-based TLSRoutes direct traffic to individual Kafka brokers through an Azure Load Balancer.

```
External Client
      │
      ▼ (TLS + SNI)
Azure Load Balancer (:9092)
      │
      ▼
Envoy Gateway (TLS Passthrough)
      │
      ├── SNI: bootstrap.kafka.aks.mydomain.com  → ClusterIP: bootstrap service
      ├── SNI: broker-10.kafka.aks.mydomain.com  → ClusterIP: broker-10 service
      ├── SNI: broker-11.kafka.aks.mydomain.com  → ClusterIP: broker-11 service
      └── SNI: broker-12.kafka.aks.mydomain.com  → ClusterIP: broker-12 service
```

## Key Differences from KIND Example

| Aspect | KIND (original) | AKS (this deployment) |
|--------|----------------|-----------------------|
| Service type | NodePort (30992) | LoadBalancer (Azure LB) |
| Storage class | `standard` | `managed-csi` (Azure Managed Disks) |
| Envoy replicas | 1 | 2 (HA behind LB) |
| Domain | `*.strimzi.gateway.api.test` | `*.kafka.aks.mydomain.com` |
| External access | `localhost:9092` | Azure public IP on port 9092 |

## Files

| File | Purpose |
|------|---------|
| `00-install-prerequisites.sh` | Helm installs: Gateway API CRDs, Envoy Gateway, Cert-Manager, Strimzi |
| `01-gateway-class.yaml` | GatewayClass + EnvoyProxy with LoadBalancer |
| `02-gateway.yaml` | Gateway with TLS Passthrough on port 9092 |
| `03-certificate.yaml` | Self-signed ClusterIssuer + wildcard cert `*.kafka.aks.mydomain.com` |
| `04-kafka-node-pools.yaml` | 1 Controller + 3 Broker node pools (KRaft mode) |
| `05-kafka.yaml` | Kafka cluster with cluster-ip listener, SCRAM-SHA-512, per-broker advertisedHost |
| `06-tls-routes.yaml` | 4 TLSRoutes: bootstrap + broker-10/11/12 (SNI routing) |
| `07-user-and-topic.yaml` | KafkaUser (SCRAM-SHA-512) + KafkaTopic (12 partitions, RF=3) |
| `08-create-client-config.sh` | Extracts CA cert + JAAS credentials, builds client.properties |
| `deploy-all.sh` | Orchestrates all steps sequentially with health waits |

---

## Step-by-Step Deployment

### Step 0 — Install Prerequisites

```bash
# Gateway API CRDs
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml

# Envoy Gateway
helm upgrade --install envoy-gateway oci://docker.io/envoyproxy/gateway-helm \
  --version v1.2.1 \
  --namespace envoy-gateway-system \
  --create-namespace \
  --wait

# Cert-Manager
helm repo add jetstack https://charts.jetstack.io --force-update
helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.2 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --wait

# Strimzi Kafka Operator
helm repo add strimzi https://strimzi.io/charts/ --force-update
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.44.0 \
  --namespace strimzi \
  --create-namespace \
  --set watchNamespaces="{strimzi}" \
  --wait
```

**Verify prerequisites:**

```bash
kubectl get pods -n envoy-gateway-system
kubectl get pods -n cert-manager
kubectl get pods -n strimzi
```

### Step 1 — GatewayClass + EnvoyProxy (LoadBalancer for AKS)

```bash
kubectl apply -f 01-gateway-class.yaml
```

This creates:
- A `GatewayClass` named `kafka-gateway-class` backed by Envoy Gateway
- An `EnvoyProxy` configured with `type: LoadBalancer` so Azure provisions a public IP on port 9092

### Step 2 — Gateway (TLS Passthrough)

```bash
kubectl apply -f 02-gateway.yaml
```

Creates a `Gateway` in the `strimzi` namespace listening on port 9092 with `TLS` protocol and `Passthrough` mode. Envoy does not terminate TLS — it forwards the encrypted stream based on SNI.

### Step 3 — Self-Signed Certificate

```bash
kubectl apply -f 03-certificate.yaml
```

Creates:
- A `ClusterIssuer` using self-signed certificates
- A `Certificate` with wildcard SAN `*.kafka.aks.mydomain.com`

**Wait for it to be ready:**

```bash
kubectl wait --for=condition=Ready certificate/kafka-certificate -n strimzi --timeout=120s
kubectl get secret kafka-certificate -n strimzi
```

### Step 4 — Kafka Node Pools (KRaft)

```bash
kubectl apply -f 04-kafka-node-pools.yaml
```

Creates two node pools:
- **controller** — 1 replica, IDs 0–9, KRaft controller role
- **broker** — 3 replicas, IDs 10–100, data broker role

Both use `managed-csi` (Azure Managed Disks) with 10Gi PVCs.

### Step 5 — Deploy Kafka Cluster

```bash
kubectl apply -f 05-kafka.yaml
```

Deploys the Kafka cluster `gateway-api-kafka` with:
- KRaft mode (no ZooKeeper)
- Listener `gateway` on port 9092 (type: `cluster-ip`)
- SCRAM-SHA-512 authentication
- TLS via the cert-manager certificate
- Per-broker `advertisedHost` mapped to external DNS names

**Wait for Kafka to be ready (3–5 minutes):**

```bash
kubectl wait kafka/gateway-api-kafka --for=condition=Ready --timeout=600s -n strimzi
kubectl get pods -n strimzi
```

### Step 6 — TLSRoutes (SNI Routing)

```bash
kubectl apply -f 06-tls-routes.yaml
```

Creates 4 TLSRoutes:

| Route | SNI Hostname | Target Service |
|-------|-------------|----------------|
| Bootstrap | `bootstrap.kafka.aks.mydomain.com` | `gateway-api-kafka-kafka-gateway-bootstrap:9092` |
| Broker 10 | `broker-10.kafka.aks.mydomain.com` | `gateway-api-kafka-broker-gateway-10:9092` |
| Broker 11 | `broker-11.kafka.aks.mydomain.com` | `gateway-api-kafka-broker-gateway-11:9092` |
| Broker 12 | `broker-12.kafka.aks.mydomain.com` | `gateway-api-kafka-broker-gateway-12:9092` |

### Step 7 — KafkaUser + KafkaTopic

```bash
kubectl apply -f 07-user-and-topic.yaml
```

Creates:
- `kafka-test-user` with SCRAM-SHA-512 and full ACL permissions
- `test-topic` with 12 partitions and replication factor 3

**Wait for user to be ready:**

```bash
kubectl wait kafkauser/kafka-test-user --for=condition=Ready --timeout=120s -n strimzi
```

### Step 8 — Configure DNS + Client

**Get the Azure Load Balancer public IP:**

```bash
kubectl get svc -n envoy-gateway-system \
  -l gateway.envoyproxy.io/owning-gateway-name=kafka-gateway \
  -o jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}'
```

**Add DNS entries** (or `/etc/hosts` for testing):

```
<LB_PUBLIC_IP>  bootstrap.kafka.aks.mydomain.com
<LB_PUBLIC_IP>  broker-10.kafka.aks.mydomain.com
<LB_PUBLIC_IP>  broker-11.kafka.aks.mydomain.com
<LB_PUBLIC_IP>  broker-12.kafka.aks.mydomain.com
```

**Generate client config:**

```bash
bash 08-create-client-config.sh
```

This extracts the CA certificate and SCRAM credentials from Kubernetes secrets and writes `/tmp/kafka-client.properties`.

---

## Testing External Connectivity

```bash
# List topics
kafka-topics.sh \
  --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
  --list \
  --command-config /tmp/kafka-client.properties

# Produce messages
kafka-console-producer.sh \
  --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
  --topic test-topic \
  --producer.config /tmp/kafka-client.properties

# Consume messages
kafka-console-consumer.sh \
  --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
  --topic test-topic \
  --from-beginning \
  --consumer.config /tmp/kafka-client.properties
```

---

## Automated Full Deployment

To deploy everything in one shot:

```bash
cd aks-strimzi && bash deploy-all.sh
```

---

## Verification Commands

```bash
# Check all pods
kubectl get pods -n strimzi -o wide

# Check Kafka cluster status
kubectl get kafka -n strimzi

# Check TLSRoutes are accepted
kubectl get tlsroutes -n strimzi

# Check Gateway status
kubectl get gateway -n strimzi

# Check the Envoy service (LoadBalancer IP)
kubectl get svc -n envoy-gateway-system

# Check certificate
kubectl get certificate -n strimzi

# Check Kafka services created by Strimzi
kubectl get svc -n strimzi | grep gateway

# Describe a specific TLSRoute for troubleshooting
kubectl describe tlsroute kafka-bootstrap -n strimzi
```

---

## Cleanup

```bash
# Delete Kafka resources
kubectl delete -f 07-user-and-topic.yaml
kubectl delete -f 06-tls-routes.yaml
kubectl delete -f 05-kafka.yaml
kubectl delete -f 04-kafka-node-pools.yaml
kubectl delete -f 03-certificate.yaml
kubectl delete -f 02-gateway.yaml
kubectl delete -f 01-gateway-class.yaml

# Uninstall Helm releases
helm uninstall strimzi-kafka-operator -n strimzi
helm uninstall cert-manager -n cert-manager
helm uninstall envoy-gateway -n envoy-gateway-system

# Delete namespaces
kubectl delete namespace strimzi cert-manager envoy-gateway-system

# Remove Gateway API CRDs
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml
```
