# Migration: Envoy Gateway (L4) → Kroxylicious (L7)

## Architecture Change

```
BEFORE (L4 TLS Passthrough):
  Client → Azure LB:9092 → Envoy Gateway → [SNI routing] → TLSRoute → Kafka ClusterIP → Broker
  (Envoy never decrypts; 4 TLSRoutes needed; per-broker advertisedHost in Kafka CR)

AFTER (L7 Kafka Protocol Proxy):
  Client → Azure LB:9083 → Kroxylicious → [Kafka protocol aware] → Strimzi Kafka Broker
  (Kroxylicious terminates TLS, rewrites metadata automatically, native Strimzi integration)
```

## Infrastructure Status

| Component | Status | Details |
|-----------|--------|---------|
| **AKS Nodes** | 2 nodes Ready | Standard_D2s_v3, K8s v1.35.0 |
| **Cert-Manager** | v1.16.2 | Self-signed wildcard cert `*.kafka.aks.mydomain.com` |
| **Strimzi Operator** | v0.51.0 | Kafka 4.1.0 KRaft mode |
| **Kroxylicious Operator** | v0.25.0 | L7 Kafka protocol proxy |
| **Kroxylicious Proxy** | 1/1 Running | Serving virtual cluster `gateway-api-kafka` |
| **Azure LoadBalancer** | `4.158.213.42:9083` | SNI-based routing via Kroxylicious |

## Kroxylicious Proxy Resources

| Resource | Name | Config |
|----------|------|--------|
| **KafkaProxy** | `kafka-proxy` | 250m/512Mi request, 500m/768Mi limit |
| **KafkaProxyIngress** | `external-lb` | LoadBalancer with SNI routing |
| **KafkaService** | `kafka-backend` | `strimziKafkaRef` → `gateway-api-kafka`, listener `tls`, auto CA trust |
| **VirtualKafkaCluster** | `gateway-api-kafka` | TLS cert from `kafka-certificate` Secret |

## External Access Endpoints

| Endpoint | Address |
|----------|---------|
| **Bootstrap** | `bootstrap.kafka.aks.mydomain.com:9083` |
| **Broker 10** | `broker-10.kafka.aks.mydomain.com:9083` |
| **Broker 11** | `broker-11.kafka.aks.mydomain.com:9083` |
| **Broker 12** | `broker-12.kafka.aks.mydomain.com:9083` |

## Key Differences: Old vs New

| Aspect | Old (Envoy Gateway) | New (Kroxylicious) |
|--------|---------------------|-------------------|
| **Proxy layer** | Envoy Gateway (L4 TCP) | Kroxylicious (L7 Kafka-aware) |
| **TLS handling** | Passthrough — broker terminates TLS | Terminated at proxy, re-encrypted to broker |
| **Metadata rewriting** | Manual `advertisedHost` per broker in Kafka CR | Automatic — Kroxylicious intercepts Metadata/DescribeCluster/FindCoordinator responses |
| **Routing mechanism** | 4 TLSRoutes (bootstrap + 3 brokers) | Single VirtualKafkaCluster — no per-broker routes |
| **Strimzi integration** | None — manual ClusterIP services | Native `strimziKafkaRef` — auto-discovers bootstrap and trusts Strimzi CA |
| **Kafka listener type** | `cluster-ip` with per-broker advertisedHost config | Simple `internal` TLS — no per-broker config needed |
| **External port** | 9092 | 9083 (assigned by operator) |
| **StorageClass** | `managed-csi` (Delete policy) | `managed-csi-retain` (Retain policy) |
| **Protocol visibility** | None — encrypted passthrough | Full L7 — proxy decodes Kafka protocol frames |
| **Metrics** | Envoy TCP connection metrics only | Kafka-specific: per-broker connections, request/response counts, API key breakdown |

## Kubernetes Resources Comparison

| Category | Old Resources | New Resources |
|----------|--------------|---------------|
| **Proxy infrastructure** | GatewayClass, EnvoyProxy, Gateway | KafkaProxy |
| **Network exposure** | 4× TLSRoute (bootstrap + broker-10/11/12) | 1× KafkaProxyIngress (loadBalancer) |
| **Backend reference** | N/A (hardcoded in Kafka CR advertisedHost) | 1× KafkaService (strimziKafkaRef) |
| **Virtual cluster** | N/A | 1× VirtualKafkaCluster |
| **CRDs installed** | Gateway API CRDs (10 CRDs) | Kroxylicious CRDs (5 CRDs) |
| **Operator namespace** | `envoy-gateway-system` | `kroxylicious-operator` |
| **Total K8s objects** | 7 (GatewayClass + EnvoyProxy + Gateway + 4 TLSRoutes) | 4 (KafkaProxy + Ingress + Service + VirtualCluster) |

## Kafka Cluster Configuration Changes

| Setting | Old | New |
|---------|-----|-----|
| **Listener name** | `gateway` | `tls` |
| **Listener type** | `cluster-ip` | `internal` |
| **Listener port** | 9092 | 9093 |
| **Per-broker advertisedHost** | Yes (broker-10/11/12.kafka.aks.mydomain.com) | No — Kroxylicious rewrites automatically |
| **brokerCertChainAndKey** | cert-manager `kafka-certificate` Secret | Strimzi auto-generated cluster CA |
| **default.replication.factor** | Not set | 3 |
| **min.insync.replicas** (cluster) | Not set | 2 |
| **offsets.topic.replication.factor** | Not set | 3 |
| **transaction.state.log.replication.factor** | Not set | 3 |
| **StorageClass** | `managed-csi` | `managed-csi-retain` |

## Topic Configuration

| Setting | Old | New |
|---------|-----|-----|
| **retention.ms** | Default (168h) | `604800000` (7 days) |
| **min.insync.replicas** | Default (1) | `2` (tolerates 1 broker failure) |
| **segment.ms** | Default | `86400000` (24 hours) |
| **segment.bytes** | Default | `10485760` (10 MB) for `test-topic`, `1073741824` (1 GB) for `events`/`orders` |
| **Topics created** | `test-topic` only | `test-topic`, `events`, `orders` |

## StorageClass Comparison

| Property | Old (`managed-csi`) | New (`managed-csi-retain`) |
|----------|--------------------|-----------------------------|
| **Provisioner** | disk.csi.azure.com | disk.csi.azure.com |
| **Reclaim Policy** | Delete | Retain |
| **Volume Binding** | WaitForFirstConsumer | WaitForFirstConsumer |
| **Volume Expansion** | Yes | Yes |
| **Disk Type** | StandardSSD_LRS | StandardSSD_LRS |

## TLS Certificate Flow

| Leg | Old (Envoy) | New (Kroxylicious) |
|-----|-------------|-------------------|
| **Client → Proxy** | N/A (passthrough) | cert-manager self-signed wildcard `*.kafka.aks.mydomain.com` |
| **Proxy → Broker** | N/A (passthrough) | Strimzi cluster CA (auto-trusted via `trustStrimziCaCertificate: true`) |
| **Client → Broker** | Direct TLS — cert-manager cert on broker | Two-hop: client ↔ proxy TLS + proxy ↔ broker TLS |

## Proxy Metrics (L7 Visibility)

| Metric | Description |
|--------|-------------|
| `kroxylicious_client_to_proxy_connections_total` | Connection attempts per broker (bootstrap, 10, 11, 12) |
| `kroxylicious_client_to_proxy_active_connections` | Current open client connections per broker |
| `kroxylicious_proxy_to_server_connections_total` | Proxy-to-broker connection attempts |
| `kroxylicious_client_to_proxy_request_total` | Kafka API requests by `api_key` and `api_version` |
| `kroxylicious_proxy_to_client_response_total` | Kafka API responses by `api_key` and `api_version` |
| `kroxylicious_client_to_proxy_request_size_bytes` | Request payload sizes |
| `kroxylicious_virtual_cluster_state` | Virtual cluster lifecycle: initializing → serving → draining → stopped |

## Test Results After Migration

| Test | Result | Details |
|------|--------|---------|
| **Producer** | 20 sent, 20 delivered, 0 failed | Partitions: 2, 3, 5, 7, 8, 10 |
| **Consumer** | 40 messages consumed from beginning | 20 old (Envoy era) + 20 new (Kroxylicious) |
| **Proxy connections** | bootstrap=2, broker-10=2, broker-11=2, broker-12=3 | All brokers reachable through proxy |

## File Structure

| File | Purpose |
|------|---------|
| `aks-kroxylicious/00-install-prerequisites.sh` | Cert-Manager 1.16.2 + Strimzi 0.51.0 |
| `aks-kroxylicious/01-install-kroxylicious.sh` | Kroxylicious Operator v0.25.0 |
| `aks-kroxylicious/02-storage-class.yaml` | `managed-csi-retain` (Retain, WaitForFirstConsumer) |
| `aks-kroxylicious/03-certificate.yaml` | Self-signed ClusterIssuer + wildcard cert |
| `aks-kroxylicious/04-kafka-node-pools.yaml` | 1 controller + 3 brokers with `managed-csi-retain` |
| `aks-kroxylicious/05-kafka.yaml` | Kafka 4.1.0 KRaft with internal TLS + SCRAM listener |
| `aks-kroxylicious/06-user-and-topics.yaml` | KafkaUser + 3 topics with retention/segment config |
| `aks-kroxylicious/07-kroxylicious.yaml` | KafkaProxy + KafkaProxyIngress + KafkaService + VirtualKafkaCluster |
| `aks-kroxylicious/cleanup-envoy.sh` | Removes old Envoy Gateway/TLSRoute/GatewayClass resources |
| `aks-kroxylicious/deploy-all.sh` | Full orchestrated deployment pipeline |
| `setup-credentials-kroxy.sh` | Extracts credentials for Python scripts and CLI |

## Deployment Commands

```bash
# 1. Clean up old Envoy Gateway resources
cd aks-kroxylicious
bash cleanup-envoy.sh

# 2. Deploy full stack
bash deploy-all.sh

# 3. Generate client credentials
cd ..
bash setup-credentials-kroxy.sh

# 4. Update /etc/hosts
sudo sed -i '/kafka.aks.mydomain.com/d' /etc/hosts
echo "4.158.213.42  bootstrap.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
echo "4.158.213.42  broker-10.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
echo "4.158.213.42  broker-11.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts
echo "4.158.213.42  broker-12.kafka.aks.mydomain.com" | sudo tee -a /etc/hosts

# 5. Test
python3 producer.py --count 20
python3 consumer.py --from-beginning
```
