# Architecture: Kafka on AKS with Kroxylicious L7 Proxy

## Overview

This deployment runs Apache Kafka 4.1.0 on Azure Kubernetes Service (AKS) using the Strimzi Operator for lifecycle management and Kroxylicious as an L7 Kafka protocol-aware proxy for external access. Kroxylicious replaces the previous Envoy Gateway L4 TLS passthrough approach.

---

## Network Flow

```
                                   EXTERNAL
                                   ────────
  ┌──────────────────┐
  │  Kafka Client    │    TLS + SCRAM-SHA-512
  │  (Producer /     │    SNI: bootstrap.kafka.aks.mydomain.com
  │   Consumer)      ├──────────────────────────────────────────┐
  └──────────────────┘                                          │
                                                                ▼
                                                     ┌─────────────────────┐
                                                     │  Azure Load Balancer│
                                                     │  4.158.213.42:9083  │
                                                     │  (Layer 4 TCP)      │
                                                     └────────┬────────────┘
                                                              │
                                   KUBERNETES CLUSTER         │
                                   ───────────────────        │
                                                              ▼
                                                     ┌─────────────────────┐
                                                     │  Kroxylicious Proxy │
                                                     │  (L7 Kafka-aware)   │
                                                     │                     │
                                                     │  1. Terminates TLS  │
                                                     │  2. Reads SNI       │
                                                     │  3. Decodes Kafka   │
                                                     │     protocol frames │
                                                     │  4. Rewrites broker │
                                                     │     metadata        │
                                                     │  5. Re-encrypts     │
                                                     │     to broker       │
                                                     └────────┬────────────┘
                                                              │
                                                 Internal TLS │ (Strimzi cluster CA)
                                                              │
                          ┌───────────────────────────────────┼───────────────────────┐
                          │                                   │                       │
                          ▼                                   ▼                       ▼
                 ┌─────────────────┐                ┌─────────────────┐     ┌─────────────────┐
                 │  Broker 10      │                │  Broker 11      │     │  Broker 12      │
                 │  10Gi PVC       │                │  10Gi PVC       │     │  10Gi PVC       │
                 │  (managed-csi-  │                │  (managed-csi-  │     │  (managed-csi-  │
                 │   retain)       │                │   retain)       │     │   retain)       │
                 └────────┬────────┘                └────────┬────────┘     └────────┬────────┘
                          │                                   │                       │
                          │            KRaft consensus         │                       │
                          └───────────────────┬───────────────┘───────────────────────┘
                                              │
                                     ┌────────┴────────┐
                                     │  Controller 0   │
                                     │  10Gi PVC       │
                                     │  (KRaft leader) │
                                     └─────────────────┘
```

---

## Component Deep Dive

### 1. Azure Load Balancer (Layer 4)

The Azure Load Balancer is the entry point for external traffic. It operates at Layer 4 (TCP) and forwards all traffic on port 9083 to the Kroxylicious proxy pod inside the cluster.

```
┌─────────────────────────────────────────────────────────────────┐
│  Azure Load Balancer                                            │
│                                                                 │
│  Public IP:  4.158.213.42                                       │
│  Port:       9083 (external) → 9291 (container target)         │
│  Protocol:   TCP                                                │
│  Type:       Standard SKU                                       │
│                                                                 │
│  The LB does NOT inspect TLS or Kafka protocol.                 │
│  It forwards raw TCP bytes to the Kroxylicious pod.             │
│  All intelligence (SNI, metadata rewriting) is in               │
│  Kroxylicious at L7.                                            │
└─────────────────────────────────────────────────────────────────┘
```

| Property | Value |
|----------|-------|
| **External IP** | 4.158.213.42 |
| **External Port** | 9083 |
| **Container Target Port** | 9291 |
| **Protocol** | TCP (no TLS inspection) |
| **Created by** | Kroxylicious Operator (via KafkaProxyIngress) |
| **Service name** | `external-lb` in `strimzi` namespace |

### 2. Kroxylicious Proxy (Layer 7)

Kroxylicious is the core of this architecture. Unlike Envoy Gateway (which operated at L4 with TLS passthrough), Kroxylicious understands the Kafka wire protocol and operates at L7.

```
┌─────────────────────────────────────────────────────────────────┐
│  Kroxylicious Proxy Pod                                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  TLS Termination                                        │    │
│  │                                                         │    │
│  │  Client sends TLS ClientHello with SNI hostname.        │    │
│  │  Kroxylicious presents the wildcard cert                │    │
│  │  (*.kafka.aks.mydomain.com) and completes handshake.    │    │
│  │                                                         │    │
│  │  The SNI hostname determines which broker the client    │    │
│  │  wants to reach:                                        │    │
│  │    bootstrap.kafka.aks.mydomain.com → bootstrap         │    │
│  │    broker-10.kafka.aks.mydomain.com → broker 10         │    │
│  │    broker-11.kafka.aks.mydomain.com → broker 11         │    │
│  │    broker-12.kafka.aks.mydomain.com → broker 12         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Kafka Protocol Decoder                                 │    │
│  │                                                         │    │
│  │  After TLS termination, Kroxylicious sees plaintext     │    │
│  │  Kafka protocol frames. It decodes the API key and      │    │
│  │  version of each request:                               │    │
│  │                                                         │    │
│  │    ApiKey  0 = Produce                                  │    │
│  │    ApiKey  1 = Fetch                                    │    │
│  │    ApiKey  3 = Metadata         ← intercepted           │    │
│  │    ApiKey 10 = FindCoordinator  ← intercepted           │    │
│  │    ApiKey 36 = SaslHandshake    ← passed through        │    │
│  │    ApiKey 60 = DescribeCluster  ← intercepted           │    │
│  │                                                         │    │
│  │  Most requests pass through unmodified. Metadata,       │    │
│  │  FindCoordinator, and DescribeCluster responses are     │    │
│  │  intercepted for address rewriting.                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Metadata Rewriting                                     │    │
│  │                                                         │    │
│  │  When a Kafka broker responds with Metadata, it         │    │
│  │  includes internal K8s addresses:                       │    │
│  │                                                         │    │
│  │  BEFORE (broker response):                              │    │
│  │    broker-10 → gateway-api-kafka-broker-10.gateway-     │    │
│  │                api-kafka-kafka-brokers.strimzi.svc:9093 │    │
│  │    broker-11 → gateway-api-kafka-broker-11.gateway-     │    │
│  │                api-kafka-kafka-brokers.strimzi.svc:9093 │    │
│  │    broker-12 → gateway-api-kafka-broker-12.gateway-     │    │
│  │                api-kafka-kafka-brokers.strimzi.svc:9093 │    │
│  │                                                         │    │
│  │  AFTER (rewritten by Kroxylicious):                     │    │
│  │    broker-10 → broker-10.kafka.aks.mydomain.com:9083    │    │
│  │    broker-11 → broker-11.kafka.aks.mydomain.com:9083    │    │
│  │    broker-12 → broker-12.kafka.aks.mydomain.com:9083    │    │
│  │                                                         │    │
│  │  This is automatic — no per-broker advertisedHost       │    │
│  │  configuration needed in the Kafka CR.                  │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Upstream TLS (proxy → broker)                          │    │
│  │                                                         │    │
│  │  Kroxylicious establishes a SEPARATE TLS session to     │    │
│  │  the Strimzi broker using the cluster CA certificate.   │    │
│  │                                                         │    │
│  │  Trust: auto-configured via trustStrimziCaCertificate   │    │
│  │  Secret: gateway-api-kafka-cluster-ca-cert              │    │
│  │  Path:   /opt/kroxylicious/target-cluster/trusted-      │    │
│  │          certs/gateway-api-kafka-cluster-ca-cert/ca.crt │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  Resources: 250m/512Mi request, 500m/768Mi limit                │
│  Metrics:   http://localhost:9190/metrics (Prometheus)           │
└─────────────────────────────────────────────────────────────────┘
```

| Property | Value |
|----------|-------|
| **Pod name** | `kafka-proxy-*` |
| **Image** | `quay.io/kroxylicious/proxy:0.25.0` |
| **Internal listen port** | 9291 (SNI gateway) |
| **Management port** | 9190 (Prometheus metrics, liveness) |
| **Downstream TLS cert** | cert-manager `kafka-certificate` Secret |
| **Upstream TLS trust** | Strimzi cluster CA (auto-mounted) |
| **CPU request/limit** | 250m / 500m |
| **Memory request/limit** | 512Mi / 768Mi |

### 3. SASL Authentication Flow

SCRAM-SHA-512 is a challenge-response protocol. Kroxylicious does NOT terminate SASL — it passes the frames transparently between client and broker.

```
  Client                    Kroxylicious                  Broker
    │                           │                            │
    │  TLS ClientHello (SNI)    │                            │
    ├──────────────────────────►│                            │
    │  TLS ServerHello + Cert   │                            │
    │◄──────────────────────────┤                            │
    │  TLS Finished             │                            │
    ├──────────────────────────►│                            │
    │                           │  TLS ClientHello           │
    │    (TLS session 1)        ├───────────────────────────►│
    │                           │  TLS ServerHello + Cert    │
    │                           │◄───────────────────────────┤
    │                           │  TLS Finished              │
    │                           ├───────────────────────────►│
    │                           │      (TLS session 2)       │
    │                           │                            │
    │  SaslHandshake            │  SaslHandshake             │
    ├──────────────────────────►├───────────────────────────►│
    │  SaslHandshake Response   │  SaslHandshake Response    │
    │◄──────────────────────────┤◄───────────────────────────┤
    │                           │                            │
    │  SaslAuthenticate         │  SaslAuthenticate          │
    │  (SCRAM client-first)     │  (forwarded as-is)         │
    ├──────────────────────────►├───────────────────────────►│
    │  SaslAuthenticate         │  SaslAuthenticate          │
    │  (server-first)           │  (forwarded as-is)         │
    │◄──────────────────────────┤◄───────────────────────────┤
    │                           │                            │
    │  SaslAuthenticate         │  SaslAuthenticate          │
    │  (client-final)           │  (forwarded as-is)         │
    ├──────────────────────────►├───────────────────────────►│
    │  SaslAuthenticate         │  SaslAuthenticate          │
    │  (server-final: OK)       │  (forwarded as-is)         │
    │◄──────────────────────────┤◄───────────────────────────┤
    │                           │                            │
    │  Metadata Request         │  Metadata Request          │
    ├──────────────────────────►├───────────────────────────►│
    │  Metadata Response        │  Metadata Response         │
    │  (REWRITTEN addresses)    │◄───────────────────────────┤
    │◄──────────────────────────┤  (original internal addrs) │
    │                           │                            │
```

Key points:
- **Two separate TLS sessions** — client↔proxy uses cert-manager cert, proxy↔broker uses Strimzi CA
- **SASL flows end-to-end** — the password hash is verified by the broker, not the proxy
- **Metadata is the only rewritten response** — all other requests/responses are forwarded as-is
- **The proxy never sees the plaintext password** — SCRAM-SHA-512 sends salted hashes

### 4. Strimzi Kafka Cluster

The Kafka cluster runs in KRaft mode (no ZooKeeper) managed by the Strimzi Operator.

```
┌─────────────────────────────────────────────────────────────────┐
│  Strimzi Kafka Cluster: gateway-api-kafka                       │
│  Namespace: strimzi                                             │
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Broker 10   │  │ Broker 11   │  │ Broker 12   │             │
│  │             │  │             │  │             │             │
│  │ Roles:      │  │ Roles:      │  │ Roles:      │             │
│  │  - broker   │  │  - broker   │  │  - broker   │             │
│  │             │  │             │  │             │             │
│  │ Listener:   │  │ Listener:   │  │ Listener:   │             │
│  │  tls:9093   │  │  tls:9093   │  │  tls:9093   │             │
│  │  SCRAM-512  │  │  SCRAM-512  │  │  SCRAM-512  │             │
│  │             │  │             │  │             │             │
│  │ PVC: 10Gi   │  │ PVC: 10Gi   │  │ PVC: 10Gi   │             │
│  │ managed-csi │  │ managed-csi │  │ managed-csi │             │
│  │ -retain     │  │ -retain     │  │ -retain     │             │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
│         │                │                │                     │
│         └────────┬───────┘────────┬───────┘                     │
│                  │  KRaft Raft    │                              │
│           ┌──────┴──────┐        │                              │
│           │ Controller 0│        │                              │
│           │             │        │                              │
│           │ Roles:      │        │                              │
│           │  - controller        │                              │
│           │             │        │                              │
│           │ PVC: 10Gi   │        │                              │
│           │ managed-csi │        │                              │
│           │ -retain     │        │                              │
│           └─────────────┘        │                              │
│                                  │                              │
│  ┌───────────────────────────────┴────────────────────────┐     │
│  │  Entity Operator                                       │     │
│  │  - Topic Operator:  manages KafkaTopic resources       │     │
│  │  - User Operator:   manages KafkaUser resources        │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                 │
│  Services:                                                      │
│    gateway-api-kafka-kafka-bootstrap (ClusterIP, 9091/9093)     │
│    gateway-api-kafka-kafka-brokers   (Headless, 9090/9091/9093) │
└─────────────────────────────────────────────────────────────────┘
```

| Property | Value |
|----------|-------|
| **Kafka version** | 4.1.0 |
| **Mode** | KRaft (no ZooKeeper) |
| **Brokers** | 3 (IDs: 10, 11, 12) |
| **Controllers** | 1 (ID: 0) |
| **Listener name** | `tls` |
| **Listener type** | `internal` |
| **Listener port** | 9093 |
| **Authentication** | SCRAM-SHA-512 |
| **Authorization** | Simple ACLs |
| **default.replication.factor** | 3 |
| **min.insync.replicas** | 2 |
| **Storage per pod** | 10Gi Azure Managed Disk |
| **StorageClass** | `managed-csi-retain` (Retain policy) |

### 5. Kroxylicious Operator CRDs

The Kroxylicious Operator manages the proxy lifecycle through 5 Custom Resource Definitions.

```
┌─────────────────────────────────────────────────────────────────┐
│  Kroxylicious Operator (namespace: kroxylicious-operator)       │
│                                                                 │
│  Watches CRDs in all namespaces, generates:                     │
│    - ConfigMap (proxy-config.yaml)                              │
│    - Deployment (proxy pods)                                    │
│    - Service (LoadBalancer / ClusterIP)                         │
│                                                                 │
│  ┌──────────────────────┐       ┌──────────────────────┐        │
│  │  KafkaProxy          │       │  KafkaProxyIngress   │        │
│  │  name: kafka-proxy   │◄──────┤  name: external-lb   │        │
│  │                      │  ref  │                      │        │
│  │  Resources:          │       │  Type: loadBalancer   │        │
│  │   cpu: 250m-500m     │       │  Bootstrap:           │        │
│  │   mem: 512Mi-768Mi   │       │   bootstrap.kafka.    │        │
│  └──────────┬───────────┘       │   aks.mydomain.com   │        │
│             │                   │  Broker pattern:      │        │
│             │                   │   broker-$(nodeId).   │        │
│             │                   │   kafka.aks.          │        │
│             │                   │   mydomain.com        │        │
│             │                   └──────────┬───────────┘        │
│             │                              │                    │
│             ▼                              ▼                    │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  VirtualKafkaCluster                                 │       │
│  │  name: gateway-api-kafka                             │       │
│  │                                                      │       │
│  │  References:                                         │       │
│  │   proxyRef        → kafka-proxy                      │       │
│  │   targetKafkaRef  → kafka-backend (KafkaService)     │       │
│  │   ingressRef      → external-lb (KafkaProxyIngress)  │       │
│  │   certificateRef  → kafka-certificate (Secret)       │       │
│  │                                                      │       │
│  │  Status:                                             │       │
│  │   ResolvedRefs: True                                 │       │
│  │   Accepted: True                                     │       │
│  │   bootstrapServer: bootstrap.kafka.aks.              │       │
│  │                    mydomain.com:9083                  │       │
│  └──────────────────────────┬───────────────────────────┘       │
│                             │                                   │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────┐       │
│  │  KafkaService                                        │       │
│  │  name: kafka-backend                                 │       │
│  │                                                      │       │
│  │  strimziKafkaRef:                                    │       │
│  │    name: gateway-api-kafka                           │       │
│  │    listenerName: tls                                 │       │
│  │    trustStrimziCaCertificate: true                   │       │
│  │                                                      │       │
│  │  nodeIdRanges:                                       │       │
│  │    brokers: 10-12                                    │       │
│  │                                                      │       │
│  │  Status (auto-resolved):                             │       │
│  │    bootstrapServers: gateway-api-kafka-kafka-        │       │
│  │      bootstrap.strimzi.svc:9093                      │       │
│  │    tls.trustAnchorRef:                               │       │
│  │      gateway-api-kafka-cluster-ca-cert/ca.crt        │       │
│  └──────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

| CRD | Name | Purpose |
|-----|------|---------|
| **KafkaProxy** | `kafka-proxy` | Proxy instance with resource limits and network tuning |
| **KafkaProxyIngress** | `external-lb` | LoadBalancer exposure with SNI routing pattern |
| **KafkaService** | `kafka-backend` | Backend Strimzi Kafka cluster reference with auto CA trust |
| **VirtualKafkaCluster** | `gateway-api-kafka` | Ties proxy + ingress + service + TLS cert together |
| **KafkaProtocolFilter** | (not used) | Optional: Record Encryption, Validation, Multi-tenancy |

### 6. TLS Certificate Chain

Two independent TLS sessions exist — the proxy bridges them.

```
  CLIENT SIDE                     PROXY                      BROKER SIDE
  ──────────                     ─────                       ───────────

  ┌────────────────┐     ┌────────────────────────┐     ┌────────────────┐
  │ Client trusts: │     │ Downstream cert:       │     │ Broker cert:   │
  │                │     │                        │     │                │
  │ cert-manager   │     │ *.kafka.aks.           │     │ Strimzi auto-  │
  │ self-signed CA │     │ mydomain.com           │     │ generated cert │
  │                │     │                        │     │                │
  │ (ca.crt from   │     │ Issued by:             │     │ Signed by:     │
  │  kafka-        │     │ kafka-selfsigned-issuer │     │ cluster CA     │
  │  certificate   │     │                        │     │                │
  │  Secret)       │     │ Secret:                │     │ Secret:        │
  │                │     │ kafka-certificate      │     │ gateway-api-   │
  │                │     │ (tls.crt + tls.key)    │     │ kafka-cluster- │
  │                │     │                        │     │ ca-cert        │
  └────────────────┘     │ Upstream trust:        │     └────────────────┘
                         │                        │
                         │ Strimzi cluster CA     │
                         │ (auto-mounted via      │
                         │ trustStrimziCa-        │
                         │ Certificate: true)     │
                         └────────────────────────┘

  Session 1: Client ↔ Proxy         Session 2: Proxy ↔ Broker
  Cert: cert-manager wildcard       Cert: Strimzi auto-generated
  CA:   self-signed                  CA:   Strimzi cluster CA
  Port: 9083 (via Azure LB)         Port: 9093 (internal)
```

| Leg | Certificate | Issuer | Secret |
|-----|-------------|--------|--------|
| **Client → Proxy** | `*.kafka.aks.mydomain.com` | `kafka-selfsigned-issuer` (cert-manager) | `kafka-certificate` |
| **Proxy → Broker** | Auto-generated per broker | Strimzi cluster CA | `gateway-api-kafka-cluster-ca-cert` |

### 7. Storage Architecture

```
  ┌──────────────────────────────────────────────────────────────┐
  │  StorageClass: managed-csi-retain                            │
  │                                                              │
  │  Provisioner:     disk.csi.azure.com                         │
  │  Reclaim Policy:  Retain                                     │
  │  Volume Binding:  WaitForFirstConsumer                       │
  │  Disk Type:       StandardSSD_LRS                            │
  │  Expansion:       Enabled                                    │
  │                                                              │
  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │
  │  │ PVC: 10Gi    │ │ PVC: 10Gi    │ │ PVC: 10Gi    │         │
  │  │ Broker 10    │ │ Broker 11    │ │ Broker 12    │         │
  │  │              │ │              │ │              │         │
  │  │ Azure Disk   │ │ Azure Disk   │ │ Azure Disk   │         │
  │  │ (StandardSSD)│ │ (StandardSSD)│ │ (StandardSSD)│         │
  │  └──────────────┘ └──────────────┘ └──────────────┘         │
  │                                                              │
  │  ┌──────────────┐                                            │
  │  │ PVC: 10Gi    │  Retain policy means:                      │
  │  │ Controller 0 │  - PV survives PVC deletion                │
  │  │              │  - Data preserved if pod/namespace deleted  │
  │  │ Azure Disk   │  - Manual cleanup required to free disk    │
  │  │ (StandardSSD)│                                            │
  │  └──────────────┘  WaitForFirstConsumer means:               │
  │                    - Disk created in same AZ as the pod      │
  │                    - Avoids cross-zone mount failures         │
  └──────────────────────────────────────────────────────────────┘
```

| Property | Value |
|----------|-------|
| **StorageClass** | `managed-csi-retain` |
| **Total PVCs** | 4 (3 brokers + 1 controller) |
| **Total storage** | 40Gi |
| **Disk type** | Azure StandardSSD (managed disk) |
| **Reclaim policy** | Retain (survives deletion) |
| **Binding mode** | WaitForFirstConsumer (AZ-aware) |

### 8. Kafka Topics

```
  ┌──────────────────────────────────────────────────────────────┐
  │  Topic: test-topic                                           │
  │  Partitions: 12  │  Replicas: 3  │  min.insync.replicas: 2  │
  │                                                              │
  │  P0  [B10, B11, B12]    P4  [B10, B11, B12]                 │
  │  P1  [B11, B12, B10]    P5  [B11, B12, B10]                 │
  │  P2  [B12, B10, B11]    P6  [B12, B10, B11]                 │
  │  P3  [B10, B11, B12]    P7  [B10, B11, B12]                 │
  │              ...                    ...                      │
  │  P8  [B11, B12, B10]    P10 [B11, B12, B10]                 │
  │  P9  [B12, B10, B11]    P11 [B12, B10, B11]                 │
  │                                                              │
  │  retention.ms:    604800000 (7 days)                         │
  │  segment.ms:      86400000  (24 hours)                       │
  │  segment.bytes:   10485760  (10 MB)                          │
  └──────────────────────────────────────────────────────────────┘

  ┌──────────────────────────┐  ┌──────────────────────────┐
  │  Topic: events           │  │  Topic: orders           │
  │  Partitions: 12          │  │  Partitions: 12          │
  │  Replicas: 3             │  │  Replicas: 3             │
  │  min.insync.replicas: 2  │  │  min.insync.replicas: 2  │
  │  retention.ms: 604800000 │  │  retention.ms: 604800000 │
  │  segment.bytes: 1 GB     │  │  segment.bytes: 1 GB     │
  └──────────────────────────┘  └──────────────────────────┘
```

| Topic | Partitions | Replicas | min.insync | segment.bytes | retention |
|-------|-----------|----------|------------|---------------|-----------|
| **test-topic** | 12 | 3 | 2 | 10 MB | 7 days |
| **events** | 12 | 3 | 2 | 1 GB | 7 days |
| **orders** | 12 | 3 | 2 | 1 GB | 7 days |

### 9. Observability

Kroxylicious exposes Prometheus metrics at `:9190/metrics`.

```
  ┌─────────────────────────────────────────────────────────────┐
  │  Prometheus Metrics Endpoint                                 │
  │  http://kafka-proxy-pod:9190/metrics                         │
  │                                                              │
  │  Connection metrics:                                         │
  │    kroxylicious_client_to_proxy_connections_total             │
  │    kroxylicious_client_to_proxy_active_connections            │
  │    kroxylicious_proxy_to_server_connections_total             │
  │    kroxylicious_proxy_to_server_active_connections            │
  │    kroxylicious_client_to_proxy_disconnects_total             │
  │      labels: virtual_cluster, node_id, cause                 │
  │                                                              │
  │  Message metrics:                                            │
  │    kroxylicious_client_to_proxy_request_total                 │
  │    kroxylicious_proxy_to_client_response_total                │
  │    kroxylicious_client_to_proxy_request_size_bytes            │
  │      labels: virtual_cluster, node_id, api_key, api_version  │
  │                                                              │
  │  Virtual cluster lifecycle:                                  │
  │    kroxylicious_virtual_cluster_state                         │
  │      states: initializing → serving → draining → stopped     │
  └─────────────────────────────────────────────────────────────┘
```

| Metric | Type | Description |
|--------|------|-------------|
| `client_to_proxy_connections_total` | Counter | Client connection attempts per broker |
| `client_to_proxy_active_connections` | Gauge | Currently open client connections |
| `proxy_to_server_connections_total` | Counter | Proxy-to-broker connection attempts |
| `client_to_proxy_request_total` | Counter | Kafka API requests by api_key/version |
| `client_to_proxy_request_size_bytes` | Histogram | Request payload sizes |
| `virtual_cluster_state` | StateSet | Lifecycle state (serving, draining, etc.) |
| `client_to_proxy_disconnects_total` | Counter | Disconnects by cause (idle, client_closed, server_closed) |

---

## Namespace Layout

```
  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐
  │  strimzi             │  │  kroxylicious-       │  │  cert-manager       │
  │                      │  │  operator            │  │                     │
  │  - Kafka brokers     │  │                      │  │  - cert-manager     │
  │  - Controller        │  │  - kroxylicious-     │  │    controller       │
  │  - Entity operator   │  │    operator pod      │  │  - webhook          │
  │  - Strimzi operator  │  │                      │  │  - cainjector       │
  │  - Kroxylicious      │  │  Watches CRDs in     │  │                     │
  │    proxy pod         │  │  all namespaces      │  │  Manages:           │
  │  - LB Service        │  │                      │  │  - ClusterIssuer    │
  │  - KafkaProxy CRs    │  │                      │  │  - Certificates     │
  │  - Certificate       │  │                      │  │                     │
  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘
```

| Namespace | Components |
|-----------|-----------|
| **strimzi** | Kafka cluster, Entity Operator, Strimzi Operator, Kroxylicious proxy pod, LB Service, all Kroxylicious CRs, TLS Certificate |
| **kroxylicious-operator** | Kroxylicious Operator pod (watches all namespaces) |
| **cert-manager** | cert-manager controller, webhook, CA injector |
