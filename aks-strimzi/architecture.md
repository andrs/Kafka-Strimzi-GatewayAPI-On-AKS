# Architecture: Kafka on AKS with Strimzi & Gateway API

## High-Level Overview

This architecture exposes Apache Kafka running inside AKS to external clients using the Kubernetes Gateway API with TLS passthrough. Instead of giving each Kafka broker its own LoadBalancer (expensive) or using NodePort (fragile), we route all traffic through a **single Azure Load Balancer** and use **SNI-based routing** at the Envoy Gateway to direct each connection to the correct broker.

```
                         ┌─────────────────────────────────────────────┐
                         │              External Client                │
                         │  (kafka-console-producer, application, etc) │
                         └──────────────────┬──────────────────────────┘
                                            │
                              TLS ClientHello with SNI:
                              "broker-10.kafka.aks.mydomain.com"
                                            │
                                            ▼
                    ┌───────────────────────────────────────────┐
                    │         Azure Load Balancer (L4)          │
                    │         Public IP: 85.210.67.170          │
                    │              Port: 9092                   │
                    └──────────────────┬────────────────────────┘
                                       │
                           ┌───────────┴───────────┐
                           ▼                       ▼
                  ┌─────────────────┐    ┌─────────────────┐
                  │  Envoy Proxy    │    │  Envoy Proxy    │
                  │  Replica 1      │    │  Replica 2      │
                  │  (Node 0)       │    │  (Node 1)       │
                  └────────┬────────┘    └────────┬────────┘
                           │                      │
                           │  SNI Inspection       │
                           │  (no TLS termination) │
                           │                      │
              ┌────────────┼──────────────────────┼────────────┐
              │            ▼                      ▼            │
              │  ┌──────────────────────────────────────────┐  │
              │  │           TLSRoute Rules (SNI match)     │  │
              │  │                                          │  │
              │  │  bootstrap.kafka.aks.mydomain.com        │  │
              │  │    → gateway-api-kafka-kafka-gateway-    │  │
              │  │      bootstrap:9092                      │  │
              │  │                                          │  │
              │  │  broker-10.kafka.aks.mydomain.com        │  │
              │  │    → gateway-api-kafka-broker-gateway-   │  │
              │  │      10:9092                             │  │
              │  │                                          │  │
              │  │  broker-11.kafka.aks.mydomain.com        │  │
              │  │    → gateway-api-kafka-broker-gateway-   │  │
              │  │      11:9092                             │  │
              │  │                                          │  │
              │  │  broker-12.kafka.aks.mydomain.com        │  │
              │  │    → gateway-api-kafka-broker-gateway-   │  │
              │  │      12:9092                             │  │
              │  └──────────────────────────────────────────┘  │
              │                                                │
              │            Kubernetes Cluster (AKS)            │
              │                                                │
              │  ┌──────────────────────────────────────────┐  │
              │  │         ClusterIP Services                │  │
              │  │                                          │  │
              │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │  │
              │  │  │Broker 10 │ │Broker 11 │ │Broker 12 │ │  │
              │  │  │ :9092    │ │ :9092    │ │ :9092    │ │  │
              │  │  └─────┬────┘ └─────┬────┘ └─────┬────┘ │  │
              │  └────────┼────────────┼────────────┼──────┘  │
              │           ▼            ▼            ▼         │
              │  ┌──────────────────────────────────────────┐  │
              │  │           Kafka Broker Pods               │  │
              │  │                                          │  │
              │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ │  │
              │  │  │broker-10 │ │broker-11 │ │broker-12 │ │  │
              │  │  │ KRaft    │ │ KRaft    │ │ KRaft    │ │  │
              │  │  │ 10Gi PVC │ │ 10Gi PVC │ │ 10Gi PVC │ │  │
              │  │  └──────────┘ └──────────┘ └──────────┘ │  │
              │  │                                          │  │
              │  │  ┌──────────────┐                        │  │
              │  │  │controller-0  │                        │  │
              │  │  │ KRaft        │                        │  │
              │  │  │ 10Gi PVC     │                        │  │
              │  │  └──────────────┘                        │  │
              │  └──────────────────────────────────────────┘  │
              └────────────────────────────────────────────────┘
```

---

## The Kafka Client Connection Problem

Kafka uses a unique connection protocol that makes external exposure difficult:

1. **Client connects to bootstrap address** — e.g., `bootstrap.kafka.aks.mydomain.com:9092`
2. **Kafka returns metadata** — a list of all brokers with their **advertised addresses**
3. **Client disconnects from bootstrap** and **reconnects directly to the assigned broker** — e.g., `broker-11.kafka.aks.mydomain.com:9092`

This means every broker needs its own unique, externally reachable address. Traditional approaches each have drawbacks:

| Approach | Problem |
|----------|---------|
| One LoadBalancer per broker | Expensive — 4 Azure LBs (1 bootstrap + 3 brokers) at ~$18/month each |
| NodePort per broker | Fragile — depends on node IPs, port collisions, no HA |
| Single Ingress | HTTP-only — Kafka uses a binary TCP protocol, not HTTP |

### The Gateway API Solution

The Gateway API with TLS passthrough solves this by using **SNI (Server Name Indication)** — a field in the TLS handshake that contains the hostname the client wants to reach. Envoy inspects this field **without decrypting** the traffic and routes to the correct backend.

**Result:** One Azure Load Balancer, one IP, one port — but each broker is individually addressable via its hostname.

---

## Component Deep Dive

### Layer 1: Azure Load Balancer

```
Azure Load Balancer (L4)
├── Frontend IP: 85.210.67.170
├── Frontend Port: 9092
├── Backend Pool: Envoy Proxy pods (2 replicas)
├── Health Probe: TCP on port 9092
└── Distribution: 5-tuple hash (src IP, src port, dst IP, dst port, protocol)
```

The Azure LB is automatically provisioned by AKS when Envoy Gateway creates a `Service` of type `LoadBalancer`. It operates at Layer 4 (TCP) — it does not inspect TLS or application content. It simply forwards TCP connections to one of the Envoy proxy pods.

**Why LoadBalancer instead of NodePort (used in the KIND tutorial):**
- NodePort requires knowing node IPs (which change on scale events)
- NodePort range is limited (30000–32767), not port 9092
- LoadBalancer provides a stable public IP with built-in health checks
- Azure LB integrates with AKS networking (Azure CNI) natively

### Layer 2: Envoy Gateway (Control Plane + Data Plane)

The Envoy Gateway consists of two parts:

**Control Plane** (`envoy-gateway` pod in `envoy-gateway-system`):
- Watches Kubernetes Gateway API resources (GatewayClass, Gateway, TLSRoute)
- Translates them into Envoy xDS configuration
- Pushes config to the data plane via gRPC

**Data Plane** (`envoy-strimzi-kafka-gateway-*` pods in `envoy-gateway-system`):
- 2 Envoy proxy replicas (for high availability)
- Spread across both AKS nodes
- Performs SNI-based routing without TLS termination

```
                    Envoy Proxy (Data Plane)
                    ┌─────────────────────────────┐
                    │                             │
  TCP connection    │  1. Accept TCP connection   │
  arrives on 9092 ──┤  2. Read TLS ClientHello    │
                    │  3. Extract SNI hostname    │
                    │  4. Match against TLSRoutes │
                    │  5. Proxy TCP stream to     │
                    │     matching ClusterIP svc  │
                    │                             │
                    │  (NO TLS termination —      │
                    │   encrypted bytes pass      │
                    │   through unchanged)        │
                    └─────────────────────────────┘
```

**Key configuration — `EnvoyProxy` resource:**

```yaml
spec:
  provider:
    type: Kubernetes
    kubernetes:
      envoyDeployment:
        replicas: 2              # HA across nodes
      envoyService:
        type: LoadBalancer       # Azure LB (not NodePort)
        patch:
          value:
            spec:
              ports:
              - name: kafka-port
                port: 9092       # External port
                protocol: TCP
                targetPort: 9092
```

### Layer 3: Gateway + TLSRoutes (Routing Rules)

**Gateway** — declares a listener on port 9092 with TLS Passthrough:

```yaml
spec:
  gatewayClassName: kafka-gateway-class
  listeners:
  - name: kafka-listener
    protocol: TLS
    port: 9092
    tls:
      mode: Passthrough    # Do NOT terminate TLS — just inspect SNI
```

**TLSRoutes** — match SNI hostnames to backend services:

```
┌───────────────────────────────────────────────────────────────────────┐
│                        TLSRoute Routing Table                        │
├─────────────────────────────────────┬─────────────────────────────────┤
│ SNI Hostname                        │ Backend ClusterIP Service       │
├─────────────────────────────────────┼─────────────────────────────────┤
│ bootstrap.kafka.aks.mydomain.com    │ gateway-api-kafka-kafka-        │
│                                     │   gateway-bootstrap:9092        │
├─────────────────────────────────────┼─────────────────────────────────┤
│ broker-10.kafka.aks.mydomain.com    │ gateway-api-kafka-broker-       │
│                                     │   gateway-10:9092               │
├─────────────────────────────────────┼─────────────────────────────────┤
│ broker-11.kafka.aks.mydomain.com    │ gateway-api-kafka-broker-       │
│                                     │   gateway-11:9092               │
├─────────────────────────────────────┼─────────────────────────────────┤
│ broker-12.kafka.aks.mydomain.com    │ gateway-api-kafka-broker-       │
│                                     │   gateway-12:9092               │
└─────────────────────────────────────┴─────────────────────────────────┘
```

Each TLSRoute is a simple rule: "if the client's TLS handshake says it wants hostname X, send the connection to service Y."

### Layer 4: Strimzi ClusterIP Services

Strimzi creates these services automatically when the Kafka listener is configured with `type: cluster-ip`:

```
gateway-api-kafka-kafka-gateway-bootstrap   ClusterIP   10.0.85.44     9092/TCP
    → Load-balances across all 3 broker pods (initial metadata request)

gateway-api-kafka-broker-gateway-10         ClusterIP   10.0.213.132   9092/TCP
    → Routes only to broker-10 pod

gateway-api-kafka-broker-gateway-11         ClusterIP   10.0.54.94     9092/TCP
    → Routes only to broker-11 pod

gateway-api-kafka-broker-gateway-12         ClusterIP   10.0.2.189     9092/TCP
    → Routes only to broker-12 pod
```

The **bootstrap** service is a standard Kubernetes service that load-balances across all brokers — it's used only for the initial metadata request. After that, clients connect to individual broker services.

The **per-broker** services are the critical piece — they ensure that when a client reconnects to `broker-10.kafka.aks.mydomain.com`, the connection reaches specifically broker-10, not a random broker.

### Layer 5: Kafka Cluster (KRaft Mode)

```
┌─────────────────────────────────────────────────────────┐
│                    Kafka Cluster                        │
│                   (KRaft — no ZooKeeper)                │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │            Controller Pool (1 replica)           │    │
│  │                                                 │    │
│  │  controller-0 (ID: 0)                           │    │
│  │    Role: KRaft controller                       │    │
│  │    Manages: metadata, partition assignments,    │    │
│  │             leader elections                    │    │
│  │    Storage: 10Gi Azure Managed Disk             │    │
│  │    No client traffic — internal only            │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │             Broker Pool (3 replicas)             │    │
│  │                                                 │    │
│  │  broker-10          broker-11          broker-12│    │
│  │    Role: data       Role: data        Role: data│    │
│  │    Storage: 10Gi    Storage: 10Gi     Storage:  │    │
│  │                                       10Gi      │    │
│  │    Advertised:      Advertised:       Advertised│    │
│  │    broker-10.       broker-11.        broker-12.│    │
│  │    kafka.aks.       kafka.aks.        kafka.aks.│    │
│  │    mydomain.com     mydomain.com      mydomain. │    │
│  │    :9092            :9092             com:9092   │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │            Entity Operator                       │    │
│  │                                                 │    │
│  │  ┌─────────────────┐  ┌──────────────────┐      │    │
│  │  │ Topic Operator  │  │  User Operator   │      │    │
│  │  │ Manages         │  │  Manages         │      │    │
│  │  │ KafkaTopic CRs  │  │  KafkaUser CRs   │      │    │
│  │  └─────────────────┘  └──────────────────┘      │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

**KRaft vs ZooKeeper:**
KRaft (Kafka Raft) replaces ZooKeeper as the metadata management layer. The controller node runs the Raft consensus protocol internally, eliminating the need for a separate ZooKeeper ensemble. This simplifies operations (fewer pods, fewer PVCs, no ZK tuning).

**Separation of roles:**
- **Controllers** handle metadata only — they don't store or serve topic data
- **Brokers** handle client connections and data storage
- Node IDs are deliberately separated: controllers get 0–9, brokers get 10–100

**The `advertisedHost` configuration is critical:**

```yaml
brokers:
- broker: 10
  advertisedHost: broker-10.kafka.aks.mydomain.com
  advertisedPort: 9092
```

This tells each broker: "When a client asks for metadata, tell them to reach me at `broker-10.kafka.aks.mydomain.com:9092`." Without this, Kafka would advertise its internal pod hostname (e.g., `gateway-api-kafka-broker-10.gateway-api-kafka-kafka-brokers.strimzi.svc`), which is unreachable from outside the cluster.

---

## Security Architecture

### TLS (Encryption in Transit)

```
Client ←──── TLS 1.2/1.3 ────→ Kafka Broker
              (end-to-end)

The Envoy proxy NEVER sees plaintext.
It only reads the unencrypted SNI field from the TLS ClientHello.
```

- **Certificate:** Self-signed wildcard `*.kafka.aks.mydomain.com`
- **Issued by:** cert-manager with a self-signed ClusterIssuer
- **Stored in:** Kubernetes Secret `kafka-certificate` (namespace: strimzi)
- **Used by:** Kafka brokers (configured via `brokerCertChainAndKey`)
- **Client trust:** Clients must import the CA cert into a truststore

### Authentication (SCRAM-SHA-512)

```
Client                              Kafka Broker
  │                                      │
  ├── SASL handshake (SCRAM-SHA-512) ───→│
  │                                      │
  │←── Server challenge ────────────────┤│
  │                                      │
  ├── Client proof (salted password) ───→│
  │                                      │
  │←── Server verification ─────────────┤│
  │                                      │
  │    ✓ Authenticated                   │
```

- **Mechanism:** SCRAM-SHA-512 (Salted Challenge Response Authentication Mechanism)
- **User:** `kafka-test-user` — credentials stored in Kubernetes Secret
- **Protocol:** `SASL_SSL` — SASL authentication over TLS
- SCRAM is preferred over PLAIN because the password is never sent over the wire (even though TLS encrypts the channel, defense in depth matters)

### Authorization (ACLs)

```yaml
authorization:
  type: simple    # Kafka's built-in ACL authorizer
```

The `kafka-test-user` has full access for testing. In production, restrict ACLs to specific topics, consumer groups, and operations.

---

## Network Flow: Complete Connection Lifecycle

```
Step 1: Bootstrap Connection
══════════════════════════════

Client                    Azure LB              Envoy               Bootstrap Svc         Broker-11
  │                         │                     │                      │                    │
  ├── TLS ClientHello ─────→│                     │                      │                    │
  │   SNI: bootstrap.       ├── TCP forward ─────→│                      │                    │
  │   kafka.aks.            │                     ├── SNI match ────────→│                    │
  │   mydomain.com          │                     │   "bootstrap..."     ├── forward ────────→│
  │                         │                     │                      │   (any broker)     │
  │←─── TLS ServerHello ───┤←────────────────────┤←─────────────────────┤←───────────────────┤
  │     (certificate)       │                     │                      │                    │
  │                         │                     │                      │                    │
  ├── SASL Auth ───────────→│────────────────────→│─────────────────────→│───────────────────→│
  │←── Auth OK ────────────┤←────────────────────┤←─────────────────────┤←───────────────────┤
  │                         │                     │                      │                    │
  ├── Metadata Request ────→│────────────────────→│─────────────────────→│───────────────────→│
  │←── Metadata Response ──┤←────────────────────┤←─────────────────────┤←───────────────────┤
  │   brokers:              │                     │                      │                    │
  │   - broker-10.kafka.aks.mydomain.com:9092                                                │
  │   - broker-11.kafka.aks.mydomain.com:9092                                                │
  │   - broker-12.kafka.aks.mydomain.com:9092                                                │
  │   topic "test-topic"                                                                     │
  │   partition 7 leader: broker-11                                                          │
  │                         │                     │                      │                    │
  ├── Close connection ────→│                     │                      │                    │


Step 2: Direct Broker Connection
═════════════════════════════════

Client                    Azure LB              Envoy               Broker-11 Svc         Broker-11
  │                         │                     │                      │                    │
  ├── TLS ClientHello ─────→│                     │                      │                    │
  │   SNI: broker-11.       ├── TCP forward ─────→│                      │                    │
  │   kafka.aks.            │                     ├── SNI match ────────→│                    │
  │   mydomain.com          │                     │   "broker-11..."     ├── forward ────────→│
  │                         │                     │                      │   (only broker-11) │
  │←── TLS + Auth ─────────┤←────────────────────┤←─────────────────────┤←───────────────────┤
  │                         │                     │                      │                    │
  ├── Produce to            │                     │                      │                    │
  │   test-topic:7 ────────→│────────────────────→│─────────────────────→│───────────────────→│
  │←── ACK ────────────────┤←────────────────────┤←─────────────────────┤←───────────────────┤
  │                         │                     │                      │                    │
  │   (persistent TCP connection — kept open for ongoing produce/consume)                    │
```

---

## Strimzi Operator Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Strimzi Cluster Operator                       │
│                                                                  │
│  Watches:                        Creates/Manages:                │
│  ├── Kafka CR                    ├── StatefulSets (brokers,      │
│  ├── KafkaNodePool CR            │   controllers)                │
│  ├── KafkaTopic CR               ├── Services (bootstrap,       │
│  └── KafkaUser CR                │   per-broker ClusterIP)       │
│                                  ├── ConfigMaps (broker config)  │
│                                  ├── Secrets (internal certs)    │
│                                  ├── PodDisruptionBudgets        │
│                                  └── NetworkPolicies             │
│                                                                  │
│  Reconciliation Loop:                                            │
│  1. Observe desired state (CRs)                                  │
│  2. Compare with actual state (K8s resources)                    │
│  3. Create/update/delete to converge                             │
│  4. Perform rolling updates when config changes                  │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                       Entity Operator                            │
│  (deployed per Kafka cluster, not globally)                      │
│                                                                  │
│  ┌────────────────────┐    ┌─────────────────────┐               │
│  │   Topic Operator   │    │    User Operator     │               │
│  │                    │    │                     │               │
│  │  Watches:          │    │  Watches:           │               │
│  │  KafkaTopic CRs    │    │  KafkaUser CRs      │               │
│  │                    │    │                     │               │
│  │  Manages:          │    │  Manages:           │               │
│  │  Topics inside     │    │  SCRAM credentials  │               │
│  │  Kafka via Admin   │    │  ACLs inside Kafka  │               │
│  │  API               │    │  Secrets in K8s     │               │
│  └────────────────────┘    └─────────────────────┘               │
└──────────────────────────────────────────────────────────────────┘
```

---

## Storage Architecture

```
┌───────────────────────────────────────────────┐
│              Azure Managed Disks               │
│              (StorageClass: managed-csi)        │
│                                                │
│  PVC: data-gateway-api-kafka-controller-0      │
│  ├── Size: 10Gi                                │
│  ├── Access: ReadWriteOnce                     │
│  ├── Provisioner: disk.csi.azure.com           │
│  └── Contains: KRaft metadata log              │
│                                                │
│  PVC: data-gateway-api-kafka-broker-10         │
│  ├── Size: 10Gi                                │
│  ├── Contains: topic partition data,           │
│  │             commit log segments              │
│  │                                             │
│  PVC: data-gateway-api-kafka-broker-11         │
│  ├── Size: 10Gi                                │
│  │                                             │
│  PVC: data-gateway-api-kafka-broker-12         │
│  ├── Size: 10Gi                                │
└───────────────────────────────────────────────┘
```

- **managed-csi** is the AKS default StorageClass backed by Azure Premium SSD managed disks
- Disks are zonal — tied to the availability zone of the node they're attached to
- PVCs survive pod restarts and rescheduling (data is persistent)
- Each disk is mounted at `/var/lib/kafka/data` inside the broker container

---

## Certificate Architecture

```
┌──────────────────────┐
│    ClusterIssuer     │
│  kafka-selfsigned-   │
│  issuer              │
│  (self-signed)       │
└──────────┬───────────┘
           │ issues
           ▼
┌──────────────────────┐        ┌──────────────────────┐
│    Certificate       │        │   Kubernetes Secret   │
│  kafka-certificate   │───────→│  kafka-certificate    │
│                      │creates │                      │
│  SAN:                │        │  tls.crt (cert)      │
│  *.kafka.aks.        │        │  tls.key (private)   │
│  mydomain.com        │        │  ca.crt  (CA cert)   │
└──────────────────────┘        └──────────┬───────────┘
                                           │
                              referenced by│
                                           ▼
                                ┌──────────────────────┐
                                │   Kafka Brokers       │
                                │                      │
                                │  brokerCertChainAndKey│
                                │    secretName:        │
                                │    kafka-certificate  │
                                │    certificate:       │
                                │    tls.crt            │
                                │    key: tls.key       │
                                └──────────────────────┘
```

The wildcard certificate `*.kafka.aks.mydomain.com` covers all hostnames:
- `bootstrap.kafka.aks.mydomain.com`
- `broker-10.kafka.aks.mydomain.com`
- `broker-11.kafka.aks.mydomain.com`
- `broker-12.kafka.aks.mydomain.com`

For production, replace the self-signed issuer with Let's Encrypt or an internal CA.

---

## Why This Architecture Over Alternatives

### vs. One LoadBalancer per Broker

```
This architecture:           Alternative:
1 Azure LB ($18/mo)         4 Azure LBs ($72/mo)
1 public IP                 4 public IPs
1 port (9092)               4 ports (9092 each)
SNI routing (smart)         Direct routing (simple but expensive)
```

### vs. Ingress Controller (NGINX/Traefik)

Standard Ingress controllers operate at Layer 7 (HTTP). Kafka uses a binary TCP protocol — you cannot route Kafka traffic through an HTTP Ingress. The Gateway API's `TLSRoute` with Passthrough mode operates at Layer 4 (TCP+TLS), making it compatible with Kafka's protocol.

### vs. Strimzi's Built-in LoadBalancer Listener

Strimzi supports `type: loadbalancer` listeners natively, which creates one LoadBalancer per broker. The Gateway API approach consolidates all traffic through a single LB, reducing cost and Azure resource consumption.

### vs. Strimzi's Built-in Ingress Listener

Strimzi's `type: ingress` listener uses NGINX Ingress with TCP ConfigMap extensions. This works but is a workaround — it requires manual TCP port mapping in the NGINX ConfigMap and doesn't scale cleanly. The Gateway API is the Kubernetes-native solution designed for this use case.

---

## Pod Placement

```
Node 0 (aks-aks-12614529-vmss000000)     Node 1 (aks-aks-12614529-vmss000001)
├── broker-10                             ├── controller-0
├── broker-11                             ├── envoy-proxy (replica 2)
├── broker-12                             ├── envoy-gateway (control plane)
├── envoy-proxy (replica 1)              ├── cert-manager (3 pods)
├── entity-operator                       └── strimzi-cluster-operator
└── kube-system pods                      └── kube-system pods
```

Envoy proxies are spread across both nodes for availability. If one node goes down, the surviving Envoy proxy continues handling traffic through the Azure LB.
