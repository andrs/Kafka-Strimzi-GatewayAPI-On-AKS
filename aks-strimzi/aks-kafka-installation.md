# AKS Kafka Installation Log — 2026-10-12

## Environment

| Component | Value |
|-----------|-------|
| AKS Cluster | `aks` |
| Resource Group | `aks-cluster-andres` |
| Region | UK South |
| Kubernetes Version | v1.35.0 |
| Node Pool | `aks` — 2x `Standard_D2s_v3` |
| Node OS | Ubuntu 24.04.5 LTS |
| Container Runtime | containerd 2.3.6-1 |

---

## Step 0 — Scale AKS Cluster to 2 Nodes

```bash
az aks nodepool list \
  --resource-group aks-cluster-andres \
  --cluster-name aks \
  --output table

az aks nodepool scale \
  --resource-group aks-cluster-andres \
  --cluster-name aks \
  --name aks \
  --node-count 2

kubectl get nodes -o wide
```

---

## Step 1 — Install Helm

Helm was not pre-installed on the machine.

```bash
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

Installed: **Helm v3.22.0**

---

## Step 2 — Install Gateway API CRDs

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/experimental-install.yaml
```

CRDs created:
- `gatewayclasses.gateway.networking.k8s.io`
- `gateways.gateway.networking.k8s.io`
- `httproutes.gateway.networking.k8s.io`
- `tlsroutes.gateway.networking.k8s.io`
- `tcproutes.gateway.networking.k8s.io`
- `udproutes.gateway.networking.k8s.io`
- `grpcroutes.gateway.networking.k8s.io`
- `referencegrants.gateway.networking.k8s.io`
- `backendtlspolicies.gateway.networking.k8s.io`
- `backendlbpolicies.gateway.networking.k8s.io`

---

## Step 3 — Install Envoy Gateway

```bash
helm upgrade --install envoy-gateway oci://docker.io/envoyproxy/gateway-helm \
  --version v1.2.1 \
  --namespace envoy-gateway-system \
  --create-namespace \
  --wait
```

Result: **Deployed** in `envoy-gateway-system`

---

## Step 4 — Install Cert-Manager

```bash
helm repo add jetstack https://charts.jetstack.io --force-update

helm upgrade --install cert-manager jetstack/cert-manager \
  --version v1.16.2 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --wait
```

Result: **Deployed** in `cert-manager` (3 pods: controller, cainjector, webhook)

---

## Step 5 — Install Strimzi Kafka Operator

### First attempt — Strimzi 0.44.0 (FAILED)

```bash
helm repo add strimzi https://strimzi.io/charts/ --force-update

helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.44.0 \
  --namespace strimzi \
  --create-namespace \
  --set watchNamespaces="{strimzi}" \
  --wait
```

**Issue 1:** Pre-existing RoleBindings from a previous install blocked Helm.

```
Error: 3 errors occurred:
  * rolebindings.rbac.authorization.k8s.io "strimzi-cluster-operator-watched" already exists
  * rolebindings.rbac.authorization.k8s.io "strimzi-cluster-operator-entity-operator-delegation" already exists
  * rolebindings.rbac.authorization.k8s.io "strimzi-cluster-operator" already exists
```

Fix:

```bash
kubectl delete rolebinding \
  strimzi-cluster-operator-watched \
  strimzi-cluster-operator-entity-operator-delegation \
  strimzi-cluster-operator \
  -n strimzi
```

**Issue 2:** After clearing RoleBindings, operator pod entered `CrashLoopBackOff`.

```bash
kubectl logs strimzi-cluster-operator-6d57797987-vpdtn -n strimzi | head -30
```

Root cause: Strimzi 0.44.0 does not support Kubernetes 1.35.

```
ERROR PlatformFeaturesAvailability:138 - Detection of Kubernetes version failed.
ERROR Main:114 - Failed to gather environment facts
```

Also: `STRIMZI_NAMESPACE: strimzi,strimzi` — duplicate namespace from `watchNamespaces` overlapping with install namespace.

### Resolution — Upgrade to Strimzi 0.51.0

```bash
helm uninstall strimzi-kafka-operator -n strimzi

# Clean leftover rolebindings (if any)
kubectl delete rolebinding \
  strimzi-cluster-operator-watched \
  strimzi-cluster-operator-entity-operator-delegation \
  strimzi-cluster-operator \
  -n strimzi 2>/dev/null; true

# Check available versions
helm search repo strimzi/strimzi-kafka-operator --versions | head -15

# Install 0.51.0 (supports K8s 1.35, ships Kafka 4.1.0)
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.51.0 \
  --namespace strimzi \
  --create-namespace \
  --set watchNamespaces="{strimzi}" \
  --wait
```

Result: **Deployed** — operator pod `1/1 Running`

---

## Step 6 — Apply GatewayClass + EnvoyProxy

```bash
kubectl apply -f 01-gateway-class.yaml
```

Result:
- `gatewayclass.gateway.networking.k8s.io/kafka-gateway-class created`
- `envoyproxy.gateway.envoyproxy.io/kafka-proxy-config created`

---

## Step 7 — Apply Self-Signed ClusterIssuer + Certificate

```bash
kubectl apply -f 03-certificate.yaml

kubectl wait --for=condition=Ready certificate/kafka-certificate -n strimzi --timeout=120s
```

Result:
- `clusterissuer.cert-manager.io/kafka-selfsigned-issuer created`
- `certificate.cert-manager.io/kafka-certificate created`
- Certificate condition met (Ready)

---

## Step 8 — Apply Gateway (TLS Passthrough)

```bash
kubectl apply -f 02-gateway.yaml
```

Result: `gateway.gateway.networking.k8s.io/kafka-gateway created`

Azure automatically provisioned a LoadBalancer with external IP **`85.210.67.170`** on port `9092`.

---

## Step 9 — Apply Kafka Node Pools

```bash
kubectl apply -f 04-kafka-node-pools.yaml
```

Result:
- `kafkanodepool.kafka.strimzi.io/controller created`
- `kafkanodepool.kafka.strimzi.io/broker created`

---

## Step 10 — Deploy Kafka Cluster

The original manifest used Kafka `3.7.1`, but Strimzi 0.51.0 only supports Kafka `4.1.0`. The manifest was updated before applying.

```bash
# Version was updated in 05-kafka.yaml: 3.7.1 → 4.1.0
kubectl apply -f 05-kafka.yaml

kubectl wait kafka/gateway-api-kafka --for=condition=Ready --timeout=600s -n strimzi
```

### Entity Operator CrashLoop (ISSUE)

After the Kafka cluster deployed, the entity operator pod entered `CrashLoopBackOff`.

```bash
kubectl logs gateway-api-kafka-entity-operator-6f6f7f8bb7-4dfhq -n strimzi -c user-operator --tail=30
```

Root cause: Strimzi 0.51.0 expects CRD API version `v1` for KafkaUser/KafkaTopic, but old CRDs from the previous 0.44.0 install only had `v1beta2`.

```
Caused by: Failure executing: GET at:
  .../apis/kafka.strimzi.io/v1/namespaces/strimzi/kafkausers — Message: Not Found.
```

Fix — apply updated CRDs from 0.51.0:

```bash
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-crds-0.51.0.yaml
```

Then restart the entity operator:

```bash
kubectl delete pod -n strimzi -l strimzi.io/name=gateway-api-kafka-entity-operator
```

After restart, entity operator came up `2/2 Running` and Kafka cluster reported **Ready**.

---

## Step 11 — Apply TLS Routes

```bash
kubectl apply -f 06-tls-routes.yaml
```

Result:
- `tlsroute.gateway.networking.k8s.io/kafka-bootstrap created`
- `tlsroute.gateway.networking.k8s.io/kafka-broker-10 created`
- `tlsroute.gateway.networking.k8s.io/kafka-broker-11 created`
- `tlsroute.gateway.networking.k8s.io/kafka-broker-12 created`

---

## Step 12 — Apply KafkaUser + KafkaTopic

```bash
kubectl apply -f 07-user-and-topic.yaml

kubectl wait kafkauser/kafka-test-user --for=condition=Ready --timeout=120s -n strimzi
```

Result:
- `kafkauser.kafka.strimzi.io/kafka-test-user created` — Ready (SCRAM-SHA-512)
- `kafkatopic.kafka.strimzi.io/test-topic created` — Ready (12 partitions, RF=3)

---

## Final State

### Helm Releases

| Release | Namespace | Chart Version | Status |
|---------|-----------|---------------|--------|
| envoy-gateway | envoy-gateway-system | v1.2.1 | Deployed |
| cert-manager | cert-manager | v1.16.2 | Deployed |
| strimzi-kafka-operator | strimzi | 0.51.0 | Deployed |

### Kafka Cluster

| | |
|---|---|
| Name | `gateway-api-kafka` |
| Kafka Version | 4.1.0 |
| Mode | KRaft (no ZooKeeper) |
| Metadata Version | 4.1-IV1 |
| Status | **Ready** |

### Pods in `strimzi` Namespace

| Pod | Ready | Status |
|-----|-------|--------|
| strimzi-cluster-operator-5f8595494d-bjbtf | 1/1 | Running |
| gateway-api-kafka-controller-0 | 1/1 | Running |
| gateway-api-kafka-broker-10 | 1/1 | Running |
| gateway-api-kafka-broker-11 | 1/1 | Running |
| gateway-api-kafka-broker-12 | 1/1 | Running |
| gateway-api-kafka-entity-operator-6f6f7f8bb7-jfct8 | 2/2 | Running |

### Networking

| Resource | Value |
|----------|-------|
| GatewayClass | `kafka-gateway-class` — Accepted |
| Gateway | `kafka-gateway` — Programmed |
| Azure LB External IP | **`85.210.67.170`** |
| Port | `9092` |
| Envoy Proxies | 2 replicas (HA) |

### TLS Routes

| Route | SNI Hostname | Backend Service |
|-------|-------------|-----------------|
| kafka-bootstrap | `bootstrap.kafka.aks.mydomain.com` | `gateway-api-kafka-kafka-gateway-bootstrap:9092` |
| kafka-broker-10 | `broker-10.kafka.aks.mydomain.com` | `gateway-api-kafka-broker-gateway-10:9092` |
| kafka-broker-11 | `broker-11.kafka.aks.mydomain.com` | `gateway-api-kafka-broker-gateway-11:9092` |
| kafka-broker-12 | `broker-12.kafka.aks.mydomain.com` | `gateway-api-kafka-broker-gateway-12:9092` |

### Storage (Azure Managed Disks — managed-csi)

| PVC | Size | Status |
|-----|------|--------|
| data-gateway-api-kafka-controller-0 | 10Gi | Bound |
| data-gateway-api-kafka-broker-10 | 10Gi | Bound |
| data-gateway-api-kafka-broker-11 | 10Gi | Bound |
| data-gateway-api-kafka-broker-12 | 10Gi | Bound |

### Certificate

| | |
|---|---|
| ClusterIssuer | `kafka-selfsigned-issuer` (self-signed) — Ready |
| Certificate | `kafka-certificate` — Ready |
| Secret | `kafka-certificate` |
| SAN | `*.kafka.aks.mydomain.com` |

---

## Issues Encountered & Fixes

### 1. Helm not installed

**Symptom:** `helm: command not found`
**Fix:** Installed via official script:
```bash
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### 2. Strimzi 0.44.0 — stale RoleBindings

**Symptom:** Helm install fails with `already exists` errors for RoleBindings.
**Root Cause:** Leftover resources from a previous Strimzi install not managed by Helm.
**Fix:**
```bash
kubectl delete rolebinding \
  strimzi-cluster-operator-watched \
  strimzi-cluster-operator-entity-operator-delegation \
  strimzi-cluster-operator \
  -n strimzi
```

### 3. Strimzi 0.44.0 — incompatible with K8s 1.35

**Symptom:** Operator pod `CrashLoopBackOff` with `Detection of Kubernetes version failed`.
**Root Cause:** Strimzi 0.44.0 does not support Kubernetes 1.35.
**Fix:** Upgraded to Strimzi 0.51.0 (latest stable, supports K8s 1.35, ships Kafka 4.1.0).
```bash
helm uninstall strimzi-kafka-operator -n strimzi
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --version 0.51.0 --namespace strimzi --set watchNamespaces="{strimzi}" --wait
```

### 4. Kafka version mismatch

**Symptom:** Manifest referenced Kafka 3.7.1 but Strimzi 0.51.0 only supports 4.1.0.
**Fix:** Updated `05-kafka.yaml` version field from `3.7.1` to `4.1.0`.

### 5. Entity Operator CrashLoopBackOff — old CRDs

**Symptom:** User/Topic operator informers fail with `404 Not Found` on `kafka.strimzi.io/v1`.
**Root Cause:** CRDs were from Strimzi 0.44.0 (only `v1beta2`), but 0.51.0 expects `v1`.
**Fix:**
```bash
kubectl apply -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-crds-0.51.0.yaml
kubectl delete pod -n strimzi -l strimzi.io/name=gateway-api-kafka-entity-operator
```

---

## Next Steps

1. **Configure DNS** — Point these hostnames to `85.210.67.170`:
   ```
   85.210.67.170  bootstrap.kafka.aks.mydomain.com
   85.210.67.170  broker-10.kafka.aks.mydomain.com
   85.210.67.170  broker-11.kafka.aks.mydomain.com
   85.210.67.170  broker-12.kafka.aks.mydomain.com
   ```

2. **Generate client config:**
   ```bash
   bash 08-create-client-config.sh
   ```

3. **Test connectivity:**
   ```bash
   kafka-topics.sh --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
     --list --command-config /tmp/kafka-client.properties
   ```
