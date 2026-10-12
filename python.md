# Python Kafka Test Suite — Execution & Instructions

## Overview

This test suite validates external access to the Kafka cluster running on AKS through Gateway API (Envoy + TLS passthrough) with SCRAM-SHA-512 authentication. It consists of a mock producer generating e-commerce transaction events and a consumer displaying them in a formatted output.

---

## Prerequisites

### 1. System Requirements

- Python 3.7+
- pip
- kubectl (configured for AKS cluster)
- Access to Kubernetes cluster with deployed Kafka

### 2. DNS Configuration

Ensure these entries are in `/etc/hosts` pointing to your Azure Load Balancer IP:

```bash
85.210.67.170  bootstrap.kafka.aks.mydomain.com
85.210.67.170  broker-10.kafka.aks.mydomain.com
85.210.67.170  broker-11.kafka.aks.mydomain.com
85.210.67.170  broker-12.kafka.aks.mydomain.com
```

---

## Setup

### Step 1: Extract Credentials

```bash
bash setup-credentials.sh
```

**What it does:**
- Extracts CA certificate from `kafka-certificate` secret
- Extracts SCRAM-SHA-512 credentials from `kafka-test-user` secret
- Creates `creds/` directory with:
  - `ca.crt` — CA certificate for TLS verification
  - `.env` — Environment variables for Python scripts
  - `client.properties` — Java/CLI client config

**Output:**
```
==> Extracting CA certificate from secret/kafka-certificate...
==> Extracting SCRAM credentials from secret/kafka-test-user...
==> Writing .env file...
==> Writing client.properties (for Kafka CLI / Docker)...
==> Getting Azure Load Balancer external IP...

============================================================
 Credentials written to: /home/ubuntu/kafka-strimzi-gateway-api/creds/
============================================================

 Files:
   ca.crt              — CA certificate
   client.properties   — Kafka CLI config
   .env                — Python scripts config

 Load Balancer IP: 85.210.67.170
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `confluent-kafka>=2.6.0` — High-performance Kafka client
- `python-dotenv>=1.0.0` — Environment variable loader

---

## Producer Usage

### Basic Usage

```bash
# Send 1 message per second (default, unlimited)
python3 producer.py

# Send at 3 messages per second
python3 producer.py --rate 3

# Send exactly 15 messages then stop
python3 producer.py --count 15

# Burst mode: 10 messages/sec, 50 total
python3 producer.py --rate 10 --count 50
```

### Example Execution

**Command:**
```bash
python3 producer.py --rate 3 --count 15
```

**Output:**
```
Producer starting — topic=test-topic  rate=3.0/sec  count=15
Bootstrap: bootstrap.kafka.aks.mydomain.com:9092
------------------------------------------------------------
  SEND  #1       order=be971c8c  customer=CUST-9088  amount=$69.00  status=pending
  SEND  #2       order=6f82df4b  customer=CUST-8011  amount=$399.95  status=completed
  ACK   partition= 6  offset=1         [1 delivered]
  SEND  #3       order=dc383391  customer=CUST-7730  amount=$649.75  status=refunded
  ACK   partition= 2  offset=2         [2 delivered]
  SEND  #4       order=0a011cf8  customer=CUST-3020  amount=$199.96  status=refunded
  ACK   partition= 9  offset=4         [3 delivered]
  SEND  #5       order=65a5c1eb  customer=CUST-3697  amount=$89.98  status=completed
  SEND  #6       order=2ddcfa8b  customer=CUST-9254  amount=$89.99  status=completed
  SEND  #7       order=cb3e63a0  customer=CUST-7212  amount=$239.97  status=completed
  SEND  #8       order=7089e5f9  customer=CUST-2284  amount=$99.98  status=refunded
  SEND  #9       order=8cfb5394  customer=CUST-2027  amount=$89.98  status=completed
  SEND  #10      order=892c3634  customer=CUST-8551  amount=$99.80  status=refunded
  SEND  #11      order=82bf4bce  customer=CUST-9941  amount=$795.00  status=pending
  ACK   partition= 9  offset=5         [10 delivered]
  SEND  #12      order=17c90077  customer=CUST-9817  amount=$24.95  status=completed
  SEND  #13      order=64c7abee  customer=CUST-1955  amount=$249.95  status=pending
  SEND  #14      order=63119fbf  customer=CUST-9770  amount=$359.96  status=completed
  SEND  #15      order=28c3b616  customer=CUST-4414  amount=$172.50  status=completed
------------------------------------------------------------
Flushing 2 pending messages...
Done — sent=15  delivered=15  failed=0
```

**Result:** 15/15 messages delivered successfully, 0 failed

### What the Producer Does

1. **Generates realistic mock data:**
   - E-commerce transactions with order IDs, customer IDs
   - 10 different products across 3 categories (electronics, sportswear, kitchen)
   - Random quantities (1-5), calculated totals
   - Status: completed, pending, or refunded
   - Regions: us-east, us-west, eu-west, eu-central, ap-southeast
   - ISO 8601 timestamps

2. **Connection:**
   - Bootstrap: `bootstrap.kafka.aks.mydomain.com:9092`
   - Security: `SASL_SSL` with `SCRAM-SHA-512`
   - TLS: Custom CA certificate verification
   - Acks: `all` (wait for all in-sync replicas)
   - Retries: 3 with exponential backoff

3. **Partitioning:**
   - Key: `customer_id` (ensures messages from same customer go to same partition)
   - Kafka distributes across 12 partitions based on key hash

4. **Delivery callbacks:**
   - Logs acknowledgments with partition and offset
   - Shows progress every 10 messages
   - Tracks delivered vs failed count

---

## Consumer Usage

### Basic Usage

```bash
# Read new messages only (from latest offset)
python3 consumer.py

# Read all messages from the beginning
python3 consumer.py --from-beginning

# Use a custom consumer group
python3 consumer.py --group my-custom-group

# Read from beginning with custom group
python3 consumer.py --group analytics-team --from-beginning
```

### Example Execution

**Command:**
```bash
python3 consumer.py --from-beginning
```

**Output:**
```
Consumer starting — topic=test-topic  group=test-consumer-group  offset=earliest
Bootstrap: bootstrap.kafka.aks.mydomain.com:9092
Waiting for messages... (Ctrl+C to stop)
------------------------------------------------------------------------------------------------------------------------
      #       Part:Offset    Time       Order        Customer     Product                Qty     Amount  Status
------------------------------------------------------------------------------------------------------------------------
  [+] #1      P 2: 2         13:11:37  order=6f82df4b    customer=CUST-8011   Wireless Headphones     qty=5  $  399.95  completed
  [!] #2      P 2: 3         13:11:39  order=7089e5f9    customer=CUST-2284   Coffee Maker            qty=2  $   99.98  refunded
  [+] #3      P 2: 4         13:11:41  order=63119fbf    customer=CUST-9770   Blender                 qty=4  $  359.96  completed
  [+] #4      P 4: 2         13:11:38  order=65a5c1eb    customer=CUST-3697   Cast Iron Skillet       qty=2  $   89.98  completed
  [+] #5      P 4: 3         13:11:39  order=cb3e63a0    customer=CUST-7212   Wireless Headphones     qty=3  $  239.97  completed
  [+] #6      P 4: 4         13:11:39  order=8cfb5394    customer=CUST-2027   Cast Iron Skillet       qty=2  $   89.98  completed
  [~] #7      P 5: 3         13:11:40  order=82bf4bce    customer=CUST-9941   Mechanical Keyboard     qty=5  $  795.00  pending
  [~] #8      P 6: 1         13:11:37  order=be971c8c    customer=CUST-9088   Yoga Mat                qty=2  $   69.00  pending
  [+] #9      P 6: 2         13:11:38  order=2ddcfa8b    customer=CUST-9254   Blender                 qty=1  $   89.99  completed
  [~] #10     P 6: 3         13:11:41  order=64c7abee    customer=CUST-1955   Coffee Maker            qty=5  $  249.95  pending
  [!] #11     P 9: 4         13:11:37  order=dc383391    customer=CUST-7730   Running Shoes           qty=5  $  649.75  refunded
  [!] #12     P 9: 5         13:11:40  order=892c3634    customer=CUST-8551   Water Bottle            qty=4  $   99.80  refunded
  [+] #13     P 9: 6         13:11:41  order=28c3b616    customer=CUST-4414   Yoga Mat                qty=5  $  172.50  completed
  [!] #14     P10: 0         13:11:38  order=0a011cf8    customer=CUST-3020   Coffee Maker            qty=4  $  199.96  refunded
  [+] #15     P10: 1         13:11:40  order=17c90077    customer=CUST-9817   Water Bottle            qty=1  $   24.95  completed

Shutting down gracefully...
------------------------------------------------------------------------------------------------------------------------
Closing consumer — 15 messages received
```

**Result:** 15 messages consumed successfully

### What the Consumer Does

1. **Subscribes to topic:**
   - Topic: `test-topic`
   - Group: `test-consumer-group` (default, or custom via `--group`)
   - Auto-commit: enabled (offsets committed every 5 seconds)
   - Session timeout: 30 seconds

2. **Formatted output:**
   - **Status icons:**
     - `[+]` — completed transactions
     - `[~]` — pending transactions
     - `[!]` — refunded transactions
   - **Columns:**
     - Message number (sequence in this consumer session)
     - Partition and offset (e.g., `P 2: 4` = partition 2, offset 4)
     - Time (HH:MM:SS extracted from ISO timestamp)
     - Order ID, Customer ID
     - Product name, quantity
     - Total amount, status

3. **Consumer groups:**
   - Multiple consumers can share the same group to parallelize processing
   - Each partition is assigned to exactly one consumer in the group
   - Offsets are tracked per group — different groups can read independently

---

## Architecture Validation

### End-to-End Flow Verified

```
Producer (localhost)
    │
    ├── TLS handshake with SNI: bootstrap.kafka.aks.mydomain.com
    │
    ▼
Azure Load Balancer (85.210.67.170:9092)
    │
    ├── TCP forward to Envoy proxy
    │
    ▼
Envoy Gateway (TLS Passthrough)
    │
    ├── SNI match → routes to bootstrap ClusterIP service
    │
    ▼
Kafka Broker (any of 3 brokers)
    │
    ├── SCRAM-SHA-512 authentication
    │
    ├── Returns metadata with advertised hosts:
    │     - broker-10.kafka.aks.mydomain.com:9092
    │     - broker-11.kafka.aks.mydomain.com:9092
    │     - broker-12.kafka.aks.mydomain.com:9092
    │
    ▼
Producer reconnects to specific broker (e.g., broker-11)
    │
    ├── TLS handshake with SNI: broker-11.kafka.aks.mydomain.com
    │
    ▼
Azure LB → Envoy → SNI match → broker-11 ClusterIP → Broker-11 pod
    │
    ├── Producer sends message with key=customer_id
    │
    ▼
Kafka appends to partition (determined by hash(customer_id))
    │
    ├── Returns ACK (partition, offset)
    │
    ▼
Producer logs delivery confirmation

Consumer (localhost)
    │
    ├── Same connection flow as producer
    │
    ▼
Subscribes to topic with consumer group
    │
    ├── Kafka assigns partitions to this consumer
    │
    ▼
Consumer polls and receives messages
    │
    ├── Parses JSON, formats output
    │
    ▼
Consumer commits offsets (auto-commit every 5s)
```

---

## Message Distribution Analysis

From the execution above, messages were distributed across **6 different partitions** (out of 12 total):

| Partition | Message Count | Offsets |
|-----------|---------------|---------|
| P2 | 3 | 2, 3, 4 |
| P4 | 3 | 2, 3, 4 |
| P5 | 1 | 3 |
| P6 | 3 | 1, 2, 3 |
| P9 | 3 | 4, 5, 6 |
| P10 | 2 | 0, 1 |

This distribution is determined by:
- **Key:** `customer_id` (e.g., `CUST-8011`)
- **Hash function:** Kafka's default MurmurHash2
- **Formula:** `partition = hash(key) % num_partitions`

Customers with the same ID always go to the same partition, maintaining order per customer.

---

## Troubleshooting

### Connection Refused

```
ERROR: Local: Broker transport failure
```

**Fix:**
1. Verify DNS entries in `/etc/hosts`
2. Check Azure LB IP: `kubectl get svc -n envoy-gateway-system`
3. Test connectivity: `nc -zv bootstrap.kafka.aks.mydomain.com 9092`

### Authentication Failed

```
ERROR: SASL authentication failed
```

**Fix:**
1. Re-run `bash setup-credentials.sh`
2. Verify credentials: `cat creds/.env | grep PASSWORD`
3. Check user exists: `kubectl get kafkauser kafka-test-user -n strimzi`

### SSL Certificate Error

```
ERROR: SSL handshake failed
```

**Fix:**
1. Verify CA cert exists: `ls -lh creds/ca.crt`
2. Check certificate is valid: `kubectl get certificate kafka-certificate -n strimzi`
3. Re-extract: `bash setup-credentials.sh`

### No Messages Received

**Consumer shows "Waiting for messages..." but nothing appears:**

1. Check topic has messages:
   ```bash
   kubectl exec -n strimzi gateway-api-kafka-broker-10 -- \
     kafka-run-class.sh kafka.tools.GetOffsetShell \
     --broker-list localhost:9092 --topic test-topic
   ```

2. Verify consumer group offset:
   - Using `--from-beginning` reads from offset 0
   - Without it, reads only new messages

3. Produce a test message while consumer is running

---

## Configuration Reference

### Environment Variables (creds/.env)

| Variable | Value | Purpose |
|----------|-------|---------|
| `KAFKA_BOOTSTRAP` | `bootstrap.kafka.aks.mydomain.com:9092` | Bootstrap server |
| `KAFKA_TOPIC` | `test-topic` | Target topic |
| `KAFKA_USERNAME` | `kafka-test-user` | SCRAM username |
| `KAFKA_PASSWORD` | `<generated>` | SCRAM password |
| `KAFKA_CA_CERT` | `./creds/ca.crt` | CA certificate path |
| `KAFKA_GROUP_ID` | `test-consumer-group` | Default consumer group |

### Producer Configuration

```python
{
    "bootstrap.servers": "bootstrap.kafka.aks.mydomain.com:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "kafka-test-user",
    "sasl.password": "<from .env>",
    "ssl.ca.location": "./creds/ca.crt",
    "acks": "all",           # Wait for all in-sync replicas
    "retries": 3,            # Retry failed sends
    "linger.ms": 50,         # Batch small messages within 50ms
}
```

### Consumer Configuration

```python
{
    "bootstrap.servers": "bootstrap.kafka.aks.mydomain.com:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "kafka-test-user",
    "sasl.password": "<from .env>",
    "ssl.ca.location": "./creds/ca.crt",
    "group.id": "test-consumer-group",
    "auto.offset.reset": "earliest",  # Start from beginning (or "latest")
    "enable.auto.commit": True,       # Commit offsets every 5s
    "session.timeout.ms": 30000,      # 30 seconds
}
```

---

## Success Metrics

Based on the execution above:

| Metric | Value | Status |
|--------|-------|--------|
| Messages sent | 15 | ✅ |
| Messages delivered | 15 | ✅ |
| Messages failed | 0 | ✅ |
| Messages consumed | 15 | ✅ |
| Partitions used | 6 of 12 | ✅ |
| TLS connection | Success | ✅ |
| SCRAM-SHA-512 auth | Success | ✅ |
| SNI routing | Success | ✅ |
| End-to-end latency | <1 second | ✅ |

---

## Next Steps

1. **Load Testing:**
   ```bash
   # Burst 1000 messages at 100/sec
   python3 producer.py --rate 100 --count 1000
   ```

2. **Multi-Consumer Testing:**
   ```bash
   # Terminal 1
   python3 consumer.py --group load-test --from-beginning
   
   # Terminal 2
   python3 consumer.py --group load-test --from-beginning
   
   # Partitions will be split between both consumers
   ```

3. **Monitoring:**
   - Check broker metrics: `kubectl logs -n strimzi gateway-api-kafka-broker-10 | grep SEND`
   - Check consumer lag: `kafka-consumer-groups.sh --bootstrap-server ... --describe --group test-consumer-group`

4. **Production Hardening:**
   - Replace self-signed cert with Let's Encrypt
   - Implement proper ACLs (restrict to specific topics)
   - Add observability (Prometheus metrics)
   - Configure backpressure handling in producer
