# Kafka External Access Test Suite

End-to-end test for Kafka on AKS exposed via Gateway API (Envoy + TLS passthrough + SCRAM-SHA-512).

## Quick Start

### 1. Extract credentials from Kubernetes

```bash
bash setup-credentials.sh
```

This creates `creds/ca.crt`, `creds/.env`, and `creds/client.properties`.

### 2. Configure DNS

Get the Load Balancer IP from the setup output, then add to `/etc/hosts`:

```bash
# Replace with your actual LB IP
sudo tee -a /etc/hosts <<EOF
85.210.67.170  bootstrap.kafka.aks.mydomain.com
85.210.67.170  broker-10.kafka.aks.mydomain.com
85.210.67.170  broker-11.kafka.aks.mydomain.com
85.210.67.170  broker-12.kafka.aks.mydomain.com
EOF
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the producer

```bash
# 1 message per second (default)
python producer.py

# 5 messages per second
python producer.py --rate 5

# Send exactly 50 messages then stop
python producer.py --count 50

# Burst mode: 10/sec, 200 total
python producer.py --rate 10 --count 200
```

### 5. Run the consumer (in another terminal)

```bash
# Read new messages only
python consumer.py

# Read all messages from the beginning
python consumer.py --from-beginning

# Custom consumer group
python consumer.py --group my-test-group --from-beginning
```

---

## CLI / Docker Alternatives (No Python Required)

### Option A: Kafka CLI (requires local Kafka install)

```bash
# List topics
kafka-topics.sh \
  --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
  --command-config creds/client.properties \
  --list

# Produce (interactive — type messages and press Enter)
kafka-console-producer.sh \
  --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
  --producer.config creds/client.properties \
  --topic test-topic

# Consume
kafka-console-consumer.sh \
  --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
  --consumer.config creds/client.properties \
  --topic test-topic \
  --from-beginning
```

### Option B: Docker (no local install needed)

```bash
# Copy credentials into a temp dir for Docker mount
CREDS_DIR="$(pwd)/creds"

# List topics
docker run --rm -it \
  --add-host bootstrap.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-10.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-11.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-12.kafka.aks.mydomain.com:85.210.67.170 \
  -v "${CREDS_DIR}:/creds:ro" \
  apache/kafka:3.7.1 \
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
    --command-config /creds/client.properties \
    --list

# Produce (interactive)
docker run --rm -it \
  --add-host bootstrap.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-10.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-11.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-12.kafka.aks.mydomain.com:85.210.67.170 \
  -v "${CREDS_DIR}:/creds:ro" \
  apache/kafka:3.7.1 \
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
    --producer.config /creds/client.properties \
    --topic test-topic

# Consume
docker run --rm -it \
  --add-host bootstrap.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-10.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-11.kafka.aks.mydomain.com:85.210.67.170 \
  --add-host broker-12.kafka.aks.mydomain.com:85.210.67.170 \
  -v "${CREDS_DIR}:/creds:ro" \
  apache/kafka:3.7.1 \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server bootstrap.kafka.aks.mydomain.com:9092 \
    --consumer.config /creds/client.properties \
    --topic test-topic \
    --from-beginning
```

### Option C: kcat (kafkacat)

```bash
# Produce
echo '{"test": "hello from kcat"}' | kcat -P \
  -b bootstrap.kafka.aks.mydomain.com:9092 \
  -t test-topic \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=kafka-test-user \
  -X sasl.password="$(cat creds/.env | grep KAFKA_PASSWORD | cut -d= -f2)" \
  -X ssl.ca.location=creds/ca.crt

# Consume
kcat -C \
  -b bootstrap.kafka.aks.mydomain.com:9092 \
  -t test-topic \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=kafka-test-user \
  -X sasl.password="$(cat creds/.env | grep KAFKA_PASSWORD | cut -d= -f2)" \
  -X ssl.ca.location=creds/ca.crt \
  -o beginning
```

---

## File Layout

```
test/
├── setup-credentials.sh   — Extracts creds from K8s secrets
├── requirements.txt       — Python dependencies
├── producer.py            — Mock e-commerce event producer
├── consumer.py            — Formatted event consumer
├── README.md              — This file
└── creds/                 — Created by setup-credentials.sh
    ├── ca.crt             — Kafka CA certificate
    ├── .env               — Config for Python scripts
    └── client.properties  — Config for Kafka CLI / Docker
```
