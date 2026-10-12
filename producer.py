#!/usr/bin/env python3
"""
Mock Kafka producer — generates realistic e-commerce transaction events
and sends them to a Kafka topic via SASL_SSL / SCRAM-SHA-512.

Usage:
    python producer.py                     # 1 msg/sec (default)
    python producer.py --rate 5            # 5 msgs/sec
    python producer.py --count 100         # send exactly 100 then stop
    python producer.py --rate 10 --count 0 # unlimited at 10/sec
"""

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaError
from dotenv import load_dotenv


def load_config():
    env_path = os.path.join(os.path.dirname(__file__), "creds", ".env")
    load_dotenv(env_path)

    required = ["KAFKA_BOOTSTRAP", "KAFKA_TOPIC", "KAFKA_USERNAME",
                "KAFKA_PASSWORD", "KAFKA_CA_CERT"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        print(f"Run: bash setup-credentials.sh")
        sys.exit(1)

    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "SCRAM-SHA-512",
        "sasl.username":     os.environ["KAFKA_USERNAME"],
        "sasl.password":     os.environ["KAFKA_PASSWORD"],
        "ssl.ca.location":   os.environ["KAFKA_CA_CERT"],
        "acks":              "all",
        "retries":           3,
        "linger.ms":         50,
    }


PRODUCTS = [
    ("Wireless Headphones", 79.99, "electronics"),
    ("Running Shoes", 129.95, "sportswear"),
    ("Coffee Maker", 49.99, "kitchen"),
    ("Mechanical Keyboard", 159.00, "electronics"),
    ("Yoga Mat", 34.50, "sportswear"),
    ("Cast Iron Skillet", 44.99, "kitchen"),
    ("USB-C Hub", 39.99, "electronics"),
    ("Water Bottle", 24.95, "sportswear"),
    ("Blender", 89.99, "kitchen"),
    ("Webcam HD", 69.99, "electronics"),
]

STATUSES = ["completed", "completed", "completed", "pending", "refunded"]
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-southeast"]


def generate_event():
    product_name, base_price, category = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    return {
        "event_type": "transaction",
        "order_id": str(uuid.uuid4())[:8],
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product": product_name,
        "category": category,
        "quantity": qty,
        "unit_price": base_price,
        "total_amount": round(base_price * qty, 2),
        "currency": "USD",
        "status": random.choice(STATUSES),
        "region": random.choice(REGIONS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


delivered = 0
failed = 0


def delivery_callback(err, msg):
    global delivered, failed
    if err:
        failed += 1
        print(f"  FAIL  partition={msg.partition()} err={err}")
    else:
        delivered += 1
        if delivered % 10 == 0 or delivered <= 3:
            print(f"  ACK   partition={msg.partition():>2}  "
                  f"offset={msg.offset():<8}  "
                  f"[{delivered} delivered]")


shutdown = False


def handle_signal(sig, frame):
    global shutdown
    shutdown = True
    print("\nShutting down gracefully...")


def main():
    parser = argparse.ArgumentParser(description="Kafka mock producer")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="Messages per second (default: 1)")
    parser.add_argument("--count", type=int, default=0,
                        help="Total messages to send, 0=unlimited (default: 0)")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    config = load_config()
    topic = os.environ["KAFKA_TOPIC"]
    producer = Producer(config)

    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0

    print(f"Producer starting — topic={topic}  rate={args.rate}/sec  "
          f"count={'unlimited' if args.count == 0 else args.count}")
    print(f"Bootstrap: {config['bootstrap.servers']}")
    print("-" * 60)

    try:
        while not shutdown:
            if args.count > 0 and sent >= args.count:
                break

            event = generate_event()
            key = event["customer_id"]
            value = json.dumps(event)

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_callback,
            )
            sent += 1

            print(f"  SEND  #{sent:<6}  order={event['order_id']}  "
                  f"customer={event['customer_id']}  "
                  f"amount=${event['total_amount']:.2f}  "
                  f"status={event['status']}")

            producer.poll(0)

            if interval > 0:
                time.sleep(interval)

    finally:
        print("-" * 60)
        remaining = len(producer)
        print(f"Flushing {remaining} pending messages...")
        producer.flush(timeout=10)
        print(f"Done — sent={sent}  delivered={delivered}  failed={failed}")


if __name__ == "__main__":
    main()
