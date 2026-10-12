#!/usr/bin/env python3
"""
Mock Kafka consumer — subscribes to a topic and prints e-commerce
transaction events with formatted output.

Usage:
    python consumer.py                           # default group, latest
    python consumer.py --from-beginning          # read all messages
    python consumer.py --group my-group          # custom consumer group
"""

import argparse
import json
import os
import signal
import sys
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv


def load_config(group_id, from_beginning):
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
        "bootstrap.servers":  os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol":  "SASL_SSL",
        "sasl.mechanism":     "SCRAM-SHA-512",
        "sasl.username":      os.environ["KAFKA_USERNAME"],
        "sasl.password":      os.environ["KAFKA_PASSWORD"],
        "ssl.ca.location":    os.environ["KAFKA_CA_CERT"],
        "group.id":           group_id,
        "auto.offset.reset":  "earliest" if from_beginning else "latest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    }


STATUS_ICONS = {
    "completed": "+",
    "pending":   "~",
    "refunded":  "!",
}


def format_message(msg_num, data, partition, offset):
    status = data.get("status", "unknown")
    icon = STATUS_ICONS.get(status, "?")

    ts = data.get("timestamp", "")
    try:
        dt = datetime.fromisoformat(ts)
        ts_short = dt.strftime("%H:%M:%S")
    except (ValueError, TypeError):
        ts_short = ts[:19] if ts else "??:??:??"

    return (
        f"  [{icon}] #{msg_num:<5}  "
        f"P{partition:>2}:{'':1}{offset:<8}  "
        f"{ts_short}  "
        f"order={data.get('order_id', '?'):<10}  "
        f"customer={data.get('customer_id', '?'):<10}  "
        f"{data.get('product', '?'):<22}  "
        f"qty={data.get('quantity', '?')}  "
        f"${data.get('total_amount', 0):>8.2f}  "
        f"{status}"
    )


shutdown = False


def handle_signal(sig, frame):
    global shutdown
    shutdown = True
    print("\nShutting down gracefully...")


def main():
    parser = argparse.ArgumentParser(description="Kafka mock consumer")
    parser.add_argument("--group", type=str, default=None,
                        help="Consumer group ID (default: from .env)")
    parser.add_argument("--from-beginning", action="store_true",
                        help="Read from earliest offset")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    group_id = args.group or os.getenv("KAFKA_GROUP_ID", "test-consumer-group")
    config = load_config(group_id, args.from_beginning)
    topic = os.environ["KAFKA_TOPIC"]

    consumer = Consumer(config)
    consumer.subscribe([topic])

    print(f"Consumer starting — topic={topic}  group={group_id}  "
          f"offset={'earliest' if args.from_beginning else 'latest'}")
    print(f"Bootstrap: {config['bootstrap.servers']}")
    print(f"Waiting for messages... (Ctrl+C to stop)")
    print("-" * 120)
    print(f"  {'':3} {'#':<7} {'Part:Offset':<14} {'Time':<10} "
          f"{'Order':<12} {'Customer':<12} {'Product':<22} "
          f"{'Qty':>3}  {'Amount':>9}  Status")
    print("-" * 120)

    msg_count = 0
    error_count = 0

    try:
        while not shutdown:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                error_count += 1
                print(f"  ERROR: {msg.error()}")
                if error_count > 20:
                    print("Too many errors, exiting.")
                    break
                continue

            error_count = 0
            msg_count += 1

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                print(f"  WARN  #{msg_count} — could not parse JSON: "
                      f"{msg.value()[:100]}")
                continue

            print(format_message(msg_count, value, msg.partition(), msg.offset()))

    finally:
        print("-" * 120)
        print(f"Closing consumer — {msg_count} messages received")
        consumer.close()


if __name__ == "__main__":
    main()
