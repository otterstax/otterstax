#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Seed the kafka_ingest benchmark topic from the generated ndjson fixture.

Creates the topic (1 partition) and produces every line of
``kafka_events.ndjson`` as one Kafka message, verbatim (the lines are already the
JSON payloads).  Run once at bring-up (a run_benchmark.sh step, not a compose
service — see compose_kafka.yml for why re-seeding must be avoided).

    python benchmark/data/seed_kafka.py --broker bench_kafka:9092 \
        --file benchmark/data/fixtures/kafka_events.ndjson
"""

import argparse
import concurrent.futures
import os
import sys
from pathlib import Path

import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def _default_topic():
    cfg_path = Path(os.getenv("BENCH_YAML", "/app/bench.yaml"))
    if cfg_path.exists():
        with cfg_path.open() as f:
            cfg = yaml.safe_load(f) or {}
        return cfg.get("kafka", {}).get("topic", "bench_events")
    return "bench_events"


def create_topic(broker, topic):
    admin = AdminClient({"bootstrap.servers": broker})
    for name, fut in admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    ).items():
        try:
            fut.result(timeout=30)
            print(f"created topic {name}")
        except concurrent.futures.TimeoutError:
            raise RuntimeError(
                f"timed out creating topic {name}: broker {broker} unreachable")
        except Exception as e:  # noqa: BLE001
            if "already exists" in str(e).lower():
                print(f"topic {name} already exists")
            else:
                raise


def produce_file(broker, topic, path: Path):
    # linger a little + a large batch: bulk seeding, delivery order within the
    # single partition is preserved by librdkafka's per-partition queue.
    producer = Producer({
        "bootstrap.servers": broker,
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 1_000_000,
    })
    delivered = {"ok": 0, "err": 0}

    def _on_delivery(err, _msg):
        if err is not None:
            delivered["err"] += 1
        else:
            delivered["ok"] += 1

    total = 0
    with path.open("rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            # Serve delivery callbacks periodically so the internal queue drains.
            while True:
                try:
                    producer.produce(topic, value=line, callback=_on_delivery)
                    break
                except BufferError:
                    producer.poll(0.5)
            total += 1
            if total % 100000 == 0:
                producer.poll(0)
                print(f"  queued {total} messages...")
    producer.flush(timeout=120)
    if delivered["err"]:
        raise RuntimeError(
            f"{delivered['err']}/{total} messages failed delivery")
    print(f"seeded {delivered['ok']} records into {topic} (from {path})")


def main():
    p = argparse.ArgumentParser(description="Seed the kafka_ingest benchmark topic")
    p.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "bench_kafka:9092"))
    p.add_argument("--topic", default=_default_topic())
    p.add_argument("--file", type=Path,
                   default=Path("/fixtures/kafka_events.ndjson"))
    args = p.parse_args()

    if not args.file.exists():
        print(f"ERROR: fixture not found: {args.file}", file=sys.stderr)
        return 1
    print(f"seeding broker={args.broker} topic={args.topic} file={args.file}")
    create_topic(args.broker, args.topic)
    produce_file(args.broker, args.topic, args.file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
