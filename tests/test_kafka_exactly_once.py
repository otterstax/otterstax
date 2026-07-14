# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka exactly-once STREAM integration test (§5.2d, happy-path).

The STREAM is created with TRANSACTIONAL=true, so the worker runs a transactional
producer: each batch is begin_transaction -> produce -> send_offsets_to_-
transaction -> commit_transaction.

The output topic is verified by a READ-COMMITTED consumer — the point of the
test: it only sees records from committed transactions, so if commit never ran
the consumer would block until timeout (proving the txn commits), and exactly-once
means each transformed row appears exactly once (asserted as an exact multiset).

Cases:
  test_read_committed_exact_multiset — committed rows seen once, no dupes/misses
"""

import sys
import json
import time
import uuid
import argparse
import traceback

import psycopg
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from config import get_host, get_kafka_broker, PG_PORT

# Source records id=1..5; the stream keeps id > 2 -> expect 3,4,5 on the output.
SOURCE_RECORDS = [{"id": i, "name": f"name_{i}"} for i in range(1, 6)]
THRESHOLD = 2
EXPECTED = sorted((r["id"], r["name"]) for r in SOURCE_RECORDS if r["id"] > THRESHOLD)
CONSUME_TIMEOUT_S = 30
DRAIN_GRACE_S = 3


class ExactlyOnceTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.broker = get_kafka_broker(local)
        suffix = uuid.uuid4().hex[:8]
        self.src_topic = f"otterstax_eos_src_{suffix}"
        self.out_topic = f"otterstax_eos_out_{suffix}"
        self.source = f"eos_src_{suffix}"
        self.stream = f"eos_out_{suffix}"
        self.conn = None
        print(f"host={self.host} broker={self.broker} out_topic={self.out_topic}")

    # --- helpers ---

    def create_topic(self, topic):
        admin = AdminClient({"bootstrap.servers": self.broker})
        for name, fut in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).items():
            try:
                fut.result(timeout=15)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise

    def produce(self, topic, records):
        producer = Producer({"bootstrap.servers": self.broker})
        for rec in records:
            producer.produce(topic, value=json.dumps(rec).encode("utf-8"))
        producer.flush(15)
        print(f"Produced {len(records)} records to {topic}")

    def consume_committed(self, topic, expected_count, timeout_s):
        """Collect every row a read-committed consumer sees (duplicates visible)."""
        consumer = Consumer({
            "bootstrap.servers": self.broker,
            "group.id": "verify_" + uuid.uuid4().hex[:8],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",  # only committed transactions
        })
        consumer.subscribe([topic])
        got, deadline, grace = [], time.time() + timeout_s, None
        try:
            while time.time() < deadline:
                msg = consumer.poll(1.0)
                if msg is None or msg.error() is not None:
                    if grace is not None and time.time() >= grace:
                        break
                    continue
                rec = json.loads(msg.value().decode("utf-8"))
                got.append((rec["id"], rec["name"]))
                if len(got) >= expected_count and grace is None:
                    grace = time.time() + DRAIN_GRACE_S
        finally:
            consumer.close()
        return sorted(got)

    @staticmethod
    def assert_equal(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{a!r} != {b!r}. {msg}")

    # --- setup / teardown ---

    def setup(self):
        """Topics + CREATE SOURCE + CREATE STREAM (TRANSACTIONAL), then feed the
        source so the transactional worker transforms it onto the output topic."""
        self.create_topic(self.src_topic)
        self.create_topic(self.out_topic)
        self.conn = psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                                    password="testpass", dbname="kafka", autocommit=True)
        self.conn.prepare_threshold = None
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE SOURCE {self.source} (id INT, name VARCHAR) "
                f"WITH (KAFKA_TOPIC='{self.src_topic}', VALUE_FORMAT='JSON', "
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest');"
            )
            cur.execute(
                f"CREATE STREAM {self.stream} "
                f"WITH (KAFKA_TOPIC='{self.out_topic}', VALUE_FORMAT='JSON', "
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', "
                f"TRANSACTIONAL=true) "
                f"AS SELECT id, name FROM kafka.{self.source} WHERE id > {THRESHOLD};"
            )
        print(f"CREATE SOURCE {self.source} / STREAM {self.stream} (transactional) issued")
        self.produce(self.src_topic, SOURCE_RECORDS)

    def cleanup(self):
        if self.conn is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP STREAM {self.stream};")
                cur.execute(f"DROP SOURCE {self.source};")
                print("DROP STREAM / DROP SOURCE issued")
        finally:
            self.conn.close()

    # --- cases ---

    def test_read_committed_exact_multiset(self):
        """A read-committed consumer sees exactly the filtered rows — no dupes/misses."""
        got = self.consume_committed(self.out_topic, len(EXPECTED), CONSUME_TIMEOUT_S)
        self.assert_equal(got, EXPECTED, "read-committed output rows")
        print(f"✅ exactly-once stream produced {len(got)} committed rows (no dupes/misses)")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_read_committed_exact_multiset()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka exactly-once STREAM integration test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    args = parser.parse_args()
    try:
        ExactlyOnceTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka STREAM exactly-once\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka STREAM exactly-once\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
