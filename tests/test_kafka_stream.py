# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka continuous STREAM integration test (§5.2c, stateless).

CREATE SOURCE, then CREATE STREAM ... AS SELECT ... WHERE ... — a continuous
worker consumes the source topic, applies the SELECT (filter + projection) to
each batch via the aggregate(empty)+raw_data node-swap, and produces the results
to the stream's output topic.

Cases:
  test_stream_filters_to_output_topic — only the rows matching WHERE reach the topic
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


class StreamTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.broker = get_kafka_broker(local)
        suffix = uuid.uuid4().hex[:8]
        self.src_topic = f"otterstax_stream_src_{suffix}"
        self.out_topic = f"otterstax_stream_out_{suffix}"
        self.source = f"stream_src_{suffix}"
        self.stream = f"stream_out_{suffix}"
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

    def consume_distinct(self, topic, expected_count, timeout_s):
        """Distinct rows seen on `topic` until `expected_count`, sorted."""
        consumer = Consumer({
            "bootstrap.servers": self.broker,
            "group.id": "verify_" + uuid.uuid4().hex[:8],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([topic])
        seen, deadline = set(), time.time() + timeout_s
        try:
            while len(seen) < expected_count and time.time() < deadline:
                msg = consumer.poll(1.0)
                if msg is None or msg.error() is not None:
                    continue
                rec = json.loads(msg.value().decode("utf-8"))
                seen.add((rec["id"], rec["name"]))
        finally:
            consumer.close()
        return sorted(seen)

    @staticmethod
    def assert_equal(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{a!r} != {b!r}. {msg}")

    # --- setup / teardown ---

    def setup(self):
        """Topics + CREATE SOURCE/STREAM, then feed the source topic; the worker
        transforms each batch onto the output topic."""
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
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest') "
                f"AS SELECT id, name FROM kafka.{self.source} WHERE id > {THRESHOLD};"
            )
        print(f"CREATE SOURCE {self.source} / STREAM {self.stream} issued")
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

    def test_stream_filters_to_output_topic(self):
        """Only the rows matching the SELECT's WHERE reach the output topic."""
        got = self.consume_distinct(self.out_topic, len(EXPECTED), CONSUME_TIMEOUT_S)
        self.assert_equal(got, EXPECTED, "output topic rows (WHERE id > 2)")
        print(f"✅ stream produced {len(got)} filtered rows to the output topic")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_stream_filters_to_output_topic()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka continuous STREAM integration test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    args = parser.parse_args()
    try:
        StreamTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka STREAM continuous\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka STREAM continuous\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
