# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka STREAM write integration test (§5.2 #2 — ksql-style INSERT INTO stream).

Unlike a SOURCE (which has a backing table), a STREAM is a continuous query with
no column list. `INSERT INTO kafka.<stream> VALUES(...)` still produces the rows
to the stream's output topic (ksqlDB VALUES semantics), validated against the
stream's OUTPUT schema — the projected columns of its SELECT, computed without
data via schema_utils::aggregate_filter_schema.

To isolate the write from the stream's continuous transform we never produce to
the source topic, so the worker emits nothing and the output topic contains only
the directly-INSERTed rows.

Cases:
  test_insert_produces_to_output_topic — a matching INSERT lands on the topic
  test_schema_mismatch_rejected        — a bad INSERT errors and leaks nothing
"""

import sys
import json
import time
import uuid
import argparse
import traceback

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from config import get_host, get_kafka_broker, PG_PORT

# The stream keeps id, name; we INSERT three rows directly into the stream.
RECORDS = [{"id": i, "name": f"name_{i}"} for i in (3, 4, 5)]
EXPECTED = sorted((r["id"], r["name"]) for r in RECORDS)
CONSUME_TIMEOUT_S = 30
DRAIN_GRACE_S = 3


class StreamWriteTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.broker = get_kafka_broker(local)
        suffix = uuid.uuid4().hex[:8]
        self.src_topic = f"otterstax_sw_src_{suffix}"
        self.out_topic = f"otterstax_sw_out_{suffix}"
        self.source = f"sw_src_{suffix}"
        self.stream = f"sw_out_{suffix}"
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

    def consume(self, topic, expected_count, timeout_s):
        """Every row seen on `topic` (duplicates stay visible), sorted."""
        consumer = Consumer({
            "bootstrap.servers": self.broker,
            "group.id": "verify_" + uuid.uuid4().hex[:8],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
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
                if expected_count and len(got) >= expected_count and grace is None:
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
        """Topics + CREATE SOURCE/STREAM. The source topic is left empty so the
        stream worker emits nothing; the output topic carries only direct INSERTs."""
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
                f"AS SELECT id, name FROM kafka.{self.source};"
            )
        print(f"CREATE SOURCE {self.source} / STREAM {self.stream} issued")

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

    def test_insert_produces_to_output_topic(self):
        """A matching INSERT INTO the stream lands on its output topic (ksqlDB VALUES)."""
        values = ", ".join(f"({r['id']}, '{r['name']}')" for r in RECORDS)
        with self.conn.cursor() as cur:
            cur.execute(f"INSERT INTO kafka.{self.stream} (id, name) VALUES {values};")
        got = self.consume(self.out_topic, len(EXPECTED), CONSUME_TIMEOUT_S)
        self.assert_equal(got, EXPECTED, "output topic rows")
        print(f"✅ INSERT INTO stream produced {len(got)} rows to the output topic")

    def test_schema_mismatch_rejected(self):
        """A type-mismatched INSERT is rejected and leaks nothing extra to the topic."""
        raised = False
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"INSERT INTO kafka.{self.stream} (id, name) VALUES ('notanint', 'x');")
        except psycopg.Error as e:
            raised = True
            print(f"✅ bad INSERT rejected: {str(e).strip()[:100]}")
        self.assert_equal(raised, True, "expected the type-mismatched INSERT to error")
        leaked = self.consume(self.out_topic, len(EXPECTED) + 1, 6)
        self.assert_equal(leaked, EXPECTED, "output topic must be unchanged after a rejected INSERT")
        print("✅ rejected INSERT produced nothing extra")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_insert_produces_to_output_topic()
            self.test_schema_mismatch_rejected()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka STREAM write integration test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    args = parser.parse_args()
    try:
        StreamWriteTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka STREAM write\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka STREAM write\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
