# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka INSERT-produce integration test (§5.2b + §5.2 #1 schema guard).

`INSERT INTO kafka.<source> VALUES(...)` over the PostgreSQL wire publishes the
rows to the object's topic (durability is the Kafka log, no engine staging).

Cases:
  test_consumer_sees_produced_messages — a plain consumer sees the produced JSON
  test_topic_roundtrips_into_table     — the poller re-ingests, SELECT returns them
  test_schema_mismatch_rejected        — a type-mismatched INSERT errors, topic clean
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

NUM_RECORDS = 5
ROUNDTRIP_TIMEOUT_S = 30
POLL_INTERVAL_S = 1.0


class InsertProduceTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.broker = get_kafka_broker(local)
        suffix = uuid.uuid4().hex[:8]
        self.topic = f"otterstax_ins_test_{suffix}"
        self.source = f"ins_test_{suffix}"
        self.records = [{"id": i, "name": f"name_{i}"} for i in range(1, NUM_RECORDS + 1)]
        self.expected = sorted((r["id"], r["name"]) for r in self.records)
        self.conn = None
        print(f"host={self.host} broker={self.broker} topic={self.topic} source={self.source}")

    # --- helpers ---

    def create_topic(self, topic):
        admin = AdminClient({"bootstrap.servers": self.broker})
        for name, fut in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).items():
            try:
                fut.result(timeout=15)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise

    def consume(self, topic, expected, timeout_s=20):
        """Read `topic` from the beginning with a throwaway group."""
        consumer = Consumer({
            "bootstrap.servers": self.broker,
            "group.id": "verify_" + uuid.uuid4().hex[:8],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([topic])
        seen, deadline = [], time.time() + timeout_s
        try:
            while len(seen) < expected and time.time() < deadline:
                msg = consumer.poll(1.0)
                if msg is None or msg.error() is not None:
                    continue
                seen.append(json.loads(msg.value().decode("utf-8")))
        finally:
            consumer.close()
        return seen

    @staticmethod
    def assert_equal(a, b, msg=""):
        if a != b:
            raise AssertionError(f"{a!r} != {b!r}. {msg}")

    # --- setup / teardown ---

    def setup(self):
        """Topic + CREATE SOURCE, then INSERT the records (produced to the topic)."""
        self.create_topic(self.topic)
        self.conn = psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                                    password="testpass", dbname="kafka", autocommit=True)
        self.conn.prepare_threshold = None
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE SOURCE {self.source} (id INT, name VARCHAR) "
                f"WITH (KAFKA_TOPIC='{self.topic}', VALUE_FORMAT='JSON', "
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest');"
            )
            values = ", ".join(f"({r['id']}, '{r['name']}')" for r in self.records)
            cur.execute(f"INSERT INTO kafka.{self.source} (id, name) VALUES {values};")
        print(f"CREATE SOURCE {self.source} + INSERT {NUM_RECORDS} rows issued")

    def cleanup(self):
        if self.conn is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP SOURCE {self.source};")
                print(f"DROP SOURCE {self.source} issued")
        finally:
            self.conn.close()

    # --- cases ---

    def test_consumer_sees_produced_messages(self):
        """A plain Kafka consumer sees the produced JSON messages."""
        consumed = self.consume(self.topic, NUM_RECORDS)
        got = sorted((r["id"], r["name"]) for r in consumed)
        self.assert_equal(got, self.expected, "consumer rows")
        print(f"✅ consumer saw {len(consumed)} produced messages")

    def test_topic_roundtrips_into_table(self):
        """The source poller re-ingests the topic so SELECT returns the rows."""
        deadline, rows = time.time() + ROUNDTRIP_TIMEOUT_S, []
        while time.time() < deadline:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT id, name FROM kafka.{self.source};")
                rows = cur.fetchall() if cur.description is not None else []
            if len(set(rows)) >= NUM_RECORDS:
                break
            time.sleep(POLL_INTERVAL_S)
        self.assert_equal(sorted(set(rows)), self.expected, f"round-trip SELECT (raw {rows})")
        print(f"✅ round-trip: SELECT sees {len(set(rows))} rows")

    def test_schema_mismatch_rejected(self):
        """A type-mismatched INSERT errors and produces nothing (own fresh source)."""
        suffix = uuid.uuid4().hex[:8]
        topic, source = f"otterstax_ins_neg_{suffix}", f"ins_neg_{suffix}"
        print(f"--- negative case: topic={topic} source={source} ---")
        self.create_topic(topic)
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE SOURCE {source} (id INT, name VARCHAR) "
                f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest');"
            )
        raised = False
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"INSERT INTO kafka.{source} (id, name) VALUES ('notanint', 'x');")
        except psycopg.Error as e:
            raised = True
            print(f"✅ bad INSERT rejected: {str(e).strip()[:100]}")
        self.assert_equal(raised, True, "expected the type-mismatched INSERT to error")
        self.assert_equal(self.consume(topic, 1, 6), [], "topic must be empty after a rejected INSERT")
        print("✅ topic is empty — no garbage produced")
        with self.conn.cursor() as cur:
            cur.execute(f"DROP SOURCE {source};")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_consumer_sees_produced_messages()
            self.test_topic_roundtrips_into_table()
            self.test_schema_mismatch_rejected()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka INSERT-produce integration test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    args = parser.parse_args()
    try:
        InsertProduceTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka INSERT-produce\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka INSERT-produce\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
