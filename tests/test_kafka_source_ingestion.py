# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka SOURCE ingestion integration test.

Produces JSON records to a Kafka topic, declares a CREATE SOURCE over the
PostgreSQL wire frontend, and verifies the source's poller ingests them into
the local ``kafka.<source>`` table (visible via SELECT). Exercises the live
consume -> json_to_chunk -> INSERT path against a real broker (redpanda).

Ingestion is at-least-once, so the assertion is on the distinct row set; a raw
count above the produced count is reported but tolerated.

Cases:
  test_poller_ingests_distinct_rows — SELECT returns the produced rows (distinct)
"""

import sys
import json
import time
import uuid
import argparse
import traceback
import concurrent.futures

import psycopg
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import get_host, get_kafka_broker, PG_PORT

NUM_RECORDS = 5
POLL_TIMEOUT_S = 30
POLL_INTERVAL_S = 1.0


class SourceIngestionTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.broker = get_kafka_broker(local)
        suffix = uuid.uuid4().hex[:8]
        self.topic = f"otterstax_src_test_{suffix}"
        self.source = f"src_test_{suffix}"
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
                print(f"Created topic {name}")
            except concurrent.futures.TimeoutError:
                raise RuntimeError(
                    f"Timed out creating topic {name}: broker {self.broker} unreachable. "
                    f"Check `docker compose -f compose.test.yml ps` (is 19092 published?) "
                    f"and `... logs kafka`.")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"Topic {name} already exists")
                else:
                    raise

    def produce_records(self, topic, records):
        producer = Producer({"bootstrap.servers": self.broker})
        delivered = {"count": 0}

        def _on_delivery(err, _msg):
            if err is not None:
                raise RuntimeError(f"Delivery failed: {err}")
            delivered["count"] += 1

        for rec in records:
            producer.produce(topic, value=json.dumps(rec).encode("utf-8"), callback=_on_delivery)
        producer.flush(timeout=15)
        if delivered["count"] != len(records):
            raise RuntimeError(f"Only {delivered['count']}/{len(records)} messages delivered")
        print(f"Produced {delivered['count']} records to {topic}")

    # --- setup / teardown ---

    def setup(self):
        """Pre-create the topic and produce *before* declaring the source, so
        OFFSET_RESET='earliest' guarantees the poller reads every record."""
        self.create_topic(self.topic)
        self.produce_records(self.topic, self.records)
        self.conn = psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                                    password="testpass", dbname="kafka", autocommit=True)
        # Don't let psycopg server-prepare the poll query: while the table is still
        # empty the engine returns a schemaless result (Describe -> NoData), and a
        # prepared statement would cache that and never see the rows once they land.
        self.conn.prepare_threshold = None
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE SOURCE {self.source} (id INT, name VARCHAR) "
                f"WITH (KAFKA_TOPIC='{self.topic}', VALUE_FORMAT='JSON', "
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest');"
            )
        print(f"CREATE SOURCE {self.source} issued")

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

    def test_poller_ingests_distinct_rows(self):
        """The poller ingests the topic; SELECT returns the distinct produced rows
        (at-least-once: a raw count above the produced count is tolerated)."""
        deadline, rows = time.time() + POLL_TIMEOUT_S, []
        while time.time() < deadline:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT id, name FROM kafka.{self.source};")
                # An empty result comes back as "SELECT 0" with no RowDescription
                # (cur.description is None) — treat as zero rows and keep polling.
                rows = cur.fetchall() if cur.description is not None else []
            if len(set(rows)) >= NUM_RECORDS:
                break
            time.sleep(POLL_INTERVAL_S)

        distinct = sorted(set(rows))
        if distinct != self.expected:
            raise AssertionError(
                f"After {POLL_TIMEOUT_S}s expected {self.expected}, got distinct {distinct} (raw rows: {rows})")
        if len(rows) != NUM_RECORDS:
            print(f"⚠️  {len(rows)} raw rows for {NUM_RECORDS} records (at-least-once duplicates, tolerated)")
        print(f"✅ Ingested {len(distinct)} distinct rows matching produced records")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_poller_ingests_distinct_rows()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka SOURCE ingestion integration test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    args = parser.parse_args()
    try:
        SourceIngestionTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka SOURCE Ingestion\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka SOURCE Ingestion\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
