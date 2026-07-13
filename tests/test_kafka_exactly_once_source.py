# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka SOURCE exactly-once ingestion integration test (§5.4 Phase 3).

Like test_kafka_source_ingestion.py, but the source is declared with
``TRANSACTIONAL=true``: the poller commits each batch's rows AND its offset
advance in ONE engine transaction (BEGIN -> insert(data) -> upsert(offsets) ->
COMMIT), with the ``kafka.<source>__offsets`` table as the source of truth.

This is the happy-path verification: it proves the live transactional loop runs
end to end against a real broker (redpanda) + engine — the batch lands exactly
once and the offsets table advances. True crash-recovery exactly-once (no loss /
no dupe across a mid-batch kill) is Phase 4 and is blocked on the otterbrix
WAL-recovery-on-restart segfault.

Cases:
  test_transactional_source_ingests_exactly — SELECT returns exactly N rows (no
      duplicates) and the offsets table advances to N (earliest -> next offset).
  test_invalid_transactional_rejected — CREATE SOURCE with TRANSACTIONAL='maybe'
      is rejected as a DDL error (our invalid_parameter, not a crash).
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


class ExactlyOnceSourceTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.broker = get_kafka_broker(local)
        suffix = uuid.uuid4().hex[:8]
        self.topic = f"otterstax_eos_src_test_{suffix}"
        self.source = f"eos_src_test_{suffix}"
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
        OFFSET_RESET='earliest' guarantees the poller reads every record in one
        batch."""
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
                f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', "
                f"TRANSACTIONAL=true);"
            )
        print(f"CREATE SOURCE {self.source} (TRANSACTIONAL=true) issued")

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

    def test_transactional_source_ingests_exactly(self):
        """The transactional poller ingests the topic exactly once: SELECT returns
        exactly N rows (no duplicates), and the offsets table advances to N."""
        deadline, rows = time.time() + POLL_TIMEOUT_S, []
        while time.time() < deadline:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT id, name FROM kafka.{self.source};")
                # Empty result => no RowDescription (cur.description is None);
                # treat as zero rows and keep polling.
                rows = cur.fetchall() if cur.description is not None else []
            if len(rows) >= NUM_RECORDS:
                break
            time.sleep(POLL_INTERVAL_S)

        # Exactly-once: the RAW rows (not just distinct) must equal the produced set.
        actual = sorted(rows)
        if actual != self.expected:
            raise AssertionError(
                f"After {POLL_TIMEOUT_S}s expected exactly {self.expected}, got {actual} "
                f"({len(rows)} raw rows — duplicates would indicate the EOS commit is not atomic)")
        print(f"✅ Ingested exactly {len(rows)} rows, no duplicates")

        # The offsets table is the source of truth — it must have advanced to the
        # next offset to read. With NUM_RECORDS messages on partition 0 read from
        # earliest, the committed (next) offset is NUM_RECORDS.
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT partition_id, MAX(committed_offset) "
                f"FROM kafka.{self.source}__offsets GROUP BY partition_id;")
            offset_rows = cur.fetchall() if cur.description is not None else []
        offsets = {p: o for p, o in offset_rows}
        if offsets.get(0) != NUM_RECORDS:
            raise AssertionError(
                f"Expected offsets table to advance partition 0 to {NUM_RECORDS}, got {offsets}")
        print(f"✅ Offsets table advanced: {offsets}")

    def test_invalid_transactional_rejected(self):
        """CREATE SOURCE with a non-boolean TRANSACTIONAL is rejected as a DDL error
        (our invalid_parameter), not a crash."""
        bad = f"bad_txn_{uuid.uuid4().hex[:8]}"
        bad_topic = f"otterstax_{bad}"
        self.create_topic(bad_topic)
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"CREATE SOURCE {bad} (id INT) "
                    f"WITH (KAFKA_TOPIC='{bad_topic}', BOOTSTRAP_SERVERS='{self.broker}', "
                    f"TRANSACTIONAL=maybe);"
                )
            raise AssertionError("CREATE SOURCE with TRANSACTIONAL=maybe should have been rejected")
        except psycopg.Error as e:
            if "transactional" not in str(e).lower():
                raise AssertionError(f"Rejected, but unexpected error: {e!r}")
            print(f"✅ Invalid TRANSACTIONAL rejected: {str(e).strip().splitlines()[0]}")
        finally:
            # The reject still left the backing table + registry entry (validation
            # is post-create — see CLEANUP.md); drop best-effort so reruns are clean.
            try:
                with self.conn.cursor() as cur:
                    cur.execute(f"DROP SOURCE {bad};")
            except psycopg.Error:
                pass

    def run_all_tests(self):
        try:
            self.setup()
            self.test_transactional_source_ingests_exactly()
            self.test_invalid_transactional_rejected()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka SOURCE exactly-once ingestion integration test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    args = parser.parse_args()
    try:
        ExactlyOnceSourceTest(local=args.local).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka SOURCE Exactly-Once\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka SOURCE Exactly-Once\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
