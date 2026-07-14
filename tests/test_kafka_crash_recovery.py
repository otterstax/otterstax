# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Kafka exactly-once CRASH-RECOVERY test (§5.4 Phase 4 + Phase 5).

The fault-injection proof that the kafka runtime survives a hard crash
exactly-once. Unlike the other test_kafka_*.py scripts (which expect an
already-running server), this one OWNS the server lifecycle — it must kill and
restart the server — so it drives it directly. It needs a reachable broker
(redpanda) and a running server.

Two lifecycle modes:

  Native (--local): launches the server binary itself (Popen + kill -9 +
  restart) on a clean data dir it wipes. Needs a built server binary.

    python tests/test_kafka_crash_recovery.py --local
    # Debug build whose otterbrix asserts would SIGABRT:
    python tests/test_kafka_crash_recovery.py --local --ld-preload /tmp/noassert.so

  Docker (--docker-container <name>): the server is a container; the crash is
  `docker kill` (SIGKILL) and recovery is `docker start` of the SAME container
  (its writable layer — and thus the engine's /tmp/test_collection_sql — survives,
  so no volume is needed). This runs INSIDE the test-client container on the docker
  network (which already ships psycopg + confluent-kafka), so endpoints are the
  docker-network ones (test-otterstax:8817, kafka:9092) for both the driver's kafka
  clients and the BOOTSTRAP_SERVERS baked into CREATE SOURCE/STREAM. To kill/start a
  sibling container from inside, docker-run-tests.sh mounts /var/run/docker.sock into
  test-client (and its image carries the docker CLI). Wired in as a host-orchestrated
  step, run once after the main suite.

    # inside test-client, with the docker socket mounted:
    python test_kafka_crash_recovery.py --docker-container test_otterstax_app

Cases:
  test_source_and_stream_survive_crash — produce N, kill mid-ingest, restart;
      KafkaManager::recover() relaunches the SOURCE poller (table-seek resumes
      from the offsets table → source table reaches exactly N distinct) AND the
      STREAM worker (resumes from its Kafka offset → out topic gets the filtered
      rows exactly once).
  test_insert_values_atomic — INSERT INTO kafka.<src> VALUES (one Kafka txn);
      a read-committed consumer sees the whole batch (all-or-nothing).
  test_insert_values_atomic_through_crash — a big INSERT killed mid-flight leaves
      either the full batch or nothing visible to a read-committed consumer.
"""

import os
import sys
import json
import time
import uuid
import shutil
import signal
import argparse
import threading
import subprocess
import traceback

import psycopg
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from config import get_host, get_kafka_broker, PG_PORT

NUM_RECORDS = 5000
KILL_AT = NUM_RECORDS // 3          # kill once this many rows are committed (mid-ingest)
STREAM_CUTOFF = NUM_RECORDS // 2    # STREAM keeps rows with id < cutoff
INSERT_ROWS = 20
FANIN_BATCH = 1500                  # §5.3 fan-in: rows per source per batch (each source = 2 batches)
PG_WAIT_S = 25
DOCKER_PG_WAIT_S = 90   # docker start + WAL recover has more headroom than the native binary
WAIT_S = 60
# Hardcoded in main.cpp: ComponentManager(make_create_config("/tmp/test_collection_sql/base")).
DATA_DIR = "/tmp/test_collection_sql"


class KafkaCrashRecoveryTest:
    def __init__(self, local, server_bin, ld_preload=None, docker_container=None):
        self.docker_container = docker_container
        # docker mode runs from within the docker network (test-client), so it uses
        # the docker-network endpoints (test-otterstax:8817, kafka:9092) regardless
        # of --local — the driver and the containerised server share one broker addr.
        endpoints_local = local and not docker_container
        self.host = get_host(endpoints_local)
        self.broker = get_kafka_broker(endpoints_local)
        self.server_bin = os.path.abspath(server_bin)
        self.ld_preload = ld_preload
        self.proc = None
        print(f"host={self.host} broker={self.broker} "
              f"{'docker=' + docker_container if docker_container else 'server_bin=' + self.server_bin}")

    # --- server lifecycle (this test owns the process) ---

    def start_server(self):
        if self.docker_container:
            # docker start restarts the SAME container: its writable layer (and the
            # engine's /tmp/test_collection_sql) is intact, so recover() replays.
            # Idempotent — a no-op success if the container is already running.
            subprocess.run(["docker", "start", self.docker_container], check=True,
                           stdout=subprocess.DEVNULL)
            self._wait_for_pg(DOCKER_PG_WAIT_S)
            return
        env = dict(os.environ)  # inherits LD_LIBRARY_PATH etc.
        if self.ld_preload:
            env["LD_PRELOAD"] = self.ld_preload
        self.proc = subprocess.Popen([self.server_bin], env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                     preexec_fn=os.setsid)  # own process group → clean kill
        self._wait_for_pg(PG_WAIT_S)

    def _wait_for_pg(self, timeout_s):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                self._connect().close()
                return
            except psycopg.Error:
                time.sleep(0.3)
        raise RuntimeError(f"server did not accept PG connections within {timeout_s}s")

    def kill_server(self):
        if self.docker_container:
            # SIGKILL to PID 1 — a hard crash, NOT `docker restart` (which is a
            # graceful SIGTERM). The container is stopped but NOT removed, so its
            # data survives for the following docker start.
            subprocess.run(["docker", "kill", self.docker_container], check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return
        if self.proc is None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        subprocess.run(["pkill", "-9", "-f", self.server_bin], check=False)  # survives one signal to a stale pid
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
        self.proc = None

    # --- kafka / pg helpers ---

    def create_topic(self, topic):
        admin = AdminClient({"bootstrap.servers": self.broker})
        for name, fut in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).items():
            try:
                fut.result(timeout=15)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise

    def produce(self, topic, n, start=0):
        p = Producer({"bootstrap.servers": self.broker})
        delivered = {"n": 0}
        def cb(err, _m):
            if err is not None:
                raise RuntimeError(f"delivery failed: {err}")
            delivered["n"] += 1
        for i in range(start, start + n):
            p.produce(topic, value=json.dumps({"id": i, "seq": i}).encode(), callback=cb)
            if i % 1000 == 0:
                p.poll(0)
        p.flush(timeout=60)
        if delivered["n"] != n:
            raise RuntimeError(f"only {delivered['n']}/{n} delivered to {topic}")
        print(f"produced {n} to {topic}")

    def consume_ids(self, topic, expected, timeout_s=WAIT_S):
        """Read-committed consume of `topic` until `expected` distinct ids (or
        timeout). Returns (total_messages, set_of_distinct_ids) so a caller can
        assert both no-loss (distinct == expected) and no-dupes (total == distinct)."""
        c = Consumer({"bootstrap.servers": self.broker, "group.id": f"verify_{uuid.uuid4().hex[:8]}",
                      "auto.offset.reset": "earliest", "isolation.level": "read_committed",
                      "enable.auto.commit": False})
        c.subscribe([topic])
        total, seen = 0, set()
        deadline = time.time() + timeout_s
        try:
            while time.time() < deadline and len(seen) < expected:
                msg = c.poll(1.0)
                if msg is None or msg.error():
                    continue
                total += 1
                seen.add(json.loads(msg.value())["id"])
        finally:
            c.close()
        return total, seen

    def _connect(self):
        conn = psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                               password="testpass", dbname="postgres", autocommit=True, connect_timeout=1)
        conn.prepare_threshold = None  # don't cache the empty-table schemaless Describe
        return conn

    def _count(self, conn, table):
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM kafka.{table};")
            return cur.fetchone()[0] if cur.description is not None else 0

    # --- setup / teardown ---

    def setup(self):
        # docker mode: the data dir lives in the container's writable layer, not
        # on the host — the clean slate is a fresh container recreated by the
        # harness before this test (docker-run-tests.sh Step 8c). Here we only
        # ensure the server is up. Native mode: wipe the host dir + launch.
        if not self.docker_container:
            shutil.rmtree(DATA_DIR, ignore_errors=True)
        self.start_server()

    def cleanup(self):
        self.kill_server()
        # Native owns the host data dir; docker leaves teardown (compose down) to
        # the harness so it never touches the container's internal storage.
        if not self.docker_container:
            shutil.rmtree(DATA_DIR, ignore_errors=True)

    # --- cases ---

    def test_source_and_stream_survive_crash(self):
        """A SOURCE and a STREAM over it both survive a kill -9 mid-ingest: after
        restart recover() relaunches the poller (source → exactly N distinct) and
        the stream worker (out topic → filtered rows, exactly once)."""
        suffix = uuid.uuid4().hex[:8]
        topic = f"otterstax_crash_{suffix}"
        out_topic = f"otterstax_crash_out_{suffix}"
        source = f"crash_src_{suffix}"
        stream = f"crash_stream_{suffix}"

        self.create_topic(topic)
        self.create_topic(out_topic)
        self.produce(topic, NUM_RECORDS)

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SOURCE {source} (id INT, seq BIGINT) "
                        f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true);")
            cur.execute(f"CREATE STREAM {stream} "
                        f"WITH (KAFKA_TOPIC='{out_topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true) "
                        f"AS SELECT id, seq FROM kafka.{source} WHERE id < {STREAM_CUTOFF};")

        # Wait until the source table has committed rows, THEN kill mid-ingest.
        # NB: we kill only after count > 0 — a recovered EMPTY otterbrix table
        # reports no schema (otterbrix-bug-repros.md B-WAL-1/B-WAL-3 family), so
        # recover() can only re-read the source columns once data exists. Remove
        # this ">0 before kill" requirement when that engine bug is fixed.
        deadline, killed_at = time.time() + WAIT_S, None
        while time.time() < deadline:
            c = self._count(conn, source)
            if c >= KILL_AT:
                killed_at = c
                break
            time.sleep(0.1)
        conn.close()
        if killed_at is None:
            raise AssertionError(f"ingest never reached {KILL_AT} to kill at")
        self.kill_server()
        print(f"killed -9 mid-ingest at {killed_at}/{NUM_RECORDS}")

        # Restart — recover() relaunches both the poller and the stream worker.
        self.start_server()
        print("restarted; waiting for poller + stream recovery to finish ingest")

        conn = self._connect()
        deadline = time.time() + WAIT_S
        while time.time() < deadline:
            if self._count(conn, source) >= NUM_RECORDS:
                break
            time.sleep(0.5)
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*), count(DISTINCT id), min(id), max(id) FROM kafka.{source};")
            cnt, distinct, lo, hi = cur.fetchone()
        conn.close()
        if (cnt, distinct, lo, hi) != (NUM_RECORDS, NUM_RECORDS, 0, NUM_RECORDS - 1):
            raise AssertionError(f"SOURCE not exactly-once after crash: count={cnt} distinct={distinct} "
                                 f"range={lo}..{hi}, expected {NUM_RECORDS} / 0..{NUM_RECORDS-1}")
        print(f"✅ SOURCE survived crash exactly-once: {cnt} rows, {distinct} distinct")

        # STREAM: out topic must hold the filtered rows (id < cutoff) exactly once.
        total, ids = self.consume_ids(out_topic, expected=STREAM_CUTOFF)
        if len(ids) != STREAM_CUTOFF:
            raise AssertionError(f"STREAM lost rows after crash: {len(ids)} distinct, expected {STREAM_CUTOFF}")
        if min(ids) != 0 or max(ids) != STREAM_CUTOFF - 1:
            raise AssertionError(f"STREAM produced wrong ids {min(ids)}..{max(ids)}, expected 0..{STREAM_CUTOFF-1}")
        if total != STREAM_CUTOFF:
            raise AssertionError(f"STREAM produced dupes after crash: {total} msgs for {STREAM_CUTOFF} distinct")
        print(f"✅ STREAM survived crash exactly-once: {total} filtered rows in out topic (ids 0..{STREAM_CUTOFF-1})")

    def test_fanin_survives_crash(self):
        """§5.3 fan-in: a STREAM plus an `INSERT INTO <stream> SELECT` query merge
        two sources into ONE output topic, exactly-once, across a kill -9.

            CREATE STREAM merged AS SELECT ... FROM src_a        (src_a's ids)
            INSERT INTO merged SELECT ... FROM src_b             (src_b's ids)
        => the out topic is the union of both sources, each id exactly once.

        Design (deterministic through kill -9, two batches):
          A. Produce batch 1 to both source topics; CREATE the SOURCEs; let both
             ingest batch 1; settle so their backing tables are durably checkpointed.
          B. CREATE the base STREAM + the fan-in INSERT INTO query (both process
             batch 1 into the out topic); settle so their kafka.__sources rows are
             durably checkpointed.
          C. Produce batch 2, then kill -9 IMMEDIATELY — the out topic is still
             missing batch 2. recover() MUST relaunch BOTH workers (the stream in
             pass 2, the INSERT INTO query in pass 3) for the union to complete;
             if the INSERT INTO query is not relaunched, src_b's batch 2 is lost.
          D. Restart; both resume from their Kafka offsets and finish the union.

        Why batches + settle (not a plain mid-ingest kill): recover() re-reads each
        source's columns from its backing TABLE and finds the INSERT INTO query in
        kafka.__sources — both engine tables. A crash-truncated EMPTY table recovers
        schemaless (B-WAL-1/B-WAL-3) → recover() skips the source and its dependent
        query. Establishing that durable state BEFORE the crash makes recovery
        deterministic while batch 2 keeps the out topic genuinely incomplete at the
        kill. Simplify to a single mid-ingest kill once B-WAL-3 is fixed."""
        suffix = uuid.uuid4().hex[:8]
        topic_a = f"otterstax_fanin_a_{suffix}"
        topic_b = f"otterstax_fanin_b_{suffix}"
        out_topic = f"otterstax_fanin_out_{suffix}"
        src_a = f"fanin_a_{suffix}"
        src_b = f"fanin_b_{suffix}"
        stream = f"fanin_merged_{suffix}"
        p = FANIN_BATCH                        # rows per source per batch (each source = 2p total)
        b_off = 10_000_000                     # disjoint id range for src_b
        expected = 4 * p                       # union: src_a 2p + src_b 2p

        for t in (topic_a, topic_b, out_topic):
            self.create_topic(t)

        # Phase A — sources ingest batch 1, then settle (durable backing tables).
        self.produce(topic_a, p, start=0)              # a batch 1: [0, p)
        self.produce(topic_b, p, start=b_off)          # b batch 1: [b_off, b_off+p)
        conn = self._connect()
        with conn.cursor() as cur:
            for name, topic in ((src_a, topic_a), (src_b, topic_b)):
                cur.execute(f"CREATE SOURCE {name} (id INT, seq BIGINT) "
                            f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
                            f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true);")
        deadline = time.time() + WAIT_S
        while time.time() < deadline and not (self._count(conn, src_a) >= p and self._count(conn, src_b) >= p):
            time.sleep(0.2)
        if not (self._count(conn, src_a) >= p and self._count(conn, src_b) >= p):
            raise AssertionError("fan-in sources did not ingest batch 1 before the crash")
        time.sleep(5)  # settle: checkpoint the source tables

        # Phase B — the base STREAM + the fan-in INSERT INTO query (process batch 1),
        # then settle so their kafka.__sources rows are durable before the crash.
        with conn.cursor() as cur:
            cur.execute(f"CREATE STREAM {stream} "
                        f"WITH (KAFKA_TOPIC='{out_topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true) "
                        f"AS SELECT id, seq FROM kafka.{src_a};")
            cur.execute(f"INSERT INTO kafka.{stream} SELECT id, seq FROM kafka.{src_b};")
        conn.close()
        time.sleep(5)  # settle: checkpoint kafka.__sources (so recover() sees the INSERT INTO query)

        # Phase C — produce batch 2, then kill immediately (out topic still missing it).
        self.produce(topic_a, p, start=p)              # a batch 2: [p, 2p)
        self.produce(topic_b, p, start=b_off + p)      # b batch 2: [b_off+p, b_off+2p)
        self.kill_server()
        print("killed -9 right after producing batch 2 (out topic incomplete)")

        # Phase D — restart: recover() relaunches the stream (pass 2) + INSERT INTO
        # query (pass 3); both resume from their Kafka offsets and finish the union.
        self.start_server()
        print("restarted; waiting for the union to complete on the out topic")
        total, ids = self.consume_ids(out_topic, expected=expected, timeout_s=180)
        a_ids = sum(1 for i in ids if i < b_off)
        b_ids = sum(1 for i in ids if i >= b_off)
        print(f"fan-in out topic: {total} msgs, {len(ids)} distinct (src_a={a_ids}/{2 * p}, src_b={b_ids}/{2 * p})")
        if len(ids) != expected or a_ids != 2 * p or b_ids != 2 * p:
            raise AssertionError(f"fan-in lost rows after crash: {len(ids)} distinct, expected {expected} "
                                 f"(src_a={a_ids}/{2 * p}, src_b={b_ids}/{2 * p})")
        if total != expected:
            raise AssertionError(f"fan-in produced dupes after crash: {total} msgs for {expected} distinct")
        print(f"✅ fan-in survived crash exactly-once: {total} rows = union of both sources "
              f"(src_a {2 * p} + src_b {2 * p}, no dupes/loss)")

    def test_insert_values_atomic(self):
        """INSERT INTO kafka.<src> VALUES commits the whole batch in one Kafka
        transaction — a read-committed consumer sees all rows or none."""
        suffix = uuid.uuid4().hex[:8]
        topic = f"otterstax_ins_{suffix}"
        source = f"ins_src_{suffix}"
        self.create_topic(topic)

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SOURCE {source} (id INT, seq BIGINT) "
                        f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true);")
            values = ", ".join(f"({i}, {i})" for i in range(INSERT_ROWS))
            cur.execute(f"INSERT INTO kafka.{source} (id, seq) VALUES {values};")
        conn.close()
        print(f"INSERT INTO {source} VALUES — {INSERT_ROWS} rows in one txn")

        total, ids = self.consume_ids(topic, expected=INSERT_ROWS)
        if len(ids) != INSERT_ROWS or total != INSERT_ROWS:
            raise AssertionError(f"INSERT VALUES not atomic: {total} msgs / {len(ids)} distinct, "
                                 f"expected {INSERT_ROWS} (a read-committed consumer must see the full batch)")
        print(f"✅ INSERT VALUES atomic: read-committed consumer saw all {total} rows")

    def test_insert_values_atomic_through_crash(self):
        """A big INSERT INTO kafka.<src> VALUES killed mid-flight is all-or-nothing:
        after restart a read-committed consumer sees either the whole batch (it
        committed before the kill) or nothing (the Kafka txn aborted) — NEVER a
        partial batch."""
        suffix = uuid.uuid4().hex[:8]
        topic = f"otterstax_inscrash_{suffix}"
        source = f"inscrash_src_{suffix}"
        self.create_topic(topic)
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SOURCE {source} (id INT, seq BIGINT) "
                        f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true);")
        conn.close()

        # Fire a large INSERT on a background thread, then kill the server while it
        # is in flight. The client connection drops (the INSERT is not retried), so
        # the txn either committed just before the kill or aborted — both atomic.
        big = NUM_RECORDS
        values = ", ".join(f"({i}, {i})" for i in range(big))
        result = {}
        def do_insert():
            try:
                c = psycopg.connect(host=self.host, port=PG_PORT, user="testuser",
                                    password="testpass", dbname="postgres", autocommit=True)
                with c.cursor() as cur:
                    cur.execute(f"INSERT INTO kafka.{source} (id, seq) VALUES {values};")
                c.close()
                result["ok"] = True
            except Exception as e:  # connection dropped by the kill — expected
                result["err"] = repr(e)
        t = threading.Thread(target=do_insert)
        t.start()
        time.sleep(0.3)  # let the produce start, then crash mid-flight
        self.kill_server()
        t.join(timeout=15)
        print(f"killed -9 mid-INSERT (client: {'committed' if result.get('ok') else 'dropped'})")

        self.start_server()  # restart (no retry — we just check nothing partial survived)
        total, ids = self.consume_ids(topic, expected=big, timeout_s=20)
        if total not in (0, big):
            raise AssertionError(f"INSERT VALUES NOT atomic through crash: {total} msgs "
                                 f"(a partial batch survived — expected 0 or {big})")
        if total == big and len(ids) != big:
            raise AssertionError(f"INSERT VALUES committed with dupes/loss: {total} msgs, {len(ids)} distinct")
        print(f"✅ INSERT VALUES atomic through crash: consumer saw {total} (∈ {{0, {big}}}, never partial)")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_source_and_stream_survive_crash()
            self.test_fanin_survives_crash()
            self.test_insert_values_atomic()
            self.test_insert_values_atomic_through_crash()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="Kafka exactly-once crash-recovery test")
    parser.add_argument("--local", action="store_true",
                        help="Use localhost endpoints instead of docker-network hostnames")
    parser.add_argument("--server-bin", default="build/Debug/server", help="path to the otterstax server binary")
    parser.add_argument("--ld-preload", default=None,
                        help="optional path forwarded into the server's LD_PRELOAD "
                             "(e.g. a local __assert_fail stub for a Debug build)")
    parser.add_argument("--docker-container", default=None, metavar="NAME",
                        help="drive a containerised server instead of a local binary: the crash is "
                             "`docker kill NAME` and recovery is `docker start NAME` (host-driven; "
                             "reaches the published PG 8817 + broker 19092, SQL uses kafka:9092)")
    args = parser.parse_args()
    try:
        KafkaCrashRecoveryTest(local=args.local, server_bin=args.server_bin,
                               ld_preload=args.ld_preload,
                               docker_container=args.docker_container).run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - Kafka Crash-Recovery (exactly-once)\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - Kafka Crash-Recovery\033[0m")
        print("=" * 70)
        print(f"\033[91m{e!r}\033[0m")
        traceback.print_exc()
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
