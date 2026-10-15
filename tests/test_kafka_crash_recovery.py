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
  restart) in an owned temporary data directory. Needs a built server binary.

    python tests/test_kafka_crash_recovery.py --local  # requires rpk on PATH

  Docker (--docker-container <name>): the server is a container; the crash is
  `docker kill` (SIGKILL) and recovery is `docker start` of the SAME container
  (its writable layer, including the configured engine data directory, survives,
  so no volume is needed). This runs INSIDE the test-client container on the docker
  network (which already ships psycopg + confluent-kafka), so endpoints are the
  docker-network ones (test-otterstax:8817, kafka:9092) for both the driver's kafka
  clients and the BOOTSTRAP_SERVERS baked into CREATE SOURCE/STREAM. To kill/start a
  sibling container from inside, docker-run-tests.sh mounts /var/run/docker.sock into
  test-client (and its image carries the docker CLI). Wired in as a host-orchestrated
  step, run once after the main suite.

    # inside test-client, with the docker socket mounted:
    python test_kafka_crash_recovery.py --docker-container <server> --docker-broker-container <broker>

The broker must be an owned Redpanda fixture. The INSERT crash temporarily applies
a producer quota to the server's default librdkafka client ID, keeping the SQL call
in flight until an uncommitted record is observed. Existing quotas are never
overwritten, and the fixture removes its quota in finally.

Cases:
  test_source_and_stream_survive_crash — ingest a prefix, kill, queue the suffix,
      restart with unread input;
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
import tempfile
import signal
import argparse
import contextlib
import threading
import subprocess
import traceback

import psycopg
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from config import get_host, get_kafka_broker, PG_PORT

NUM_RECORDS = 5000
KILL_AT = NUM_RECORDS // 3          # input prefix published before the crash
STREAM_CUTOFF = NUM_RECORDS // 2    # STREAM keeps rows with id < cutoff
INSERT_ROWS = 20
CRASH_INSERT_ROWS = 50_000         # spans multiple producer batches; 5000 fits in one
FANIN_BATCH = 1500                  # §5.3 fan-in: rows per source per batch (each source = 2 batches)
PG_WAIT_S = 25
DOCKER_PG_WAIT_S = 90   # docker start + WAL recover has more headroom than the native binary
WAIT_S = 60


class KafkaCrashRecoveryTest:
    def __init__(self, local, server_bin, docker_container=None, docker_broker_container=None):
        self.docker_container = docker_container
        self.docker_broker_container = docker_broker_container
        if docker_container and not docker_broker_container:
            raise ValueError("Docker crash tests require --docker-broker-container for the owned Redpanda fixture")
        # docker mode runs from within the docker network (test-client), so it uses
        # the docker-network endpoints (test-otterstax:8817, kafka:9092) regardless
        # of --local — the driver and the containerised server share one broker addr.
        endpoints_local = local and not docker_container
        self.host = get_host(endpoints_local)
        self.broker = get_kafka_broker(endpoints_local)
        self.server_bin = os.path.abspath(server_bin)
        self.data_dir = tempfile.TemporaryDirectory(prefix="otterstax-kafka-crash-") if not docker_container else None
        self.proc = None
        print(f"host={self.host} broker={self.broker} "
              f"{'docker=' + docker_container if docker_container else 'server_bin=' + self.server_bin}")

    # --- server lifecycle (this test owns the process) ---

    def start_server(self):
        if self.docker_container:
            # docker start restarts the SAME container: its writable layer (and the
            # engine data directory) is intact, so recover() replays.
            # Idempotent — a no-op success if the container is already running.
            subprocess.run(["docker", "start", self.docker_container], check=True,
                           stdout=subprocess.DEVNULL)
            self._wait_for_pg(DOCKER_PG_WAIT_S)
            return
        env = dict(os.environ)  # inherits LD_LIBRARY_PATH etc.
        self.proc = subprocess.Popen([self.server_bin, "--data-dir", self.data_dir.name], env=env,
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

    def kill_server(self, required=True):
        if self.docker_container:
            # SIGKILL to PID 1 — a hard crash, NOT `docker restart` (which is a
            # graceful SIGTERM). The container is stopped but NOT removed, so its
            # data survives for the following docker start.
            state = json.loads(subprocess.check_output(
                ["docker", "inspect", "--format", "{{json .State}}", self.docker_container]))
            if not state["Running"]:
                if required:
                    raise AssertionError("Server stopped before the planned crash")
                return
            subprocess.run(["docker", "kill", "--signal", "KILL", self.docker_container], check=True,
                           stdout=subprocess.DEVNULL)
            state = json.loads(subprocess.check_output(
                ["docker", "inspect", "--format", "{{json .State}}", self.docker_container]))
            assert not state["Running"] and state["ExitCode"] == 137, state
            return
        if self.proc is None:
            if required:
                raise AssertionError("No server process to crash")
            return
        if self.proc.poll() is not None:
            if required:
                raise AssertionError(f"Server exited before the crash: {self.proc.returncode}")
        else:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            assert self.proc.wait(timeout=10) == -signal.SIGKILL
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

    @contextlib.contextmanager
    def producer_quota(self):
        """Bound the fault-injection window without changing server code or timing sleeps."""
        command = (["docker", "exec", self.docker_broker_container, "rpk"]
                   if self.docker_broker_container else ["rpk"])

        def quotas(action, *args):
            output = subprocess.check_output([*command, "cluster", "quotas", action,
                                              "--name", "client-id=rdkafka", "--format", "json",
                                              "-X", f"brokers={self.broker}", *args], text=True, timeout=20)
            return json.loads(output)

        assert not quotas("describe", "--strict").get("quotas"), "Refusing to replace an existing rdkafka quota"
        try:
            result = quotas("alter", "--add", "producer_byte_rate=8192")
            assert result["status"] == "OK", result
            configured = quotas("describe", "--strict")["quotas"]
            assert len(configured) == 1 and configured[0]["values"] == [
                {"key": "producer_byte_rate", "value": "8192"}], configured
            yield
        finally:
            result = quotas("alter", "--delete", "producer_byte_rate")
            assert result["status"] == "OK", result
            assert not quotas("describe", "--strict").get("quotas"), "Fixture producer quota leaked"

    def wait_committed(self, group, topic, offset, timeout_s=WAIT_S):
        """Observe the worker's committed input offset without joining its group.

        A transactional worker commits its output and this offset together. Only
        after this barrier is the output journal ready for a complete assertion.
        """
        observer = Consumer({"bootstrap.servers": self.broker, "group.id": group,
                             "enable.auto.commit": False})
        deadline = time.monotonic() + timeout_s
        committed = None
        try:
            while time.monotonic() < deadline:
                partition = observer.committed([TopicPartition(topic, 0)], timeout=5)[0]
                if partition.error is not None:
                    raise KafkaException(partition.error)
                committed = partition.offset
                if committed == offset:
                    return
                if committed > offset:
                    raise AssertionError(f"{group} advanced beyond its input: {committed} > {offset}")
                time.sleep(0.1)
        finally:
            observer.close()
        raise AssertionError(f"{group} did not commit {topic} through {offset}: {committed}")

    def consume_ids(self, topic, timeout_s=WAIT_S):
        """Drain the entire single-partition journal, including trailing duplicates.

        Callers first await producer completion or the worker's input-offset
        barrier. The uncommitted high watermark includes unresolved transactions;
        read-committed EOF must reach it, so an open transaction cannot pass as an
        empty result. Timeout and broker errors are failures, not end of input.
        """
        watermark = Consumer({"bootstrap.servers": self.broker, "group.id": f"watermark_{uuid.uuid4().hex}",
                              "enable.auto.commit": False, "isolation.level": "read_uncommitted"})
        c = Consumer({"bootstrap.servers": self.broker, "group.id": f"verify_{uuid.uuid4().hex[:8]}",
                      "auto.offset.reset": "earliest", "isolation.level": "read_committed",
                      "enable.auto.commit": False, "enable.partition.eof": True})
        partition = TopicPartition(topic, 0)
        total, seen = 0, set()
        deadline = time.monotonic() + timeout_s
        try:
            low, high = watermark.get_watermark_offsets(partition, timeout=10, cached=False)
            assert low == 0, f"Fixture journal was truncated: {low}"
            c.assign([TopicPartition(topic, 0, 0)])
            while time.monotonic() < deadline:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(msg.error())
                    _, current_high = watermark.get_watermark_offsets(partition, timeout=10, cached=False)
                    high = max(high, current_high)
                    if msg.offset() >= high:
                        print(f"drained {topic} through offset {high}: {total} messages / {len(seen)} ids")
                        return total, seen
                    continue
                total += 1
                row = json.loads(msg.value())
                assert row["seq"] == row["id"], row
                seen.add(row["id"])
        finally:
            c.close()
            watermark.close()
        raise AssertionError(f"{topic} did not reach committed EOF {high} within {timeout_s}s")

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
        # ensure the server is up. Native mode owns a fresh temporary directory.
        self.start_server()

    def cleanup(self):
        self.kill_server(required=False)
        # Native owns the host data dir; docker leaves teardown (compose down) to
        # the harness so it never touches the container's internal storage.
        if self.data_dir is not None:
            self.data_dir.cleanup()

    # --- cases ---

    def test_verifier_reads_trailing_duplicates(self):
        topic = f"otterstax_verify_{uuid.uuid4().hex[:8]}"
        self.create_topic(topic)
        self.produce(topic, INSERT_ROWS)
        self.produce(topic, 1, start=0)
        total, ids = self.consume_ids(topic)
        assert total == INSERT_ROWS + 1 and ids == set(range(INSERT_ROWS))
        print("PASS journal verifier includes a duplicate after the complete expected ID set")

    def test_source_and_stream_survive_crash(self):
        """A SOURCE and a STREAM survive a kill -9 between controlled input batches: after
        restart recover() relaunches the poller (source → exactly N distinct) and
        the stream worker (out topic → filtered rows, exactly once)."""
        suffix = uuid.uuid4().hex[:8]
        topic = f"otterstax_crash_{suffix}"
        out_topic = f"otterstax_crash_out_{suffix}"
        source = f"crash_src_{suffix}"
        stream = f"crash_stream_{suffix}"

        self.create_topic(topic)
        self.create_topic(out_topic)

        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SOURCE {source} (id INT, seq BIGINT) "
                        f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true);")
            cur.execute(f"CREATE STREAM {stream} "
                        f"WITH (KAFKA_TOPIC='{out_topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true) "
                        f"AS SELECT id, seq FROM kafka.{source} WHERE id < {STREAM_CUTOFF};")

        # Publish only the prefix. The complete input cannot race ahead of SIGKILL.
        self.produce(topic, KILL_AT)
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
        assert killed_at == KILL_AT < NUM_RECORDS
        group = f"otterstax_stream_{stream}"
        self.wait_committed(group, topic, KILL_AT)
        self.kill_server()
        self.produce(topic, NUM_RECORDS - KILL_AT, start=KILL_AT)
        print(f"SIGKILL after {killed_at}/{NUM_RECORDS}; suffix queued while server stopped")

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
        self.wait_committed(group, topic, NUM_RECORDS)
        total, ids = self.consume_ids(out_topic)
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

        Each worker commits the first input batch before SIGKILL. The second
        batches are published while the server is stopped, so both recovered
        workers must resume from their offsets to complete the output journal.
        """
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

        # Phase A — sources ingest the first input batch.
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
        # Phase B — both workers commit their first input batches.
        with conn.cursor() as cur:
            cur.execute(f"CREATE STREAM {stream} "
                        f"WITH (KAFKA_TOPIC='{out_topic}', VALUE_FORMAT='JSON', "
                        f"BOOTSTRAP_SERVERS='{self.broker}', OFFSET_RESET='earliest', TRANSACTIONAL=true) "
                        f"AS SELECT id, seq FROM kafka.{src_a};")
            cur.execute(f"INSERT INTO kafka.{stream} SELECT id, seq FROM kafka.{src_b};")
            cur.execute(f"SELECT group_id FROM kafka.__sources WHERE kind = 'insert' AND topic = '{out_topic}';")
            insert_groups = cur.fetchall()
            assert len(insert_groups) == 1, insert_groups
            insert_group = insert_groups[0][0]
        conn.close()
        stream_group = f"otterstax_stream_{stream}"
        self.wait_committed(stream_group, topic_a, p)
        self.wait_committed(insert_group, topic_b, p)

        # Phase C — SIGKILL, then queue unread input for both recovered workers.
        self.kill_server()
        self.produce(topic_a, p, start=p)              # a batch 2: [p, 2p)
        self.produce(topic_b, p, start=b_off + p)      # b batch 2: [b_off+p, b_off+2p)
        print("SIGKILL after the first batches; second batches queued while server stopped")

        # Phase D — restart: recover() relaunches the stream (pass 2) + INSERT INTO
        # query (pass 3); both resume from their Kafka offsets and finish the union.
        self.start_server()
        print("restarted; waiting for the union to complete on the out topic")
        self.wait_committed(stream_group, topic_a, 2 * p, timeout_s=180)
        self.wait_committed(insert_group, topic_b, 2 * p, timeout_s=180)
        total, ids = self.consume_ids(out_topic, timeout_s=180)
        a_ids = sum(1 for i in ids if i < b_off)
        b_ids = sum(1 for i in ids if i >= b_off)
        print(f"fan-in out topic: {total} msgs, {len(ids)} distinct (src_a={a_ids}/{2 * p}, src_b={b_ids}/{2 * p})")
        if len(ids) != expected or a_ids != 2 * p or b_ids != 2 * p:
            raise AssertionError(f"fan-in lost rows after crash: {len(ids)} distinct, expected {expected} "
                                 f"(src_a={a_ids}/{2 * p}, src_b={b_ids}/{2 * p})")
        if total != expected:
            raise AssertionError(f"fan-in produced dupes after crash: {total} msgs for {expected} distinct")
        assert ids == set(range(2 * p)) | set(range(b_off, b_off + 2 * p))
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

        total, ids = self.consume_ids(topic)
        if len(ids) != INSERT_ROWS or total != INSERT_ROWS:
            raise AssertionError(f"INSERT VALUES not atomic: {total} msgs / {len(ids)} distinct, "
                                 f"expected {INSERT_ROWS} (a read-committed consumer must see the full batch)")
        assert ids == set(range(INSERT_ROWS))
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

        # Observe an actual broker record before killing the in-flight SQL call.
        # A fixed sleep cannot distinguish a crash during INSERT from one after it.
        big = CRASH_INSERT_ROWS
        values = ", ".join(f"({i}, {i})" for i in range(big))
        result = {}
        def do_insert():
            try:
                with self._connect() as client:
                    client.execute(f"INSERT INTO kafka.{source} (id, seq) VALUES {values};")
                result["ok"] = True
            except Exception as e:  # connection dropped by the kill — expected
                result["err"] = repr(e)
        observer = Consumer({"bootstrap.servers": self.broker, "group.id": f"inflight_{suffix}",
                             "isolation.level": "read_uncommitted", "enable.auto.commit": False,
                             "fetch.wait.max.ms": 10, "enable.partition.eof": True})
        observer.assign([TopicPartition(topic, 0, 0)])
        t = threading.Thread(target=do_insert)
        observed = False
        try:
            # Complete metadata discovery and the first fetch before INSERT.
            # Otherwise producer completion can race the observer's connection setup.
            deadline = time.monotonic() + WAIT_S
            while True:
                assert time.monotonic() < deadline, "Observer did not reach the empty fixture journal"
                msg = observer.poll(0.1)
                if msg is None:
                    continue
                if msg.error() and msg.error().code() == KafkaError._PARTITION_EOF:
                    assert msg.offset() == 0
                    break
                raise AssertionError(f"Unexpected record/error before INSERT: {msg.error()}")
            with self.producer_quota():
                t.start()
                deadline = time.monotonic() + WAIT_S
                while time.monotonic() < deadline:
                    msg = observer.poll(0.01)
                    if msg is not None:
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                continue
                            raise KafkaException(msg.error())
                        assert t.is_alive(), "INSERT finished before fault injection; no in-flight crash was tested"
                        observed = True
                        self.kill_server()
                        break
                    assert t.is_alive(), f"INSERT ended before any record was observed: {result}"
                assert observed, "INSERT did not reach Kafka before the crash deadline"
        finally:
            observer.close()
        t.join(timeout=15)
        assert not t.is_alive(), "INSERT client did not finish after SIGKILL"
        print(f"SIGKILL after first Kafka record, before SQL completion (client: {result})")

        self.start_server()  # restart (no retry — we just check nothing partial survived)
        total, ids = self.consume_ids(topic, timeout_s=180)
        if total not in (0, big):
            raise AssertionError(f"INSERT VALUES NOT atomic through crash: {total} msgs "
                                 f"(a partial batch survived — expected 0 or {big})")
        if total == big and len(ids) != big:
            raise AssertionError(f"INSERT VALUES committed with dupes/loss: {total} msgs, {len(ids)} distinct")
        assert ids == (set(range(big)) if total else set())
        print(f"✅ INSERT VALUES atomic through crash: consumer saw {total} (∈ {{0, {big}}}, never partial)")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_verifier_reads_trailing_duplicates()
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
    parser.add_argument("--docker-container", default=None, metavar="NAME",
                        help="drive a containerised server instead of a local binary: the crash is "
                             "`docker kill NAME` and recovery is `docker start NAME` (host-driven; "
                             "reaches the published PG 8817 + broker 19092, SQL uses kafka:9092)")
    parser.add_argument("--docker-broker-container", metavar="NAME",
                        help="owned Redpanda fixture container providing rpk for temporary producer throttling")
    args = parser.parse_args()
    try:
        KafkaCrashRecoveryTest(local=args.local, server_bin=args.server_bin,
                               docker_container=args.docker_container,
                               docker_broker_container=args.docker_broker_container).run_all_tests()
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
