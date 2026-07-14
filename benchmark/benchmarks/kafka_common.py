#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Runners for the Kafka benchmarks: kafka_ingest / kafka_produce / kafka_stream.

One runner per feature, registered in ``_RUNNERS``; ``kafka_main`` dispatches by
test name — the same shape as ``external_common``. Every Kafka statement the
benchmarks issue is built by a ``sql_*`` helper in the "Kafka SQL" section below,
so there is one place to look for "what query does this fire"; the runners only
orchestrate and time. Frontend-agnostic via the ``connect(host, port)`` the
external_* benchmarks use.

  kafka_ingest   SOURCE ingest throughput — sub-tests `ingest_alo` (at-least-once)
                 and `ingest_eos` (TRANSACTIONAL=true). Each rep CREATE SOURCEs
                 over the pre-seeded topic under a fresh name and times
                 CREATE SOURCE -> count(*) == num_records. The alo/eos delta is
                 the cost of exactly-once.
  kafka_produce  Write path — INSERT INTO kafka.<src> VALUES <batch> produces the
                 batch to the object's topic; the timed statement is one
                 transactional produce+flush of PRODUCE_ROWS rows.
  kafka_stream   Continuous-query throughput — CREATE STREAM AS SELECT ... FROM
                 the source, then consume the stream's output topic until every
                 row has flowed through (poll -> transform -> produce).
"""

import os
import sys
import time
import uuid
from pathlib import Path

import yaml
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).parent))
from common import BenchmarkResult, RunResult, write_txt_result

# ── dataset schema (must match data/generate_kafka_fixtures.py) ────────────────
COLUMNS = ["id", "campaign_id", "event_type", "amount", "ts"]
SOURCE_COLUMNS = "id BIGINT, campaign_id BIGINT, event_type VARCHAR, amount DOUBLE, ts BIGINT"

# ── tunables ───────────────────────────────────────────────────────────────────
INGEST_TIMEOUT_S = 180.0
STREAM_TIMEOUT_S = 180.0
POLL_INTERVAL_S = 0.05
PRODUCE_ROWS = 5000    # rows per INSERT ... VALUES batch (kafka_produce)


def _load_kafka_cfg():
    cfg_path = Path(os.getenv("BENCH_YAML", "/app/bench.yaml"))
    cfg = {}
    if cfg_path.exists():
        with cfg_path.open() as f:
            cfg = yaml.safe_load(f) or {}
    kafka = cfg.get("kafka", {})
    return {"num_records": kafka.get("num_records", 50000),
            "topic": kafka.get("topic", "bench_events")}


# ── Kafka SQL: every statement the benchmarks issue is built here ──────────────
def sql_create_source(src, topic, broker, transactional=False):
    txn = ", TRANSACTIONAL=true" if transactional else ""
    return (f"CREATE SOURCE {src} ({SOURCE_COLUMNS}) "
            f"WITH (KAFKA_TOPIC='{topic}', VALUE_FORMAT='JSON', "
            f"BOOTSTRAP_SERVERS='{broker}', OFFSET_RESET='earliest'{txn})")


def sql_create_stream(name, out_topic, src, broker):
    # WHERE id >= 0 passes every row, so the output count == num_records is known
    # up front (a selective filter would need the exact post-filter count first).
    return (f"CREATE STREAM {name} "
            f"WITH (KAFKA_TOPIC='{out_topic}', VALUE_FORMAT='JSON', "
            f"BOOTSTRAP_SERVERS='{broker}', OFFSET_RESET='earliest') "
            f"AS SELECT {', '.join(COLUMNS)} FROM kafka.{src} WHERE id >= 0")


def sql_insert_values(src, rows):
    cols = ", ".join(COLUMNS)
    vals = ", ".join(f"({r['id']}, {r['campaign_id']}, '{r['event_type']}', "
                     f"{r['amount']}, {r['ts']})" for r in rows)
    return f"INSERT INTO kafka.{src} ({cols}) VALUES {vals}"


def sql_count(src):        return f"SELECT count(*) FROM kafka.{src}"
def sql_drop_source(src):  return f"DROP SOURCE {src}"
def sql_drop_stream(name): return f"DROP STREAM {name}"


# ── low-level statement execution over a persistent connection ─────────────────
def _exec(conn, sql, fetch=False, ignore=False):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall() if fetch else []
    except Exception:
        if ignore:
            return []
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _count(conn, src):
    """count(*) over kafka.<src>; an empty table can come back schemaless
    (description is None) — treat as 0 and keep polling."""
    cur = conn.cursor()
    try:
        cur.execute(sql_count(src))
        if cur.description is None:
            return 0
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0
    finally:
        try:
            cur.close()
        except Exception:
            pass


# ── broker-side helpers (kafka_stream owns its output topic + a verify consumer) ─
def _create_topic(broker, topic):
    admin = AdminClient({"bootstrap.servers": broker})
    for _name, fut in admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    ).items():
        try:
            fut.result(timeout=15)
        except Exception as e:  # noqa: BLE001
            if "already exists" not in str(e).lower():
                raise


def _consume_count(broker, topic, expected, timeout_s):
    """Count messages on `topic` (fresh group, from earliest) until `expected` or
    timeout. Returns the count seen."""
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "bench_verify_" + uuid.uuid4().hex[:8],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    seen, deadline = 0, time.time() + timeout_s
    try:
        while seen < expected and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error() is not None:
                continue
            seen += 1
    finally:
        consumer.close()
    return seen


# ── shared ingest step (kafka_ingest alo + eos) ────────────────────────────────
def _ingest_source(conn, src, topic, broker, expected, transactional):
    """CREATE SOURCE over the pre-seeded topic; block until `expected` rows land.
    Returns (elapsed_ms, rows); raises on timeout."""
    _exec(conn, sql_drop_source(src), ignore=True)  # clear a leftover, untimed
    t0 = time.perf_counter()
    _exec(conn, sql_create_source(src, topic, broker, transactional))
    deadline, rows = t0 + INGEST_TIMEOUT_S, 0
    while time.perf_counter() < deadline:
        rows = _count(conn, src)
        if rows >= expected:
            break
        time.sleep(POLL_INTERVAL_S)
    elapsed = (time.perf_counter() - t0) * 1000
    if rows < expected:
        raise TimeoutError(f"ingested {rows}/{expected} rows in {INGEST_TIMEOUT_S}s")
    return elapsed, rows


def _synth_rows(n):
    """Deterministic rows for the produce batch (schema == COLUMNS)."""
    types = ("view", "click", "purchase")
    return [{"id": i, "campaign_id": (i % 1000) + 1, "event_type": types[i % 3],
             "amount": round(i * 0.5, 2), "ts": 1_700_000_000_000 + i}
            for i in range(n)]


def _record(result, sub, rep, reps, elapsed, rows, per_sec_label=None):
    result.runs.append(RunResult(sub, rep, elapsed, rows))
    extra = ""
    if per_sec_label and elapsed > 0:
        extra = f", {rows / (elapsed / 1000):,.0f} {per_sec_label}"
    print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({rows} rows{extra})")


# ── runners ────────────────────────────────────────────────────────────────────
def run_kafka_ingest(frontend, connect, host, port, broker, reps, out_dir):
    cfg = _load_kafka_cfg()
    topic, expected = cfg["topic"], cfg["num_records"]
    result = BenchmarkResult("kafka_ingest", frontend, reps)
    tag = uuid.uuid4().hex[:6]
    conn = connect(host, port)
    try:
        # A fresh source name per rep => fresh consumer group (group_id defaults to
        # otterstax_<name>) + OFFSET_RESET='earliest' => each rep re-reads the whole
        # topic, i.e. an independent cold ingest of the same pre-seeded dataset.
        for sub, txn in (("ingest_alo", False), ("ingest_eos", True)):
            result.queries[sub] = sql_create_source(f"bench_{sub}_{frontend}_<rep>",
                                                    topic, broker, txn)
            for rep in range(1, reps + 1):
                src = f"bench_{sub}_{frontend}_{tag}_{rep}"
                try:
                    elapsed, rows = _ingest_source(conn, src, topic, broker, expected, txn)
                    _record(result, sub, rep, reps, elapsed, rows, "rec/s")
                except Exception as exc:  # noqa: BLE001
                    result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                    print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
                finally:
                    _exec(conn, sql_drop_source(src), ignore=True)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_kafka_produce(frontend, connect, host, port, broker, reps, out_dir):
    result = BenchmarkResult("kafka_produce", frontend, reps)
    tag = uuid.uuid4().hex[:6]
    src = f"bench_produce_{frontend}_{tag}"
    prod_topic = f"bench_produce_topic_{tag}"
    rows = _synth_rows(PRODUCE_ROWS)
    sub = "produce"
    conn = connect(host, port)
    try:
        # Untimed setup: an object to produce into (INSERT publishes to its topic).
        try:
            _create_topic(broker, prod_topic)
            _exec(conn, sql_create_source(src, prod_topic, broker))
        except Exception as exc:  # noqa: BLE001
            for rep in range(1, reps + 1):
                result.runs.append(RunResult(sub, rep, 0, 0, f"setup: {exc}"))
            print(f"  produce setup ERROR {exc}")
            write_txt_result(result, out_dir)
            return result

        result.queries[sub] = f"INSERT INTO kafka.{src} (...) VALUES <{PRODUCE_ROWS} rows>"
        insert_sql = sql_insert_values(src, rows)
        for rep in range(1, reps + 1):
            try:
                t0 = time.perf_counter()
                _exec(conn, insert_sql)  # produce + flush; returns no rows
                elapsed = (time.perf_counter() - t0) * 1000
                _record(result, sub, rep, reps, elapsed, PRODUCE_ROWS, "rec/s")
            except Exception as exc:  # noqa: BLE001
                result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
        _exec(conn, sql_drop_source(src), ignore=True)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_kafka_stream(frontend, connect, host, port, broker, reps, out_dir):
    cfg = _load_kafka_cfg()
    topic, expected = cfg["topic"], cfg["num_records"]
    result = BenchmarkResult("kafka_stream", frontend, reps)
    tag = uuid.uuid4().hex[:6]
    sub = "stream"
    result.queries[sub] = ("CREATE STREAM ... AS SELECT ... FROM kafka.<src> WHERE id >= 0"
                           "  (timed until num_records rows reach the output topic)")
    conn = connect(host, port)
    try:
        for rep in range(1, reps + 1):
            # Fresh source/stream/out-topic per rep: the stream reads the pre-seeded
            # source topic from earliest and republishes every row to its own output
            # topic, which we drain to measure end-to-end stream throughput.
            src = f"bench_stream_src_{frontend}_{tag}_{rep}"
            stream = f"bench_stream_{frontend}_{tag}_{rep}"
            out_topic = f"bench_stream_out_{frontend}_{tag}_{rep}"
            try:
                _create_topic(broker, out_topic)
                _exec(conn, sql_create_source(src, topic, broker))  # register the source (untimed)
                t0 = time.perf_counter()
                _exec(conn, sql_create_stream(stream, out_topic, src, broker))
                got = _consume_count(broker, out_topic, expected, STREAM_TIMEOUT_S)
                elapsed = (time.perf_counter() - t0) * 1000
                if got < expected:
                    raise TimeoutError(f"streamed {got}/{expected} rows in {STREAM_TIMEOUT_S}s")
                _record(result, sub, rep, reps, elapsed, got, "rec/s")
            except Exception as exc:  # noqa: BLE001
                result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
            finally:
                _exec(conn, sql_drop_stream(stream), ignore=True)
                _exec(conn, sql_drop_source(src), ignore=True)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


_RUNNERS = {
    "kafka_ingest": run_kafka_ingest,
    "kafka_produce": run_kafka_produce,
    "kafka_stream": run_kafka_stream,
}


def kafka_main(test_name, frontend, default_port, connect):
    """CLI entry shared by the thin benchmarks/{mysql,postgres}/kafka_*.py scripts.
    Mirrors external_common.external_main so run_benchmark.sh drives it identically."""
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--host", default="bench_otterstax")
    p.add_argument("--port", type=int, default=default_port)
    p.add_argument("--repetitions", type=int, default=10)
    p.add_argument("--out-dir", type=Path, default=Path(f"benchmark_results/{frontend}"))
    p.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "bench_kafka:9092"))
    p.add_argument("--local", action="store_true",
                   help="host=127.0.0.1 + broker=127.0.0.1:19092 (standalone-redpanda run)")
    args = p.parse_args()

    host = "127.0.0.1" if args.local else args.host
    broker = "127.0.0.1:19092" if args.local else args.broker
    print(f"=== {test_name} ({frontend}) reps={args.repetitions} host={host} broker={broker} ===")
    result = _RUNNERS[test_name](frontend, connect, host, args.port, broker,
                                 args.repetitions, Path(args.out_dir))
    if any(r.error for r in result.runs):
        sys.exit(1)
