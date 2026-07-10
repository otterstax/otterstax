#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
"""Runner for the s3/file external-table benchmarks.

Three workloads, each measured per ``source`` (local ``file`` mount + ``s3``
MinIO bucket) so file vs s3 are directly comparable:

  external_load   CREATE EXTERNAL TABLE — load a fixture into otterbrix-internal
                  storage.  One sub-test per (source × format).  The table is
                  DROPped (untimed) before each repetition so every rep measures
                  a cold load.
  external_join   regions(parquet) ⋈ web_events(csv) on campaign_id.  Both
                  tables are loaded once (untimed setup); each rep times the
                  internal (otterbrix-on-otterbrix) join — no remote backend.
  external_dump   COPY (SELECT * FROM <loaded>) TO <target> — one sub-test per
                  (target source × format).  The source table is loaded once
                  (untimed); each rep times the writer + upload.

  external_join_cross  external `regions` (s3/file) ⋈ otterbrix-internal
                  `weights` (CREATE TABLE + INSERT VALUES).  Mirrors the
                  supported shape in tests/test_mysql_join_otb_local_s3.py — the
                  s3/file source is loaded into the engine, then joined against
                  a hand-built engine-native table.  One sub-test per source.
  external_join_all   three origins at once: s3 parquet `regions` ⋈ file csv
                  `web_events` ⋈ otterbrix-internal `weights`, all on
                  campaign_id.  Models the "everything joined together" shape.

The join keys are int64 on every side (parquet/csv loaders emit int64; the
engine table declares `campaign_id bigint`) — a direct backend.campaign_id
(int32) ⋈ s3.campaign_id (int64) silently drops all rows (see FIX_JOIN.md), so
these benchmarks deliberately resolve every side to otterbrix-internal storage
first, exactly like the tests they are modelled on.

Output reuses common.BenchmarkResult / write_txt_result, so results land beside
the other benchmarks and feed generate_summary.py unchanged.

Only loading, internal joins and dumps here — no remote-backend / federated
JOIN shapes, per the s3/file feature surface.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from common import BenchmarkResult, RunResult, write_txt_result

# In-container view of the seeded MinIO bucket and the /fixtures mount.
# Mirrors run_benchmark.sh (_register_s3_credentials) + compose_minio.yml.
S3_ALIAS = "bench_minio"
S3_BUCKET = "bench-bucket"
FILE_FIXTURE_DIR = "/fixtures"

# format -> (fixture filename, logical table name)
FORMATS = {
    "parquet": ("regions.parquet", "regions"),
    "csv": ("web_events.csv", "web_events"),
    "ndjson": ("campaigns.ndjson", "campaigns"),
}


# ── SQL fragment builders (mirror tests/external_helpers.py) ───────────────────
def _location(source, filename):
    if source == "s3":
        return f"s3://{S3_BUCKET}/{filename}"
    return f"{FILE_FIXTURE_DIR}/{filename}"


def _create_with(source, location, fmt):
    alias = f"s3_alias = '{S3_ALIAS}', " if source == "s3" else ""
    return f"WITH ({alias}location = '{location}', format = '{fmt}')"


def _copy_with(source, fmt):
    alias = f"s3_alias = '{S3_ALIAS}', " if source == "s3" else ""
    return f"WITH ({alias}format = '{fmt}')"


def _copy_target(source, name, fmt):
    filename = f"{name}.{fmt}"
    if source == "s3":
        return f"s3://{S3_BUCKET}/exported/{filename}"
    return f"/tmp/bench_copy_{filename}"


# ── statement execution over a persistent connection ──────────────────────────
def _exec(conn, sql, fetch=False, ignore=False):
    """Run one statement.  fetch=True drains and returns the rows; ignore=True
    swallows errors (used for best-effort DROP between repetitions)."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall() if fetch else []
        return rows
    except Exception:
        if ignore:
            return []
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


def _drop(conn, db, table):
    _exec(conn, f"DROP TABLE {db}.{table}", ignore=True)


def _load_external(conn, db, table, source, fmt):
    filename, _ = FORMATS[fmt]
    location = _location(source, filename)
    _exec(conn, f"CREATE EXTERNAL TABLE {db}.{table} {_create_with(source, location, fmt)}")


# Cap on how many campaign_ids the hand-built `weights` table covers.  Keeps the
# INSERT VALUES statement and the resulting join cardinality bounded regardless
# of the (configurable) fixture scale.
WEIGHTS_CAMPAIGN_CAP = 200


def _campaign_cap(conn, db, regions_tbl):
    """Largest campaign_id to populate weights with — min(max in regions, cap)
    so every weights row matches and the join cardinality stays predictable."""
    try:
        mx = int(_exec(conn, f"SELECT max(campaign_id) FROM {db}.{regions_tbl}", fetch=True)[0][0])
    except Exception:  # noqa: BLE001
        mx = WEIGHTS_CAMPAIGN_CAP
    return max(1, min(mx, WEIGHTS_CAMPAIGN_CAP))


def _setup_weights(conn, db, table, max_campaign):
    """Build an otterbrix-internal table by hand (CREATE TABLE + INSERT VALUES),
    one row per campaign_id 1..max_campaign.  campaign_id is bigint so its width
    matches the int64 the parquet/csv loaders expose on the external side."""
    _exec(conn, f"CREATE TABLE {db}.{table} ("
                f"  campaign_id bigint, label string, weight double)")
    values = ", ".join(f"({cid}, 'w{cid}', {float(cid)})"
                       for cid in range(1, max_campaign + 1))
    _exec(conn, f"INSERT INTO {db}.{table} (campaign_id, label, weight) VALUES {values}")


# ── workloads ─────────────────────────────────────────────────────────────────
def run_external_load(frontend, connect, host, port, sources, reps, out_dir):
    db = f"bench_ext_load_{frontend}"
    result = BenchmarkResult("external_load", frontend, reps)
    conn = connect(host, port)
    try:
        for source in sources:
            for fmt, (filename, name) in FORMATS.items():
                sub = f"{source}_{fmt}"
                table = f"{name}_{source}"
                location = _location(source, filename)
                create_sql = f"CREATE EXTERNAL TABLE {db}.{table} {_create_with(source, location, fmt)}"
                result.queries[sub] = create_sql
                for rep in range(1, reps + 1):
                    _drop(conn, db, table)  # untimed — ensure a cold load
                    try:
                        t0 = time.perf_counter()
                        _exec(conn, create_sql)
                        elapsed = (time.perf_counter() - t0) * 1000
                        rows = int(_exec(conn, f"SELECT count(*) FROM {db}.{table}", fetch=True)[0][0])
                        result.runs.append(RunResult(sub, rep, elapsed, rows))
                        print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({rows} rows loaded)")
                    except Exception as exc:  # noqa: BLE001
                        result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                        print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
                _drop(conn, db, table)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_external_join(frontend, connect, host, port, sources, reps, out_dir):
    db = f"bench_ext_join_{frontend}"
    result = BenchmarkResult("external_join", frontend, reps)
    conn = connect(host, port)
    try:
        for source in sources:
            sub = f"{source}_join"
            r_tbl, e_tbl = f"regions_{source}", f"web_events_{source}"
            # Untimed setup: load both external tables into otterbrix storage.
            try:
                _drop(conn, db, r_tbl)
                _drop(conn, db, e_tbl)
                _load_external(conn, db, r_tbl, source, "parquet")
                _load_external(conn, db, e_tbl, source, "csv")
            except Exception as exc:  # noqa: BLE001
                for rep in range(1, reps + 1):
                    result.runs.append(RunResult(sub, rep, 0, 0, f"setup: {exc}"))
                print(f"  [{sub}] setup ERROR {exc}")
                continue
            join_sql = (
                f"SELECT r.region_id, r.campaign_id, r.country, e.event_id, e.value"
                f" FROM {db}.{r_tbl} r"
                f" JOIN {db}.{e_tbl} e ON r.campaign_id = e.campaign_id"
            )
            result.queries[sub] = join_sql
            for rep in range(1, reps + 1):
                try:
                    t0 = time.perf_counter()
                    rows = _exec(conn, join_sql, fetch=True)
                    elapsed = (time.perf_counter() - t0) * 1000
                    result.runs.append(RunResult(sub, rep, elapsed, len(rows)))
                    print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({len(rows)} rows)")
                except Exception as exc:  # noqa: BLE001
                    result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                    print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
            _drop(conn, db, r_tbl)
            _drop(conn, db, e_tbl)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_external_dump(frontend, connect, host, port, sources, reps, out_dir):
    db = f"bench_ext_dump_{frontend}"
    result = BenchmarkResult("external_dump", frontend, reps)
    conn = connect(host, port)
    src_tbl = "dump_src"
    try:
        # Untimed setup: load the fact table once from the local file so the
        # dump benchmark isolates the writer + upload, not the s3 read.
        try:
            _drop(conn, db, src_tbl)
            _load_external(conn, db, src_tbl, "file", "csv")
        except Exception as exc:  # noqa: BLE001
            for source in sources:
                for fmt in FORMATS:
                    for rep in range(1, reps + 1):
                        result.runs.append(RunResult(f"{source}_{fmt}", rep, 0, 0, f"setup: {exc}"))
            print(f"  dump setup ERROR {exc}")
            write_txt_result(result, out_dir)
            return result

        for source in sources:
            for fmt in FORMATS:
                sub = f"{source}_{fmt}"
                target = _copy_target(source, "web_events", fmt)
                copy_sql = f"COPY (SELECT * FROM {db}.{src_tbl}) TO '{target}' {_copy_with(source, fmt)}"
                result.queries[sub] = copy_sql
                for rep in range(1, reps + 1):
                    try:
                        t0 = time.perf_counter()
                        _exec(conn, copy_sql)
                        elapsed = (time.perf_counter() - t0) * 1000
                        result.runs.append(RunResult(sub, rep, elapsed, 0))
                        print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms")
                    except Exception as exc:  # noqa: BLE001
                        result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                        print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
        _drop(conn, db, src_tbl)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_external_join_cross(frontend, connect, host, port, sources, reps, out_dir):
    db = f"bench_ext_jcross_{frontend}"
    result = BenchmarkResult("external_join_cross", frontend, reps)
    conn = connect(host, port)
    try:
        for source in sources:
            sub = f"{source}_cross"
            r_tbl, w_tbl = f"regions_{source}", f"weights_{source}"
            # Untimed setup: load the external regions, hand-build weights.
            try:
                _drop(conn, db, r_tbl)
                _drop(conn, db, w_tbl)
                _load_external(conn, db, r_tbl, source, "parquet")
                _setup_weights(conn, db, w_tbl, _campaign_cap(conn, db, r_tbl))
            except Exception as exc:  # noqa: BLE001
                for rep in range(1, reps + 1):
                    result.runs.append(RunResult(sub, rep, 0, 0, f"setup: {exc}"))
                print(f"  [{sub}] setup ERROR {exc}")
                continue
            join_sql = (
                f"SELECT w.campaign_id, w.label, w.weight,"
                f" r.region_id, r.region_name, r.country"
                f" FROM {db}.{w_tbl} w"
                f" JOIN {db}.{r_tbl} r ON w.campaign_id = r.campaign_id"
            )
            result.queries[sub] = join_sql
            for rep in range(1, reps + 1):
                try:
                    t0 = time.perf_counter()
                    rows = _exec(conn, join_sql, fetch=True)
                    elapsed = (time.perf_counter() - t0) * 1000
                    result.runs.append(RunResult(sub, rep, elapsed, len(rows)))
                    print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({len(rows)} rows)")
                except Exception as exc:  # noqa: BLE001
                    result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                    print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
            _drop(conn, db, r_tbl)
            _drop(conn, db, w_tbl)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


def run_external_join_all(frontend, connect, host, port, sources, reps, out_dir):
    db = f"bench_ext_jall_{frontend}"
    result = BenchmarkResult("external_join_all", frontend, reps)
    conn = connect(host, port)
    # Mix all three origins: prefer s3 parquet + file csv when both are present,
    # otherwise fall back to whatever source the caller allowed.
    regions_src = "s3" if "s3" in sources else sources[0]
    events_src = "file" if "file" in sources else sources[0]
    sub = f"{regions_src}parquet_{events_src}csv_internal"
    r_tbl, e_tbl, w_tbl = "regions_all", "web_events_all", "weights_all"
    try:
        try:
            _drop(conn, db, r_tbl)
            _drop(conn, db, e_tbl)
            _drop(conn, db, w_tbl)
            _load_external(conn, db, r_tbl, regions_src, "parquet")
            _load_external(conn, db, e_tbl, events_src, "csv")
            _setup_weights(conn, db, w_tbl, _campaign_cap(conn, db, r_tbl))
        except Exception as exc:  # noqa: BLE001
            for rep in range(1, reps + 1):
                result.runs.append(RunResult(sub, rep, 0, 0, f"setup: {exc}"))
            print(f"  [{sub}] setup ERROR {exc}")
            write_txt_result(result, out_dir)
            return result
        join_sql = (
            f"SELECT w.campaign_id, w.label,"
            f" r.region_id, r.country, e.event_id, e.value"
            f" FROM {db}.{w_tbl} w"
            f" JOIN {db}.{r_tbl} r ON w.campaign_id = r.campaign_id"
            f" JOIN {db}.{e_tbl} e ON w.campaign_id = e.campaign_id"
        )
        result.queries[sub] = join_sql
        for rep in range(1, reps + 1):
            try:
                t0 = time.perf_counter()
                rows = _exec(conn, join_sql, fetch=True)
                elapsed = (time.perf_counter() - t0) * 1000
                result.runs.append(RunResult(sub, rep, elapsed, len(rows)))
                print(f"  [{sub}] rep {rep}/{reps}: {elapsed:.1f} ms ({len(rows)} rows)")
            except Exception as exc:  # noqa: BLE001
                result.runs.append(RunResult(sub, rep, 0, 0, str(exc)))
                print(f"  [{sub}] rep {rep}/{reps}: ERROR {exc}")
        _drop(conn, db, r_tbl)
        _drop(conn, db, e_tbl)
        _drop(conn, db, w_tbl)
    finally:
        conn.close()
    write_txt_result(result, out_dir)
    return result


_RUNNERS = {
    "external_load": run_external_load,
    "external_join": run_external_join,
    "external_dump": run_external_dump,
    "external_join_cross": run_external_join_cross,
    "external_join_all": run_external_join_all,
}


def external_main(test_name, frontend, default_port, connect):
    """CLI entry point shared by the per-frontend external_*.py scripts.
    Mirrors common.benchmark_main so run_benchmark.sh drives it identically."""
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--host", default="bench_otterstax")
    p.add_argument("--port", type=int, default=default_port)
    p.add_argument("--repetitions", type=int, default=10)
    p.add_argument("--out-dir", type=Path, default=Path(f"benchmark_results/{frontend}"))
    p.add_argument("--sources", default="file,s3",
                   help="Comma-separated subset of: file,s3 (default: both)")
    args = p.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    print(f"=== {test_name} ({frontend}) reps={args.repetitions} sources={sources} ===")
    result = _RUNNERS[test_name](frontend, connect, args.host, args.port,
                                 sources, args.repetitions, Path(args.out_dir))
    if any(r.error for r in result.runs):
        sys.exit(1)
