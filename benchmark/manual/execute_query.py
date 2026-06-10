#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax
#
# Executes a single SQL query against OtterStax and writes a summary file.
# Invoked by run_query.sh; not meant to be called directly.
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument("--frontend", required=True, choices=["mysql", "postgres", "arrow"])
parser.add_argument("--host", default="bench_otterstax")
parser.add_argument("--port", type=int, default=0)
parser.add_argument("--out-dir", default="/results")
parser.add_argument("--query-name", default="query")
parser.add_argument("sql")
args = parser.parse_args()

if args.port == 0:
    args.port = {"mysql": 8816, "postgres": 8817, "arrow": 8815}[args.frontend]

git_commit = os.environ.get("GIT_COMMIT", "unknown")
generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

rows = []
elapsed_ms = 0.0
error = None

t0 = time.perf_counter()
try:
    if args.frontend == "mysql":
        import mysql.connector
        conn = mysql.connector.connect(
            host=args.host, port=args.port, user="testuser", password="testpass"
        )
        cur = conn.cursor(buffered=True)
        cur.execute(args.sql)
        rows = cur.fetchall() or []
        col_names = [d[0] for d in (cur.description or [])]
        cur.close()
        conn.close()

    elif args.frontend == "postgres":
        import psycopg2
        conn = psycopg2.connect(
            host=args.host, port=args.port,
            user="testuser", password="testpass", dbname="default"
        )
        cur = conn.cursor()
        cur.execute(args.sql)
        rows = cur.fetchall() if cur.description else []
        col_names = [d[0] for d in (cur.description or [])]
        cur.close()
        conn.close()

    elif args.frontend == "arrow":
        from flightsql import FlightSQLClient
        client = FlightSQLClient(host=args.host, port=args.port, insecure=True)
        info = client.execute(args.sql)
        table = client.do_get(info.endpoints[0].ticket).read_all()
        rows = table.to_pylist()
        col_names = table.schema.names

except Exception as exc:
    error = str(exc)

elapsed_ms = (time.perf_counter() - t0) * 1000

# Print tabular result to stdout
if error:
    print(f"ERROR: {error}", file=sys.stderr)
else:
    if col_names:
        widths = [max(len(c), max((len(str(r[i])) for r in rows), default=0))
                  for i, c in enumerate(col_names)]
        sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
        fmt = "| " + " | ".join(f"{{:<{w}}}" for w in widths) + " |"
        print(sep)
        print(fmt.format(*col_names))
        print(sep)
        for row in rows:
            print(fmt.format(*[str(v) for v in row]))
        print(sep)
    print(f"\n{len(rows)} row(s)  {elapsed_ms:.1f} ms")

# Write summary.md — one entry per query invocation
os.makedirs(args.out_dir, exist_ok=True)
summary_path = os.path.join(args.out_dir, "summary.md")
status = "ERROR" if error else "OK"
with open(summary_path, "a") as f:
    f.write(f"Generated  : {generated}\n")
    f.write(f"Commit     : {git_commit}\n")
    f.write(f"{args.query_name:<10} : {status}\n")
    if error:
        f.write(f"error      : {error}\n")
    else:
        f.write(f"rows       : {len(rows)}\n")
    f.write(f"elapsed_ms : {elapsed_ms:.1f}\n")
    if not error:
        f.write(f"\n```sql\n{args.sql}\n```\n")
    f.write("\n---\n\n")

# Write JSON result
result_path = os.path.join(args.out_dir, f"{args.query_name}.json")
with open(result_path, "w") as f:
    json.dump({
        "generated": generated,
        "commit": git_commit,
        "frontend": args.frontend,
        "query_name": args.query_name,
        "sql": args.sql,
        "status": "ERROR" if error else "OK",
        "error": error,
        "rows": len(rows) if not error else 0,
        "elapsed_ms": round(elapsed_ms, 1),
    }, f, indent=2)

sys.exit(1 if error else 0)
