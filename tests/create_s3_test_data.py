"""
Generate test data files for S3 backend tests.
Creates sample.parquet, sample.csv, sample.ndjson in tests/scripts/test_data/
"""
import pyarrow as pa
import pyarrow.parquet as pq
import csv
import json
import os

out_dir = "tests/scripts/test_data"
os.makedirs(out_dir, exist_ok=True)

# Parquet: 100 rows events (campaign_id 1..50 for JOIN with MySQL campaigns)
table = pa.table({
    "id": range(1, 101),
    "event_name": [f"event_{i}" for i in range(100)],
    "campaign_id": [(i % 50) + 1 for i in range(100)],
    "timestamp": ["2025-01-01T00:00:00Z"] * 100,
})
pq.write_table(table, f"{out_dir}/sample.parquet")

# CSV: 50 rows users
with open(f"{out_dir}/sample.csv", "w") as f:
    w = csv.writer(f)
    w.writerow(["id", "name", "active"])
    for i in range(1, 51):
        w.writerow([i, f"user_{i}", i % 2 == 0])

# NDJSON: 200 rows logs
with open(f"{out_dir}/sample.ndjson", "w") as f:
    for i in range(200):
        level = ["INFO", "WARN", "ERROR"][i % 3]
        json.dump({"id": i, "level": level, "msg": f"log_{i}"}, f)
        f.write("\n")

print(f"Test data generated in {out_dir}/")
