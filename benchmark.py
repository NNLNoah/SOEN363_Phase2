"""
Benchmark the 20 MongoDB queries defined in mongo_queries.py.

For each query, runs N iterations and records:
  - planning_ms:  time to obtain the query plan via explain(queryPlanner)
  - execution_ms: time to run the aggregation and fully drain the cursor
  - total_ms:     planning_ms + execution_ms
  - rows:         number of documents returned

Outputs a CSV row per query with p50 and p99 across the iterations.

Usage:
    python benchmark.py                 # default: 5 iterations, 1 warm-up
    python benchmark.py --iters 30      # more iterations -> more stable p99
    python benchmark.py --out out.csv   # write to file instead of stdout
"""

import argparse
import csv
import sys
import time

import pymongo

from migration import load_env_files, get_mongo_config
from mongo_queries import ALL_QUERIES


CSV_HEADER = [
    "query", "rows",
    "planning_p50_ms", "planning_p99_ms",
    "execution_p50_ms", "execution_p99_ms",
    "total_p50_ms", "total_p99_ms",
    "status",
]


def percentile(values, p):
    """Nearest-rank percentile, 0 <= p <= 100."""
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


def time_explain(db, coll_name, pipeline):
    """Return milliseconds spent obtaining a queryPlanner explain."""
    t0 = time.perf_counter()
    db.command({
        "explain":   {"aggregate": coll_name, "pipeline": pipeline, "cursor": {}},
        "verbosity": "queryPlanner",
    })
    return (time.perf_counter() - t0) * 1000.0


def time_execute(db, coll_name, pipeline):
    """Run the aggregation and fully drain the cursor. Returns (ms, row_count)."""
    t0 = time.perf_counter()
    cursor = db[coll_name].aggregate(pipeline, allowDiskUse=True)
    count = 0
    for _ in cursor:
        count += 1
    return (time.perf_counter() - t0) * 1000.0, count


def benchmark_one(db, name, fn, iterations, warmup):
    coll_name, pipeline = fn()

    try:
        for _ in range(warmup):
            time_execute(db, coll_name, pipeline)
    except Exception as exc:
        return _error_row(name, exc)

    plan_ms_list, exec_ms_list, total_ms_list = [], [], []
    row_count = 0

    try:
        for _ in range(iterations):
            plan_ms         = time_explain(db, coll_name, pipeline)
            exec_ms, n_rows = time_execute(db, coll_name, pipeline)
            row_count = n_rows
            plan_ms_list.append(plan_ms)
            exec_ms_list.append(exec_ms)
            total_ms_list.append(plan_ms + exec_ms)
    except Exception as exc:
        return _error_row(name, exc)

    return {
        "query":            name,
        "rows":             row_count,
        "planning_p50_ms":  f"{percentile(plan_ms_list, 50):.3f}",
        "planning_p99_ms":  f"{percentile(plan_ms_list, 99):.3f}",
        "execution_p50_ms": f"{percentile(exec_ms_list, 50):.3f}",
        "execution_p99_ms": f"{percentile(exec_ms_list, 99):.3f}",
        "total_p50_ms":     f"{percentile(total_ms_list, 50):.3f}",
        "total_p99_ms":     f"{percentile(total_ms_list, 99):.3f}",
        "status":           "OK",
    }


def _error_row(name, exc):
    print(f"  {name}: FAILED -> {exc}", file=sys.stderr)
    return {
        "query": name, "rows": 0,
        "planning_p50_ms": "0.000", "planning_p99_ms": "0.000",
        "execution_p50_ms": "0.000", "execution_p99_ms": "0.000",
        "total_p50_ms": "0.000", "total_p99_ms": "0.000",
        "status": f"ERROR: {type(exc).__name__}",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iters",  type=int, default=5, help="measured iterations per query")
    ap.add_argument("--warmup", type=int, default=1,  help="warm-up iterations per query")
    ap.add_argument("--out",    type=str, default=None, help="CSV output file (default: stdout)")
    args = ap.parse_args()

    load_env_files()
    mongo_uri, mongo_db_name = get_mongo_config()

    print(f"Connecting to MongoDB at {mongo_uri} / db={mongo_db_name} ...", file=sys.stderr)
    client = pymongo.MongoClient(mongo_uri)
    db = client[mongo_db_name]
    client.admin.command("ping")

    print(f"Running {len(ALL_QUERIES)} queries x {args.iters} iterations "
          f"(+{args.warmup} warm-up)...", file=sys.stderr)

    rows = []
    for i, fn in enumerate(ALL_QUERIES, 1):
        name = f"q{i}.sql"
        print(f"  {name} ...", file=sys.stderr, end="", flush=True)
        row = benchmark_one(db, name, fn, args.iters, args.warmup)
        rows.append(row)
        print(f" {row['status']}  rows={row['rows']}  "
              f"exec_p50={row['execution_p50_ms']}ms", file=sys.stderr)

    client.close()

    out_stream = open(args.out, "w", newline="") if args.out else sys.stdout
    try:
        writer = csv.DictWriter(out_stream, fieldnames=CSV_HEADER)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    finally:
        if args.out:
            out_stream.close()
            print(f"Wrote results to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
