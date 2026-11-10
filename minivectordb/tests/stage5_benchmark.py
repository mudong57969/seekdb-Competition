"""Paired 10k x 128 benchmark with independent filtered recall and cgroup counters."""

import argparse
import heapq
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import time

from vector_validation import VectorSession, elapsed_sql, vector


def distribution(values):
    ordered = sorted(values)
    return {"count": len(values), "total_ms": sum(values),
            "median_ms": statistics.median(values),
            "p95_ms": ordered[math.ceil(len(values) * .95) - 1],
            "samples_ms": values}


def cgroup(db):
    pid = subprocess.check_output(["docker", "inspect", "-f", "{{.State.Pid}}", db.name], text=True).strip()
    name = Path(f"/proc/{pid}/cgroup").read_text().strip().split("::")[-1]
    root = Path("/sys/fs/cgroup") / name.lstrip("/")
    io = {}
    for line in (root / "io.stat").read_text().splitlines():
        device, *counters = line.split()
        io[device] = {key: int(value) for key, value in (item.split("=") for item in counters)}
    return db.counters() | {"io.stat": io}


def delta(before, after):
    result = {key: after[key] - before[key] for key in ("rchar", "wchar", "read_bytes", "write_bytes")}
    result["io.stat"] = {
        device: {key: value - before["io.stat"].get(device, {}).get(key, 0)
                 for key, value in counters.items()}
        for device, counters in after["io.stat"].items()
    }
    return result


def reference(vectors, queries, responses):
    buckets = [list(range(bucket, len(vectors), 16)) for bucket in range(16)]
    recalls = {"filtered": [], "unfiltered": []}
    for i, (query, actual) in enumerate(zip(queries, responses)):
        eligible = buckets[(i // 2) % 16] if i % 2 else range(len(vectors))
        target = heapq.nsmallest(10, eligible, key=lambda key: (math.dist(vectors[key], query), key))
        assert len(actual) == len(set(actual)) == len(target)
        assert all(key in eligible for key in actual), (i, actual)
        recall = len(set(actual).intersection(target)) / len(target)
        recalls["filtered" if i % 2 else "unfiltered"].append(recall)
        assert actual == target, (i, actual, target)
    return {key: {"mean": statistics.mean(scores), "minimum": min(scores), "queries": len(scores)}
            for key, scores in recalls.items()}


def probes(db, table, query_sql):
    probes = {
        "scalar_decode_scan": "SELECT id FROM t WHERE id < 0",
        "scalar_decode_and_topk": "SELECT id FROM t ORDER BY id DESC LIMIT 10",
        "vector_decode_and_topk": query_sql,
    }
    result = {}
    for label, sql in probes.items():
        result[label] = distribution([elapsed_sql(db, sql)[0] for _ in range(7)])
    warm, advised_cold = [], []
    before = cgroup(db)
    with table.open("rb") as file:
        for _ in range(5):
            os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            ms, _ = elapsed_sql(db, query_sql)
            advised_cold.append(ms)
            ms, _ = elapsed_sql(db, query_sql)
            warm.append(ms)
    result["page_cache"] = {"advised_cold": distribution(advised_cold),
                            "warm": distribution(warm), "paired_io": delta(before, cgroup(db))}
    read_times = []
    for _ in range(7):
        start = time.perf_counter()
        with table.open("rb") as file:
            while file.read(64 * 1024):
                pass
        read_times.append((time.perf_counter() - start) * 1000)
    result["host_warm_read_only"] = distribution(read_times)
    return result


def benchmark(image, output):
    rng = random.Random(20261110)
    vectors = [[rng.randrange(-16, 17) for _ in range(128)] for _ in range(10000)]
    queries = [[rng.randrange(-16, 17) for _ in range(128)] for _ in range(1000)]
    output.mkdir()
    data = output / "data"
    data.mkdir()
    table = data / "table-t.jsonl"
    with VectorSession(image, output, data, "benchmark") as db:
        db.ready()
        db.sql("CREATE TABLE t (id BIGINT PRIMARY KEY, bucket BIGINT, v VECTOR(128))")
        db.sql("CREATE VECTOR INDEX ix ON t(v)")
        before_write = cgroup(db)
        writes = []
        for start in range(0, len(vectors), 100):
            values = ",".join(f"({i},{i % 16},{vector(vectors[i])})" for i in range(start, start + 100))
            ms, result = elapsed_sql(db, "INSERT INTO t VALUES " + values)
            assert result["affected_rows"] == 100
            writes.append(ms)
        after_write = cgroup(db)
        dataset_bytes = table.stat().st_size
        db.mode = "ann"
        filtered, unfiltered, responses = [], [], []
        for i, query in enumerate(queries):
            where = f"WHERE bucket = {(i // 2) % 16}" if i % 2 else ""
            ms, result = elapsed_sql(db, f"SELECT id FROM t {where} ORDER BY L2_DISTANCE(v,{vector(query)}) LIMIT 10")
            (filtered if i % 2 else unfiltered).append(ms)
            responses.append([row[0] for row in result["rows"]])
        after_query = cgroup(db)
        sample_sql = f"SELECT id FROM t ORDER BY L2_DISTANCE(v,{vector(queries[0])}) LIMIT 10"
        parts = probes(db, table, sample_sql)
        mutations = {}
        for name, make_sql in [
            ("insert", lambda i: f"INSERT INTO t VALUES ({10000 + i},0,{vector(vectors[i])})"),
            ("update", lambda i: f"UPDATE t SET v = {vector(vectors[i + 1])} WHERE id = {i}"),
            ("delete", lambda i: f"DELETE FROM t WHERE id = {10000 + i}"),
        ]:
            samples = []
            for i in range(3):
                before = cgroup(db)
                ms, result = elapsed_sql(db, make_sql(i))
                assert result["affected_rows"] == 1
                samples.append({"ms": ms, "io": delta(before, cgroup(db))})
            mutations[name] = samples
        db.shutdown()
    recall = reference(vectors, queries, responses)
    return {"image": image, "rows": len(vectors), "dimensions": 128, "k": 10, "batch_rows": 100,
            "filter_buckets": 16, "dataset_bytes": dataset_bytes,
            "write": distribution(writes), "filtered": distribution(filtered),
            "unfiltered": distribution(unfiltered), "recall_at_10": recall,
            "query_total_ms": sum(filtered) + sum(unfiltered),
            "write_io": delta(before_write, after_write), "query_io": delta(after_write, after_query),
            "after_write": after_write, "after_query": after_query, "probes": parts, "mutations": mutations}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    result = benchmark(args.image, output)
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    for key in ("write", "filtered", "unfiltered"):
        print(key, {name: value for name, value in result[key].items() if name != "samples_ms"})
    print("recall_at_10", result["recall_at_10"])
    print("query_total_ms", result["query_total_ms"])


if __name__ == "__main__":
    main()
