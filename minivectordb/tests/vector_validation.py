"""Exercise vector visibility and measure a synthetic exact-search workload in Docker."""

import argparse
import json
import math
from pathlib import Path
import random
import statistics
import struct
import subprocess
import time

from crash_recovery import Session


class VectorSession(Session):
    mode = "exact"

    def send(self, op, sql=None):
        self.sequence += 1
        request = {"id": self.sequence, "op": op}
        if sql is not None:
            request.update(sql=sql, search_mode=self.mode)
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()

    def rejected(self, sql, code):
        self.send("execute", sql)
        response = self.receive()
        assert response["id"] == self.sequence and response["ok"] is False, response
        assert response["error"]["code"] == code, response

    def counters(self):
        result = self.memory()
        pid = subprocess.check_output(
            ["docker", "inspect", "-f", "{{.State.Pid}}", self.name], text=True
        ).strip()
        io = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/io").read_text().splitlines())
        return result | {key: int(io[key]) for key in ("rchar", "wchar")}


def vector(values):
    return "'" + json.dumps(values, separators=(",", ":")) + "'"


def f32(value):
    return struct.unpack("<f", struct.pack("<f", value))[0]


def compare_with_reference(db, vectors, query, limit, where=""):
    literal = vector(query)
    sql = (f"SELECT id,L2_DISTANCE(v,{literal}) AS d FROM t {where} "
           f"ORDER BY L2_DISTANCE(v,{literal}) LIMIT {limit}")
    rows = db.sql(sql)["rows"]
    target = tuple(map(f32, query))
    expected = sorted((math.dist(tuple(map(f32, point)), target), key)
                      for key, point in vectors.items())[:limit]
    assert [row[0] for row in rows] == [key for _, key in expected], (rows, expected)
    for (_, actual), (distance, _) in zip(rows, expected):
        assert math.isclose(actual, distance, rel_tol=1e-12, abs_tol=1e-12), (actual, distance)
    return rows


def consistency(image, out):
    data = out / "consistency-data"
    data.mkdir()
    keep = {9: [1, 0], 2: [-1, 0], 7: [0, 1]}
    where = "WHERE category = 'keep'"
    with VectorSession(image, out, data, "vector-before-crash") as db:
        db.ready()
        db.sql("CREATE TABLE t (id BIGINT PRIMARY KEY, category VARCHAR(8), v VECTOR(2))")
        db.sql("INSERT INTO t VALUES (9,'keep','[1,0]'), (2,'keep','[-1,0]'), (7,'keep','[0,1]'), (1,'skip','[0,0]')")
        db.sql("CREATE VECTOR INDEX ix ON t(v)")
        assert [row[0] for row in compare_with_reference(db, keep, [0, 0], 10, where)] == [2, 7, 9]
        assert compare_with_reference(db, {}, [0, 0], 10, "WHERE category = 'absent'") == []
        for value, code in [
            ("[1]", "dimension_mismatch"), ("[NaN,0]", "invalid_value"),
            ("[Infinity,0]", "invalid_value"), ("[-Infinity,0]", "invalid_value"),
            ("[3.5e38,0]", "invalid_value"),
        ]:
            db.rejected(f"INSERT INTO t VALUES (99,'keep','{value}')", code)
            db.rejected(f"SELECT id FROM t ORDER BY L2_DISTANCE(v,'{value}') LIMIT 1", code)
        db.sql("INSERT INTO t VALUES (10,'edge','[1.0000000596046448,0]'), (11,'edge','[3.4028234663852886e38,0]')")
        edges = {10: [1.0 + 2**-23, 0], 11: [f32(3.4028234663852886e38), 0]}
        assert compare_with_reference(db, edges, [1, 0], 2, "WHERE category = 'edge'")[0][1] == 2**-23
        db.sql("UPDATE t SET v = '[2,0]' WHERE id = 9")
        db.sql("DELETE FROM t WHERE id = 2")
        db.sql("INSERT INTO t VALUES (2,'keep','[4,0]')")
        keep.update({9: [2, 0], 2: [4, 0]})
        db.mode = "ann"
        compare_with_reference(db, keep, [0, 0], 10, where)
        db.sql("BEGIN")
        db.sql("UPDATE t SET v = '[0,0]' WHERE id = 2")
        assert db.sql("SELECT v FROM t WHERE id = 2")["rows"] == [[[0.0, 0.0]]]
        compare_with_reference(db, keep | {2: [0, 0]}, [0, 0], 10, where)
        db.sql("ROLLBACK")
        compare_with_reference(db, keep, [0, 0], 10, where)
        db.sql("BEGIN")
        db.sql("UPDATE t SET v = '[0,0]' WHERE id = 2")
        db.sql("CREATE VECTOR INDEX after_commit ON t(v)")
        db.sql("COMMIT")
        keep[2] = [0, 0]
        db.kill()
    with VectorSession(image, out, data, "vector-after-crash") as db:
        db.ready()
        db.rejected("CREATE VECTOR INDEX IX ON t(v)", "index_exists")
        db.rejected("CREATE VECTOR INDEX after_commit ON t(v)", "index_exists")
        for db.mode in ["exact", "ann"]:
            compare_with_reference(db, keep, [0, 0], 10, where)
            compare_with_reference(db, edges, [1, 0], 2, "WHERE category = 'edge'")
        db.shutdown()
    print("PASS: ties, filtering, empty, float32 edges, invalid vectors, update, reinsert,"
          " transaction visibility and rollback, committed vector/index after SIGKILL, independent math.dist reference", flush=True)


def oversized_scan(image, out):
    data = out / "oversized-data"
    data.mkdir()
    payload = "x" * (1024 * 1024)
    vectors = {key: [float(key)] * 128 for key in range(80)}
    with VectorSession(image, out, data, "oversized-write") as db:
        db.ready()
        db.sql("CREATE TABLE t (id BIGINT PRIMARY KEY, payload VARCHAR(1048576), v VECTOR(128))")
        db.sql("CREATE VECTOR INDEX ix ON t(v)")
        db.sql("BEGIN")
        for start in range(0, len(vectors), 4):
            rows = ",".join(f"({key},'{payload}',{vector(vectors[key])})" for key in range(start, start + 4))
            assert db.sql("INSERT INTO t VALUES " + rows)["affected_rows"] == 4
        db.sql("COMMIT")
        db.shutdown()
    with VectorSession(image, out, data, "oversized-read") as db:
        db.ready()
        start = time.perf_counter()
        result = compare_with_reference(db, vectors, [37.5] * 128, 10)
        elapsed = time.perf_counter() - start
        metrics = db.counters()
        db.shutdown()
    disk = sum(path.stat().st_size for path in data.iterdir())
    assert disk > 64 * 1024 * 1024
    return {"rows": 80, "dimensions": 128, "dataset_bytes": disk, "memory_limit_bytes": 64 * 1024 * 1024,
            "one_topk_ms": elapsed * 1000, "ids": [row[0] for row in result], "process": metrics}


def elapsed_sql(db, sql):
    start = time.perf_counter()
    result = db.sql(sql)
    return (time.perf_counter() - start) * 1000, result


def performance(image, out):
    data = out / "benchmark-data"
    data.mkdir()
    rng = random.Random(20261110)
    vectors = {key: [rng.randrange(-16, 17) for _ in range(128)] for key in range(10000)}
    queries = [[rng.randrange(-16, 17) for _ in range(128)] for _ in range(1000)]
    with VectorSession(image, out, data, "benchmark") as db:
        db.ready()
        db.sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v VECTOR(128))")
        db.sql("CREATE VECTOR INDEX ix ON t(v)")
        for first in range(0, 10000, 500):
            rows = ",".join(f"({i},{vector(vectors[i])})" for i in range(first, first + 500))
            assert db.sql("INSERT INTO t VALUES " + rows)["affected_rows"] == 500
        before = db.counters()
        writes = []
        for key in range(10):
            vectors[key][0] += 1
            ms, result = elapsed_sql(db, f"UPDATE t SET v={vector(vectors[key])} WHERE id={key}")
            assert result["affected_rows"] == 1
            writes.append(ms)
        after_write = db.counters()
        times = []
        deadline = time.monotonic() + 180
        for query in queries:
            if time.monotonic() >= deadline:
                break
            ms, result = elapsed_sql(db, f"SELECT id FROM t ORDER BY L2_DISTANCE(v,{vector(query)}) LIMIT 10")
            assert len(result["rows"]) == 10
            if len(times) < 5:
                expected = sorted(vectors, key=lambda key: (math.dist(vectors[key], query), key))[:10]
                assert result["rows"] == [[key] for key in expected]
            times.append(ms)
        after_read = db.counters()
        db.shutdown()
    counters = ("rchar", "wchar", "read_bytes", "write_bytes")
    def delta(before, after):
        return {key: after[key] - before[key] for key in counters}
    def timing(values):
        ordered = sorted(values)
        return {"count": len(values), "total_ms": sum(values), "median_ms": statistics.median(values),
                "p95_ms": ordered[math.ceil(len(values) * .95) - 1]}
    return {"scope": "synthetic inputs, benchmark-shaped; not official benchmark",
            "rows": 10000, "dimensions": 128, "k": 10, "target_queries": 1000, "query_time_cap_s": 180,
            "dataset_bytes": (data / "table-t.jsonl").stat().st_size,
            "one_row_updates": timing(writes), "queries": timing(times),
            "update_io": delta(before, after_write), "query_io": delta(after_write, after_read),
            "baseline_memory": before, "after_updates_memory": after_write, "after_queries_memory": after_read,
            "oracle_verified_queries": min(5, len(times)), "update_samples_ms": writes, "query_samples_ms": times}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("output", type=Path)
    parser.add_argument("--performance", action="store_true")
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir()
    if args.performance:
        result = performance(args.image, out)
        filename = "benchmark.json"
    else:
        consistency(args.image, out)
        result = oversized_scan(args.image, out)
        filename = "oversized.json"
    (out / filename).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if not key.endswith("_samples_ms")}), flush=True)


if __name__ == "__main__":
    main()
