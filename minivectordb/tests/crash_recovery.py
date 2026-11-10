"""Independent container tests for MiniVectorDB's process-crash model."""

import argparse
import json
from pathlib import Path
import selectors
import subprocess
import time
import uuid


class Session:
    def __init__(self, image, output, data, label, fault=None):
        self.name = "minivectordb-fault-" + uuid.uuid4().hex
        self.log = output / (label + ".stderr.log")
        self.sequence = 0
        args = [
            "docker", "run", "--name", self.name, "-i", "--network=none",
            "--read-only", "--cap-drop=ALL", "--memory=64m", "--memory-swap=64m",
            "--mount", f"type=bind,src={data},dst=/data",
        ]
        if fault:
            args += [
                "--mount", f"type=bind,src={output / 'fault-rename.so'},dst=/fault-rename.so,readonly",
                "--env", "LD_PRELOAD=/fault-rename.so", "--env", f"MINIVECTORDB_FAULT={fault}",
            ]
        with self.log.open("wb") as log:
            self.process = subprocess.Popen(args + [image], stdin=subprocess.PIPE,
                                            stdout=subprocess.PIPE, stderr=log)
        self.stopped = False
        print("+", *args, image, flush=True)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        if not self.stopped:
            subprocess.run(["docker", "kill", self.name], capture_output=True, timeout=20)
            self.process.wait(timeout=20)
        subprocess.run(["docker", "rm", self.name], capture_output=True, check=True, timeout=20)

    def receive(self):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            assert selector.select(60), f"response timeout, see {self.log}"
        line = self.process.stdout.readline()
        assert line, f"early exit, see {self.log}"
        return json.loads(line)

    def ready(self):
        assert self.receive() == {"type": "ready"}

    def send(self, op, sql=None):
        self.sequence += 1
        request = {"id": self.sequence, "op": op}
        if sql is not None:
            request.update(sql=sql, search_mode="exact")
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()

    def sql(self, sql):
        self.send("execute", sql)
        response = self.receive()
        assert response["id"] == self.sequence and response["ok"] is True, response
        return response

    def kill(self):
        print("+ docker kill", self.name, flush=True)
        subprocess.run(["docker", "kill", self.name], check=True, timeout=20)
        status = self.process.wait(timeout=20)
        assert status == 137, status
        print("container exit:", status, flush=True)
        self.stopped = True

    def shutdown(self):
        self.send("shutdown")
        assert self.receive() == {"id": self.sequence, "ok": True}
        status = self.process.wait(timeout=20)
        assert status == 0, status
        print("container exit:", status, flush=True)
        self.stopped = True

    def wait_for_fault(self, phase):
        deadline = time.monotonic() + 30
        while f"fault: {phase}" not in self.log.read_text():
            assert self.process.poll() is None, self.log.read_text()
            assert time.monotonic() < deadline, self.log.read_text()
            time.sleep(0.02)

    def memory(self):
        pid = subprocess.check_output(["docker", "inspect", "-f", "{{.State.Pid}}", self.name], text=True).strip()
        fields = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines() if ":" in line)
        result = {"VmHWM_kib": int(fields["VmHWM"].split()[0]), "VmRSS_kib": int(fields["VmRSS"].split()[0])}
        cgroup = Path(f"/proc/{pid}/cgroup").read_text().strip().split("::")[-1]
        for name in ["memory.max", "memory.peak"]:
            path = Path("/sys/fs/cgroup") / cgroup.lstrip("/") / name
            if path.is_file():
                result[name] = int(path.read_text())
        io = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/io").read_text().splitlines())
        result["read_bytes"] = int(io["read_bytes"])
        result["write_bytes"] = int(io["write_bytes"])
        return result


def check(session, expected, table="t"):
    assert session.sql(f"SELECT id FROM {table} ORDER BY id LIMIT 20")["rows"] == [[item] for item in expected]


def crash_tests(image, output):
    data = output / "crash-data"
    data.mkdir()

    def session(label, fault=None):
        return Session(image, output, data, label, fault)

    with session("autocommit-kill") as db:
        db.ready()
        db.sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        db.sql("CREATE TABLE u (id BIGINT PRIMARY KEY)")
        db.sql("INSERT INTO t VALUES (1)")
        db.kill()
    with session("uncommitted-kill") as db:
        db.ready()
        check(db, [1])
        db.sql("BEGIN")
        db.sql("DELETE FROM t")
        db.sql("INSERT INTO t VALUES (2)")
        check(db, [2])
        db.kill()
    with session("confirmed-transaction-kill") as db:
        db.ready()
        check(db, [1])
        db.sql("BEGIN")
        db.sql("UPDATE t SET id = 3 WHERE id = 1")
        db.sql("INSERT INTO t VALUES (4)")
        db.sql("INSERT INTO u VALUES (8)")
        db.sql("COMMIT")
        db.kill()
    with session("commit-before-response", "publish") as db:
        db.ready()
        check(db, [3, 4])
        check(db, [8], "u")
        db.sql("BEGIN")
        db.sql("INSERT INTO t VALUES (5)")
        db.sql("INSERT INTO u VALUES (9)")
        db.send("execute", "COMMIT")
        db.wait_for_fault("publish")
        assert len(list((data / "committed").iterdir())) == 2
        with selectors.DefaultSelector() as selector:
            selector.register(db.process.stdout, selectors.EVENT_READ)
            assert not selector.select(0), "commit response already visible"
        db.kill()
    with session("recovery-kill", "install") as db:
        db.wait_for_fault("install")
        assert len(list((data / "committed").iterdir())) == 1
        with selectors.DefaultSelector() as selector:
            selector.register(db.process.stdout, selectors.EVENT_READ)
            assert not selector.select(0), "ready emitted during recovery"
        db.kill()
    (data / "statement.tmp").write_bytes(b'{"truncated')
    (data / "pending").mkdir()
    (data / "pending" / "table-t.jsonl").write_bytes(b'{"unfinished')
    for attempt in range(2):
        with session(f"repeat-recovery-{attempt}") as db:
            db.ready()
            check(db, [3, 4, 5])
            check(db, [8, 9], "u")
            assert not any((data / name).exists() for name in ["statement.tmp", "pending", "committed"])
            db.shutdown()
    print("PASS: confirmed autocommit, abandoned transaction, confirmed multi-table commit,"
          " unacknowledged commit, recovery interrupted, torn unpublished files, and repeated recovery", flush=True)


def memory_test(image, output):
    data = output / "memory-data"
    data.mkdir()
    payload = "x" * (1024 * 1024)
    count = 80
    with Session(image, output, data, "memory-write") as db:
        db.ready()
        db.sql("CREATE TABLE t (id BIGINT PRIMARY KEY, payload VARCHAR(1048576))")
        db.sql("BEGIN")
        for start in range(0, count, 4):
            values = ",".join(f"({key},'{payload}')" for key in range(start, start + 4))
            assert db.sql("INSERT INTO t VALUES " + values)["affected_rows"] == 4
        db.sql("COMMIT")
        assert db.sql("UPDATE t SET id = 100 WHERE id = 79")["affected_rows"] == 1
        write = db.memory()
        db.shutdown()
    with Session(image, output, data, "memory-restart") as db:
        db.ready()
        assert db.sql("SELECT id FROM t ORDER BY id DESC LIMIT 3")["rows"] == [[100], [78], [77]]
        assert db.sql("SELECT id FROM t WHERE id >= 0 ORDER BY id LIMIT 80")["rows"] == (
            [[i] for i in range(79)] + [[100]]
        )
        read = db.memory()
        db.shutdown()
    files = list(data.iterdir())
    metrics = {
        "rows": count, "docker_memory_limit_bytes": 64 * 1024 * 1024,
        "dataset_bytes": sum(path.stat().st_size for path in files),
        "allocated_disk_bytes": sum(path.stat().st_blocks * 512 for path in files),
        "files": len(files), "write_process": write, "read_process": read,
    }
    assert metrics["dataset_bytes"] > metrics["docker_memory_limit_bytes"]
    (output / "resources.json").write_text(json.dumps(metrics, indent=2) + "\n")
    print("PASS: bounded-memory write, restart and scan", json.dumps(metrics), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir()
    source = Path(__file__).with_name("fault_rename.c")
    command = ["cc", "-shared", "-fPIC", "-Wall", "-Wextra", "-Werror", str(source), "-o", str(output / "fault-rename.so"), "-ldl"]
    print("+", *command, flush=True)
    subprocess.run(command, check=True)
    crash_tests(args.image, output)
    memory_test(args.image, output)


if __name__ == "__main__":
    main()
