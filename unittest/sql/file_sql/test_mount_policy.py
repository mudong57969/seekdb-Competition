from pathlib import Path
import os
import socket
import subprocess
import tempfile

root = Path(__file__).resolve().parents[3]
sdb = root / ".github/script/seekdb/sdb.py"
binary = Path(os.environ.get("SEEKDB_TEST_BINARY", root / "build_release/src/observer/seekdb"))
client = root / "deps/3rd/u01/obclient/bin/obclient"

with tempfile.TemporaryDirectory(prefix="seekdb-mount-", dir="/tmp") as local, \
     tempfile.TemporaryDirectory(prefix="seekdb-mount-", dir="/dev/shm") as mounted:
    base = Path(local) / "db"
    allowed = Path(local) / "allowed"
    child = allowed / "child"
    child.mkdir(parents=True)
    local_file = child / "fixture.csv"
    mounted_file = Path(mounted) / "fixture.csv"
    assert Path(mounted).stat().st_dev != Path("/").stat().st_dev
    for fixture in (local_file, mounted_file):
        fixture.write_text("id\n1\n")
    (allowed / "link.csv").symlink_to(mounted_file)
    os.mkfifo(allowed / "pipe.csv")
    sibling = allowed.with_name("allowed-sibling")
    sibling.mkdir()
    (sibling / "fixture.csv").write_text("id\n1\n")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = str(sock.getsockname()[1])

    def server(*args):
        subprocess.run(["python3", sdb, *args], check=True)

    def sql(statement, code=0, output=None):
        proc = subprocess.run([client, "-S", base / "run/sql.sock", "-uroot", "-NB", "-e", statement],
                              text=True, capture_output=True, timeout=15)
        if code:
            assert proc.returncode and f"ERROR {code} " in proc.stderr, (statement, proc.stderr)
        else:
            assert proc.returncode == 0, (statement, proc.stderr)
            if output is not None:
                assert proc.stdout.strip() == output, (statement, proc.stdout)
        print(f"PASS expected={code} {statement}", flush=True)

    def query(path, code=0, explain=False):
        command = "EXPLAIN " if explain else ""
        sql(f"SET SESSION enable_file_sql=ON; {command}SELECT COUNT(*) FROM read_csv('{path}')",
            code, None if explain else "1")

    server("start", "--binary", binary, "--base-dir", base, "--port", port)
    try:
        server("wait-ready", "--client", client, "--base-dir", base, "--host", "127.0.0.1",
               "--port", port, "--user", "root", "--timeout", "45")
        sql(f"SELECT COUNT(*) FROM read_csv('{mounted_file}')", 1235)
        sql("SET GLOBAL secure_file_priv='/'")
        query(local_file)
        query(mounted_file, explain=True)
        query(mounted_file)
        sql(f"SET SESSION enable_file_sql=ON; SELECT COUNT(*) FROM glob('{mounted}/*')", output="1")
        query(f"{mounted}/*.csv")
        sql(f"SET GLOBAL secure_file_priv='{mounted}'")
        query(mounted_file)
        sql(f"SET GLOBAL secure_file_priv='{allowed}'")
        query(local_file)
        for path in (mounted_file, sibling / "fixture.csv",
                     f"{allowed}/../allowed-sibling/fixture.csv", allowed / "link.csv", allowed / "pipe.csv"):
            query(path, 1227)
        sql(f"SET SESSION enable_file_sql=ON; COPY (SELECT 1 AS id) TO '{mounted}/denied.csv' WITH (FORMAT CSV, HEADER TRUE)", 1227)
        assert not (Path(mounted) / "denied.csv").exists()
        if os.environ.get("SEEKDB_TEST_BIND_MOUNT") == "1":
            alias = allowed / "alias"
            alias.mkdir()
            subprocess.run(["mount", "--bind", sibling, alias], check=True)
            try:
                query(alias / "fixture.csv", 1227)
                sql("SET GLOBAL secure_file_priv='/'")
                query(alias / "fixture.csv")
                sql(f"SET SESSION enable_file_sql=ON; COPY (SELECT 1 AS id) "
                    f"TO '{alias}/denied.csv' WITH (FORMAT CSV, HEADER TRUE)", 1227)
                assert not (sibling / "denied.csv").exists()
            finally:
                subprocess.run(["umount", alias], check=True)
        else:
            print("NOT RUN bind mount (use a private mount namespace and SEEKDB_TEST_BIND_MOUNT=1)")
        sql("SELECT 1", output="1")
    finally:
        server("stop", "--base-dir", base)
