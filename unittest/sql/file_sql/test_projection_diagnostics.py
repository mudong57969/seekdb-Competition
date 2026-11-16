"""Native SQL regression. PyArrow only produces fixtures and finds page offsets."""
import os
import socket
import subprocess
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path(__file__).resolve().parents[3]
client = root / "deps/3rd/u01/obclient/bin/obclient"
sdb = root / ".github/script/seekdb/sdb.py"
binary = Path(os.environ.get("SEEKDB_TEST_BINARY", root / "build_release/src/observer/seekdb"))

with tempfile.TemporaryDirectory(prefix="file-project-", dir="/tmp") as tmp:
    base = Path(tmp) / "db"
    files = Path(tmp) / "files"
    files.mkdir()
    path = files / "projection.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3, 4], "b": ["one", None, "two", "two"]}),
                   path, row_group_size=2, compression="snappy", use_dictionary=True)
    metadata = pq.ParquetFile(path).metadata
    assert metadata.num_row_groups == 2
    page = metadata.row_group(1).column(1).data_page_offset
    damaged = files / "damaged.parquet"
    contents = bytearray(path.read_bytes())
    # TCompact STOP cannot begin a valid page header. Leave the footer intact.
    contents[page] = 0
    damaged.write_bytes(contents)
    csv = files / "invalid.csv"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = str(sock.getsockname()[1])

    def server(*args):
        subprocess.run(["python3", sdb, *args], check=True)

    def sql(query):
        return subprocess.run([client, "-S", base / "run/sql.sock", "-uroot", "-NBr", "-e", query],
                              text=True, capture_output=True, timeout=30)

    failures = []

    def check(label, query, output=None, error=None, code=1210):
        result = sql("SET SESSION enable_file_sql=ON; " + query)
        passed = (result.returncode == 0 and result.stdout.rstrip("\n") == output if error is None
                  else result.returncode != 0 and f"ERROR {code} " in result.stderr and error in result.stderr)
        print(f"{'PASS' if passed else 'FAIL'} {label}: {result.stdout!r} {result.stderr!r}", flush=True)
        if not passed:
            failures.append(label)

    server("start", "--binary", binary, "--base-dir", base, "--port", port)
    try:
        server("wait-ready", "--client", client, "--base-dir", base, "--host", "127.0.0.1",
               "--port", port, "--user", "root", "--timeout", "60")
        sql(f"SET GLOBAL secure_file_priv='{files}'").check_returncode()
        check("single column across two groups", f"SELECT a FROM read_parquet('{path}') ORDER BY a", "1\n2\n3\n4")
        check("reverse projection and nullable dictionary", f"SELECT b,a FROM read_parquet('{path}') ORDER BY a",
              "one\t1\nNULL\t2\ntwo\t3\ntwo\t4")
        check("corrupt unused page", f"SELECT a FROM read_parquet('{damaged}') ORDER BY a", "1\n2\n3\n4")
        check("count does not decode corrupt page", f"SELECT COUNT(*) FROM read_parquet('{damaged}')", "4")
        check("corrupt projected page", f"SELECT b FROM read_parquet('{damaged}')", error="unsupported page type or invalid header")
        check("corrupt predicate column", f"SELECT COUNT(*) FROM read_parquet('{damaged}') WHERE b='two'",
              error="unsupported page type or invalid header")
        check("order retains non-output column", f"SELECT a FROM read_parquet('{path}') ORDER BY b,a",
              "2\n1\n3\n4")
        check("correlated column remains available",
              f"SELECT (SELECT ABS(p.a)) FROM read_parquet('{path}') p ORDER BY p.a", "1\n2\n3\n4")
        check("official glob", f"SELECT COUNT(*) FROM glob('{files}/*')", "2")
        check("glob outside scope", f"SELECT * FROM glob('{files}/../*')", error="Access denied", code=1227)
        for label, payload, record, line, field, offset in [
            ("bad quote after quoted newline", b'a,b\r\n1,"x\ny"\r\n2,x"y\n', 3, 4, 2, 17),
            ("invalid UTF-8 after escaped quote", b'a,b\n1,"x""\xff"\n', 2, 2, 2, 10),
            ("unterminated multiline field", b'a,b\n1,"x\ny', 2, 3, 2, 10),
            ("missing field", b'a,b\n1\n', 2, 2, 2, 5),
            ("extra field", b'a\n1,2\n', 2, 2, 2, 4),
            ("BOM and multiline header", b'\xef\xbb\xbf"a\nz",b\n1,x\x00\n', 2, 3, 2, 14),
            ("oversized field", b'a\n' + b'x' * (4 * 1024 * 1024 + 1), 2, 2, 1, 4194306),
        ]:
            csv.write_bytes(payload)
            check(label, f"SELECT * FROM read_csv('{csv}')",
                  error=f"record {record}, line {line}, field {field}, byte {offset}")
    finally:
        server("stop", "--base-dir", base)
    raise SystemExit(bool(failures))
