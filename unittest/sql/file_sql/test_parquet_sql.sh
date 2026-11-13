#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
suite="$root/tools/deploy/mysql_test/test_suite/file_sql"
run=$(mktemp -d /tmp/lab2-pq.XXXXXX)
s="$root/.github/script/seekdb/sdb.py"
client="$root/deps/3rd/u01/obclient/bin/obclient"
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
trap 'python3 "$s" stop --base-dir "$run/db"' EXIT
echo "instance=$run port=$port"
python3 "$s" start --binary "$root/build_release/src/observer/seekdb" --base-dir "$run/db" --port "$port"
python3 "$s" wait-ready --client "$client" --base-dir "$run/db" --host 127.0.0.1 --port "$port" --user root --timeout 45
f="$run/db/data_for_mysqltest_file_sql"
mkdir "$f"
cp "$suite/data/"{basic.csv,basic.jsonl,invalid.txt} "$f/"
base64 -d "$suite/data/simple.parquet.b64" > "$f/simple.parquet"
"$client" -S "$run/db/run/sql.sock" -uroot -e "SET GLOBAL secure_file_priv='$f'"
python3 - "$suite" "$run" "$f" <<'PY'
from pathlib import Path
import sys
suite,run,files=map(Path,sys.argv[1:])
t=(suite/'t/file_sql_basic.test').read_text()
r=(suite/'r/mysql/file_sql_basic.result').read_text()
(run/'supported.test').write_text('--disable_query_log\nlet $FILE_SQL_DATA_DIR = '+str(files)+';\n'+t[t.index('--echo # enable_file_sql'):t.index('--echo # COPY query')])
(run/'supported.result').write_text(r[r.index('# enable_file_sql'):r.index('# COPY query')])
PY
"${client%/*}/mysqltest" --host=127.0.0.1 --port="$port" --user=root --database=test --test-file="$run/supported.test" --result-file="$run/supported.result"
cp "$1"/native-*.parquet "$f/"
sql() { "$client" -h127.0.0.1 -P"$port" -uroot -NB -e "$1"; }
enabled() { sql "SET SESSION enable_file_sql=ON; $1"; }
expect() { local got; got=$(enabled "$1"); printf '%s\n%s\n' "$1" "$got"; test "$got" = "$2"; }
reject() { local err; if err=$(sql "$1" 2>&1); then echo "unexpected success: $1"; return 1; fi; echo "$err"; grep -q "ERROR $2 " <<< "$err"; }
q="read_parquet('$f/native-snappy.parquet')"
for codec in snappy gzip zstd; do
 expect "SELECT * FROM read_parquet('$f/native-$codec.parquet') ORDER BY id" $'1\t90.5\t雪\t1\t2026-11-13\t1970-01-01 00:00:01.234567\t1970-01-01T00:00:00.000000001\t1234.50\n2\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\tNULL\n3\t85.25\ttail\t0\t1969-12-31\t1969-12-31 23:59:59.999999\t1969-12-31T23:59:59.999999999\t-0.25'
done
expect "SELECT id,score FROM $q WHERE score>80 ORDER BY id LIMIT 1" $'1\t90.5'
expect "SELECT active,COUNT(*),SUM(amount) FROM $q GROUP BY active ORDER BY active IS NULL, active" $'0\t1\t-0.25\n1\t1\t1234.50\nNULL\t1\tNULL'
join="SELECT /*+ LEADING(t p) USE_NL(p) NO_USE_NL_MATERIALIZATION(p) */ t.id,p.id FROM (SELECT 1 AS id UNION ALL SELECT 2) t JOIN $q p ON p.id>=t.id ORDER BY t.id,p.id"
expect "$join" $'1\t1\n1\t2\n1\t3\n2\t2\n2\t3'
plan=$(enabled "EXPLAIN $join"); echo "$plan"; grep -q 'FILE SCAN' <<< "$plan"; grep -q 'NESTED-LOOP JOIN' <<< "$plan"; ! grep -q MATERIAL <<< "$plan"
reject "SELECT * FROM $q" 1235
reject "SET SESSION enable_file_sql=ON; SELECT COUNT(*) FROM $q; SET SESSION enable_file_sql=OFF; SELECT * FROM $q" 1235
ln -s "$f/native-snappy.parquet" "$f/link.parquet"
reject "SET SESSION enable_file_sql=ON; SELECT * FROM read_parquet('$f/link.parquet')" 1227
reject "SET SESSION enable_file_sql=ON; SELECT * FROM read_parquet('$f/../native-snappy.parquet')" 1227
test "$(sql 'SELECT 1')" = 1
sql 'CREATE TABLE test.lab2_parquet_count(id INT); INSERT INTO test.lab2_parquet_count VALUES (1),(2);'
test "$(sql 'SELECT COUNT(*) FROM test.lab2_parquet_count')" = 2
echo 'PASS: official supported fragments and native Parquet SQL'
"$client" -S "$run/db/run/sql.sock" -uroot -e "ALTER SYSTEM SET debug_sync_timeout='10s'"
python3 - "$client" "$run" "$f" <<'PY'
import subprocess
import sys
from pathlib import Path

client, run, files = sys.argv[1:]
files = Path(files)
base = Path(run) / "db"
args = [client, '-S', str(Path(run) / 'db/run/sql.sock'), '-uroot', '-NB', '-e']

def sql(query):
    return subprocess.check_output(args + [query], text=True, timeout=15).strip()

path = files / 'native-snappy.parquet'
for name, source in (('parquet', f"read_parquet('{path}')"),):
    statement = (
        "SET SESSION enable_file_sql=ON; SET SESSION debug_sync="
        f"'HANG_BEFORE_RESOLVER_FINISH SIGNAL {name}_ready WAIT_FOR {name}_release'; "
        f"SELECT * FROM {source}"
    )
    with subprocess.Popen(args + [statement], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as proc:
        try:
            sql(f"SET SESSION debug_sync='NOW WAIT_FOR {name}_ready'")
            assert proc.poll() is None
            replacement = path.with_suffix('.new')
            replacement.write_bytes(path.read_bytes())
            replacement.replace(path)
            sql(f"SET SESSION debug_sync='NOW SIGNAL {name}_release'")
            out, err = proc.communicate(timeout=15)
            assert proc.returncode and not out and 'changed since schema inference' in err, (out, err)
            print(name, err.strip())
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
import os
import time

pid = int((base / 'run/seekdb.pid').read_text())
def has_scan():
 for fd in Path(f'/proc/{pid}/fd').iterdir():
  try:
   if os.readlink(fd) == str(path):
    return True
  except FileNotFoundError:
   pass
 return False

path = files / 'native-snappy.parquet'
marker = 'lab2_active_parquet_cancel'
statement = ("SET SESSION enable_file_sql=ON; "
             f"SELECT id, IF(id=2,SLEEP(30),0) AS {marker} FROM read_parquet('{path}')")
with subprocess.Popen(args+[statement],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) as proc:
 try:
  deadline = time.monotonic() + 8
  while True:
   sid = sql("SELECT ID FROM information_schema.PROCESSLIST WHERE ID<>CONNECTION_ID() "
             f"AND INFO LIKE '%{marker}%'")
   if sid.isdigit() and has_scan():
    break
   if proc.poll() is not None or time.monotonic() >= deadline:
    raise RuntimeError('Parquet execution did not enter the sleep barrier')
   time.sleep(0.05)
  sql(f'KILL QUERY {sid}')
  _, err = proc.communicate(timeout=12)
  assert proc.returncode and 'interrupted' in err.lower(), err
  deadline = time.monotonic() + 5
  while has_scan():
   assert time.monotonic() < deadline, 'cancelled scan left its descriptor open'
   time.sleep(0.05)
  print('cancel', err.strip(), 'descriptor closed')
 finally:
  if proc.poll() is None:
   proc.kill()
   proc.wait(timeout=10)

PY
