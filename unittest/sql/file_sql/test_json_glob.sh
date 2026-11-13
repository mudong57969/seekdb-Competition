#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
suite="$root/tools/deploy/mysql_test/test_suite/file_sql"
run=$(mktemp -d /tmp/lab2-json.XXXXXX)
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
suite, run, files=map(Path,sys.argv[1:])
t=(suite/'t/file_sql_basic.test').read_text()
r=(suite/'r/mysql/file_sql_basic.result').read_text()
def section(s,start,end):
 i=s.index(start); return s[i:s.index(end,i)]
def parts(s,p):
 return section(s,p+'# enable_file_sql',p+'# Parquet scan')+section(s,p+'# DuckDB glob',p+'# COPY query')
(run/'supported.test').write_text('--disable_query_log\nlet $FILE_SQL_DATA_DIR = '+str(files)+';\n'+parts(t,'--echo '))
(run/'supported.result').write_text(parts(r,''))
PY
"${client%/*}/mysqltest" --host=127.0.0.1 --port="$port" --user=root --database=test --test-file="$run/supported.test" --result-file="$run/supported.result"
sql() { "$client" -h127.0.0.1 -P"$port" -uroot -NB -e "$1"; }
enabled() { sql "SET SESSION enable_file_sql=ON; $1"; }
expect() { local got; got=$(enabled "$1"); printf '%s\n%s\n' "$1" "$got"; test "$got" = "$2"; }
reject() { local err; if err=$(sql "$1" 2>&1); then echo "unexpected success: $1"; return 1; fi; echo "$err"; grep -q "ERROR $2 " <<< "$err"; }
mkdir "$f"/{json,ndjson,csv,empty}
printf '[{"id":2,"name":"张"},{"id":1,"name":null}]' > "$f/json/b.json"
printf '[{"id":3,"late":true}]' > "$f/json/a.json"
printf '{"id":2,"name":"Bob"}\n{"id":1,"name":null}\n' > "$f/ndjson/b.ndjson"
printf '{"id":3,"late":true}\n' > "$f/ndjson/a.ndjson"
printf 'id,name\n1,Alice\n' > "$f/csv/a.csv"
printf 'id,name\n2,Bob\n' > "$f/csv/b.csv"
expect "SELECT * FROM read_json('$f/json/b.json') ORDER BY id" $'1\tNULL\n2\t张'
expect "SELECT j.name FROM read_json('$f/json/b.json') AS j WHERE j.id>1 ORDER BY id LIMIT 1" '张'
expect "SELECT count(*) FROM read_ndjson('$f/basic.jsonl')" 3
expect "SELECT count(*) FROM read_csv('$f/csv/*.csv')" 2
expect "SELECT id,late FROM read_json('$f/json/*.json') ORDER BY id" $'1\tNULL\n2\tNULL\n3\t1'
expect "SELECT id,late FROM read_ndjson('$f/ndjson/*.ndjson') ORDER BY id" $'1\tNULL\n2\tNULL\n3\t1'
expect "SELECT file FROM glob('$f/json/*') ORDER BY file" "$(printf '%s\n%s' "$f/json/a.json" "$f/json/b.json")"
expect "SELECT count(*) FROM glob('$f/empty/*')" 0
expect "SELECT n.id,count(*) FROM read_ndjson('$f/ndjson/*.ndjson') n JOIN read_json('$f/json/*.json') j ON n.id=j.id GROUP BY n.id ORDER BY n.id" $'1\t1\n2\t1\n3\t1'
q="SELECT /*+ LEADING(t j) USE_NL(j) NO_USE_NL_MATERIALIZATION(j) */ t.id,j.id FROM (SELECT 1 AS id UNION ALL SELECT 2) t JOIN read_json('$f/json/*.json') j ON j.id>=t.id ORDER BY t.id,j.id"
expect "$q" $'1\t1\n1\t2\n1\t3\n2\t2\n2\t3'
plan=$(enabled "EXPLAIN $q"); echo "$plan"; grep -q 'FILE SCAN' <<< "$plan"; grep -q 'NESTED-LOOP JOIN' <<< "$plan"
reject "SELECT * FROM read_json('$f/json/b.json')" 1235
reject "SET SESSION enable_file_sql=ON; SELECT COUNT(*) FROM glob('$f/empty/*'); SET SESSION enable_file_sql=OFF; SELECT * FROM glob('$f/empty/*')" 1235
reject "SET SESSION enable_file_sql=ON; SELECT * FROM read_json('$f/basic.jsonl',format='unknown')" 1235
reject "SET SESSION enable_file_sql=ON; SELECT * FROM read_ndjson('$f/../data_for_mysqltest_file_sql/basic.jsonl')" 1227
ln -s "$f/json/a.json" "$f/empty/alias.json"
reject "SET SESSION enable_file_sql=ON; SELECT * FROM glob('$f/empty/*')" 1227
reject "SET SESSION enable_file_sql=ON; SELECT * FROM read_json('$f/empty/alias.json')" 1227
test "$(sql 'SELECT 1')" = 1
sql 'CREATE TABLE test.lab2_json_count (id INT); INSERT INTO test.lab2_json_count VALUES (1),(2);'
test "$(sql 'SELECT COUNT(*) FROM test.lab2_json_count')" = 2
echo 'PASS: official CSV, JSONL and glob fragments; native JSON, NDJSON and glob; ordinary SQL'
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

for name, source, path, content in (
    ('json', f"read_json('{files}/json/a.json')", files / 'json/a.json', '[{"id":4}]'),
    ('ndjson', f"read_ndjson('{files}/ndjson/a.ndjson')", files / 'ndjson/a.ndjson', '{"id":4}\n'),
    ('glob', f"glob('{files}/csv/*')", files / 'csv/new.csv', 'id\n4\n'),
):
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
            replacement.write_text(content)
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

path = files / 'json' / 'cancel.json'
path.write_text('[{"id":1,"pad":"' + 'x' * 131072 + '"},{"id":2},{"id":3}]')
marker = 'lab2_active_json_cancel'
statement = ("SET SESSION enable_file_sql=ON; "
             f"SELECT id, IF(id=2,SLEEP(30),0) AS {marker} FROM read_json('{path}')")
with subprocess.Popen(args+[statement],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) as proc:
 try:
  deadline = time.monotonic() + 8
  while True:
   sid = sql("SELECT ID FROM information_schema.PROCESSLIST WHERE ID<>CONNECTION_ID() "
             f"AND INFO LIKE '%{marker}%'")
   if sid.isdigit() and has_scan():
    break
   if proc.poll() is not None or time.monotonic() >= deadline:
    raise RuntimeError('JSON execution did not enter the sleep barrier')
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
