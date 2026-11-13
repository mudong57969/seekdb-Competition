#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
suite="$root/tools/deploy/mysql_test/test_suite/file_sql"
run=$(mktemp -d /tmp/lab2-copy.XXXXXX)
s="$root/.github/script/seekdb/sdb.py"
c="$root/deps/3rd/u01/obclient/bin/obclient"
export PATH="${c%/*}:$PATH"
export OBSERVER_DIR="$run/db"
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
trap 'python3 "$s" stop --base-dir "$OBSERVER_DIR"' EXIT
echo "instance=$run port=$port"
python3 "$s" start --binary "$root/build_release/src/observer/seekdb" --base-dir "$OBSERVER_DIR" --port "$port"
python3 "$s" wait-ready --client "$c" --base-dir "$OBSERVER_DIR" --host 127.0.0.1 --port "$port" --user root --timeout 45
dir="$run/mysql_test/test_suite/file_sql"
mkdir -p "$dir/include"
ln -s "$suite/data" "$dir/data"
cp "$suite/t/file_sql_basic.test" "$run/official.test"
cp "$suite/r/mysql/file_sql_basic.result" "$run/official.result"
cmp "$suite/t/file_sql_basic.test" "$run/official.test"
cmp "$suite/r/mysql/file_sql_basic.result" "$run/official.result"
sha256sum "$run/official.test" "$run/official.result" "$suite/data/"*
cat > "$dir/include/copy_file_sql_data.inc" <<'INC'
--exec mkdir -p "$OBSERVER_DIR/data_for_mysqltest_file_sql"
--exec cp mysql_test/test_suite/file_sql/data/basic.csv mysql_test/test_suite/file_sql/data/basic.jsonl mysql_test/test_suite/file_sql/data/invalid.txt "$OBSERVER_DIR/data_for_mysqltest_file_sql/"
--exec base64 -d mysql_test/test_suite/file_sql/data/simple.parquet.b64 > "$OBSERVER_DIR/data_for_mysqltest_file_sql/simple.parquet"
--exec obclient --protocol=socket -S "$OBSERVER_DIR/run/sql.sock" -uroot -e "SET GLOBAL secure_file_priv='$OBSERVER_DIR/data_for_mysqltest_file_sql'"
INC
printf '# fixtures preserved in isolated instance directory\n' > "$dir/include/remove_file_sql_data.inc"
(cd "$run" && mysqltest --host=127.0.0.1 --port="$port" --user=root --database=test --test-file="$run/official.test" --result-file="$run/official.result")
echo 'PASS: full original file_sql_basic.test'
f="$OBSERVER_DIR/data_for_mysqltest_file_sql"
sql() { "$c" -h127.0.0.1 -P"$port" -uroot -NB -e "$1"; }
enabled() { sql "SET SESSION enable_file_sql=ON; $1"; }
reject() { local err; if err=$(sql "$1" 2>&1); then echo "unexpected success: $1"; return 1; fi; echo "$err"; grep -q "ERROR $2 " <<< "$err"; }
reject "COPY (SELECT 1 AS id) TO '$f/off.csv' WITH (FORMAT CSV, HEADER TRUE)" 1235
test ! -e "$f/off.csv"
enabled "COPY (SELECT NULL AS marker UNION ALL SELECT 'NULL') TO '$f/union.csv' (format csv, header, nullstr 'NULL')"
cat "$f/union.csv"
test "$(head -1 "$f/union.csv")" = marker
test "$(enabled "SELECT COUNT(*), COUNT(marker) FROM read_csv('$f/union.csv')")" = $'2\t1'
enabled "COPY (SELECT 42 AS answer) TO '$f/nohead.csv' WITH (FORMAT CSV, HEADER FALSE)"
test "$(cat "$f/nohead.csv")" = 42
enabled "COPY (SELECT 42 AS answer WHERE 1=0) TO '$f/empty.csv' WITH (FORMAT CSV, HEADER TRUE)"
test "$(cat "$f/empty.csv")" = answer
test "$(enabled "SELECT COUNT(*) FROM read_csv('$f/empty.csv')")" = 0
reject "SET SESSION enable_file_sql=ON; COPY (SELECT 1 AS id) TO '$f/../outside.csv' WITH (FORMAT CSV, HEADER TRUE)" 1227
mkdir "$run/outside"
ln -s "$run/outside" "$f/link"
reject "SET SESSION enable_file_sql=ON; COPY (SELECT 1 AS id) TO '$f/link/escape.csv' WITH (FORMAT CSV, HEADER TRUE)" 1227
if enabled "COPY (SELECT 1 AS id) TO '$f/union.csv' WITH (FORMAT CSV, HEADER TRUE)"; then exit 1; fi
test "$(head -1 "$f/union.csv")" = marker
set +e
enabled "COPY (SELECT 7 AS id) TO '$f/race.csv' WITH (FORMAT CSV, HEADER TRUE)" > "$run/race1.log" 2>&1 & a=$!
enabled "COPY (SELECT 8 AS id) TO '$f/race.csv' WITH (FORMAT CSV, HEADER TRUE)" > "$run/race2.log" 2>&1 & b=$!
wait "$a"; ra=$?
wait "$b"; rb=$?
set -e
test "$(( (ra == 0) + (rb == 0) ))" = 1
echo "concurrent same name exit codes: $ra $rb"
cat "$run"/race{1,2}.log "$f/race.csv"
"$c" -S "$OBSERVER_DIR/run/sql.sock" -uroot -e "SELECT 19 INTO OUTFILE '$f/ordinary.txt'"
test "$(cat "$f/ordinary.txt")" = 19
test "$(sql 'SELECT 1')" = 1
sql 'CREATE TABLE test.lab2_copy_count(id INT); INSERT INTO test.lab2_copy_count VALUES (1),(2);'
test "$(sql 'SELECT COUNT(*) FROM test.lab2_copy_count')" = 2
echo 'PASS: COPY roundtrip, union headers, no overwrite, concurrency and ordinary SQL'
python3 - "$c" "$port" "$f" "$OBSERVER_DIR" <<'PY'
import os
import subprocess
import sys
import time
from pathlib import Path

client, port, files, base = sys.argv[1:]
files = Path(files)
pid = int((Path(base) / 'run/seekdb.pid').read_text())
args = [client, '-S', str(Path(base) / 'run/sql.sock'), '-uroot', '-NB', '-e']

def sql(statement):
    return subprocess.check_output(args + [statement], text=True, timeout=15).strip()

def anonymous_open():
    for fd in Path(f'/proc/{pid}/fd').iterdir():
        try:
            target = os.readlink(fd)
        except FileNotFoundError:
            continue
        if target.startswith(str(files) + '/#') and target.endswith(' (deleted)'):
            return True
    return False

sql("ALTER SYSTEM SET debug_sync_timeout='10s'")
for action in ('cancel', 'revoke'):
    marker = f'lab2_copy_{action}_probe'
    output = files / f'{action}.csv'
    cmd = ("SET SESSION enable_file_sql=ON; "
           f"COPY (SELECT IF(id=2,SLEEP(8),0) AS {marker},id "
           f"FROM read_csv('{files}/basic.csv')) TO '{output}' WITH (FORMAT CSV, HEADER TRUE)")
    with subprocess.Popen(args + [cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
        try:
            deadline = time.monotonic() + 5
            while True:
                sid = sql("SELECT ID FROM information_schema.PROCESSLIST WHERE ID<>CONNECTION_ID() "
                          f"AND INFO LIKE '%{marker}%'")
                if sid.isdigit() and anonymous_open():
                    break
                assert proc.poll() is None and time.monotonic() < deadline, 'anonymous export was not observed'
                time.sleep(0.05)
            assert not output.exists(), 'partial output visible'
            if action == 'cancel':
                sql(f'KILL QUERY {sid}')
                error = 'ERROR 1317'
            else:
                sql("SET GLOBAL secure_file_priv='/'")
                error = 'ERROR 1227'
            _, err = proc.communicate(timeout=15)
            assert proc.returncode and error in err and not output.exists(), err
            deadline = time.monotonic() + 5
            while anonymous_open():
                assert time.monotonic() < deadline, 'anonymous output descriptor remained open'
                time.sleep(0.05)
            print(action, err.strip(), 'no file published; descriptor closed')
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
            if action == 'revoke':
                sql(f"SET GLOBAL secure_file_priv='{files}'")
PY
