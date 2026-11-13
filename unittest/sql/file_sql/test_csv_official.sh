#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
suite="$root/tools/deploy/mysql_test/test_suite/file_sql"
run=$(mktemp -d /tmp/lab2-offcsv.XXXXXX)
s="$root/.github/script/seekdb/sdb.py"
client="$root/deps/3rd/u01/obclient/bin/obclient"
mysqltest="${client%/*}/mysqltest"
port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
trap 'python3 "$s" stop --base-dir "$run/db"' EXIT
echo "instance=$run port=$port"
python3 "$s" start --binary "$root/build_release/src/observer/seekdb" --base-dir "$run/db" --port "$port"
python3 "$s" wait-ready --client "$client" --base-dir "$run/db" --host 127.0.0.1 --port "$port" --user root --timeout 45
mkdir "$run/db/data_for_mysqltest_file_sql"
cp "$suite/data/basic.csv" "$run/db/data_for_mysqltest_file_sql/"
sha256sum "$suite/data/basic.csv" "$run/db/data_for_mysqltest_file_sql/basic.csv"
"$client" -S "$run/db/run/sql.sock" -uroot -e "SET GLOBAL secure_file_priv='$run/db/data_for_mysqltest_file_sql'"
python3 - "$suite" "$run" <<'PY'
from pathlib import Path
import sys
suite, run = map(Path,sys.argv[1:])
t=(suite/'t/file_sql_basic.test').read_text()
r=(suite/'r/mysql/file_sql_basic.result').read_text()
def section(s,start,end):
 i=s.index(start); return s[i:s.index(end,i)]
def csv_parts(s,prefix):
 return section(s,prefix+'# enable_file_sql',prefix+'# JSONL')+section(s,prefix+'# inferred CSV schema',prefix+'# Parquet scan')
(run/'csv.test').write_text('--disable_query_log\nlet $FILE_SQL_DATA_DIR = '+str(run/'db/data_for_mysqltest_file_sql')+';\n'+csv_parts(t,'--echo '))
(run/'csv.result').write_text(csv_parts(r,''))
PY
"$mysqltest" --host=127.0.0.1 --port="$port" --user=root --database=test --test-file="$run/csv.test" --result-file="$run/csv.result"
"$client" -S "$run/db/run/sql.sock" -uroot -e "ALTER SYSTEM SET debug_sync_timeout='10s'"
python3 - "$client" "$port" "$run" <<'PYTEST'
import subprocess,sys
from pathlib import Path
client,port,run=sys.argv[1:]
base=Path(run)/'db'
path=base/'data_for_mysqltest_file_sql'/'binding.csv'
args=[client,'-S',str(base/'run/sql.sock'),'-uroot','-NB','-e']
def sql(query):
 r=subprocess.run(args+[query],capture_output=True,text=True,timeout=20)
 if r.returncode: raise RuntimeError(r.stderr)
 return r.stdout.strip()
for action,expected in [('replace','file changed since schema inference'),('revoke','ERROR 1227')]:
 path.write_text('id\n1\n')
 ready,release='lab2_'+action+'_ready','lab2_'+action+'_release'
 command=("SET SESSION enable_file_sql=ON; SET SESSION debug_sync='HANG_BEFORE_RESOLVER_FINISH SIGNAL "
          +ready+" WAIT_FOR "+release+"'; SELECT id FROM read_csv('"+str(path)+"')")
 with subprocess.Popen(args+[command],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) as proc:
  try:
   sql("SET SESSION debug_sync='NOW WAIT_FOR "+ready+"'")
   assert proc.poll() is None
   if action=='replace':
    other=path.with_suffix('.new')
    other.write_text('id\n2\n')
    other.replace(path)
   else:
    sql("SET GLOBAL secure_file_priv='/'")
   sql("SET SESSION debug_sync='NOW SIGNAL "+release+"'")
   out,err=proc.communicate(timeout=20)
   assert proc.returncode and not out and expected in err,(proc.returncode,out,err)
   print(action,err.strip())
  finally:
   if proc.poll() is None:
    proc.kill()
    proc.wait(timeout=10)
   if action=='revoke':
    sql("SET GLOBAL secure_file_priv='"+str(path.parent)+"'")
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

path.write_text('id,pad\n1,' + 'x' * 131072 + '\n2,second\n3,third\n')
marker = 'lab2_active_csv_cancel'
statement = ("SET SESSION enable_file_sql=ON; "
             f"SELECT id, IF(id=2,SLEEP(30),0) AS {marker} FROM read_csv('{path}')")
with subprocess.Popen(args+[statement],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) as proc:
 try:
  deadline = time.monotonic() + 8
  while True:
   sid = sql("SELECT ID FROM information_schema.PROCESSLIST WHERE ID<>CONNECTION_ID() "
             f"AND INFO LIKE '%{marker}%'")
   if sid.isdigit() and has_scan():
    break
   if proc.poll() is not None or time.monotonic() >= deadline:
    raise RuntimeError('CSV execution did not enter the sleep barrier')
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
assert sql("SELECT @@session.enable_file_sql")=='0'
assert sql("SELECT 1")=='1'
PYTEST
