# MiniVectorDB local candidate

The Stage 5 baseline is commit `09fdc1e88462cb49fd27b201dfcfacdc69975825`.
The Stage 6 WAL experiment is separate from the candidate.

The engine uses Rust 1.99.1 (edition 2021). Run from the repository root:

```sh
cargo check --locked --manifest-path minivectordb/Cargo.toml
cargo test --locked --manifest-path minivectordb/Cargo.toml
cargo build --locked --manifest-path minivectordb/Cargo.toml
docker build --no-cache \
  --build-arg RUST_IMAGE=public.ecr.aws/docker/library/rust:1.99.1-bookworm \
  --build-arg RUNTIME_IMAGE=public.ecr.aws/docker/library/debian:bookworm-slim \
  -t minivectordb:stage8-candidate .
mkdir -p protocol-results
out=$(mktemp -d protocol-results/local-XXXXXX)
python3 protocol_check/run.py --image minivectordb:stage8-candidate --handshake-only --output "$out/handshake"
python3 protocol_check/run.py --image minivectordb:stage8-candidate --output "$out/full-public"
python3 minivectordb/tests/crash_recovery.py minivectordb:stage8-candidate "$out/recovery"
python3 minivectordb/tests/vector_validation.py minivectordb:stage8-candidate "$out/vector"
python3 minivectordb/tests/stage5_benchmark.py minivectordb:stage8-candidate "$out/benchmark"
```

The image speaks UTF-8 JSONL as specified in `STDIO_PROTOCOL.md` and persists in
`/data`. ANN requests use exact scanning. The official problem statement documents
`agent/conversation.json` as the default evidence path. The root
`conversation.json` placeholder is preserved.

SQL supports table creation, inserts, projections, filtering and ordering,
updates, deletes, vector distance queries, and explicit transactions.
Tables may have zero or one VECTOR column. SQL failures return a JSON
response with the request ID, a false ok field, and an error object.
The public driver limits responses to 16 MiB (16,777,216 bytes). It rejects
a reproduced 17,825,927-byte response. The published rules do not specify
how the engine should respond when a result exceeds this limit. The engine
keeps returning the full JSON response; queries that exceed the driver
limit remain a compatibility risk.

The following example creates a fresh named volume, commits one row, rolls
back a second row, and reads the retained row after a restart. The container's
default entrypoint is /usr/local/bin/minivectordb.

```sh
volume=$(docker volume create)
docker run --rm -i --network=none --read-only -v "$volume:/data" minivectordb:stage8-candidate <<'JSONL'
{"id":1,"op":"execute","sql":"CREATE TABLE docs (id BIGINT PRIMARY KEY, v VECTOR(2))"}
{"id":2,"op":"execute","sql":"BEGIN"}
{"id":3,"op":"execute","sql":"INSERT INTO docs VALUES (1, '[3,4]')"}
{"id":4,"op":"execute","sql":"COMMIT"}
{"id":5,"op":"execute","sql":"BEGIN"}
{"id":6,"op":"execute","sql":"INSERT INTO docs VALUES (2, '[0,0]')"}
{"id":7,"op":"execute","sql":"ROLLBACK"}
{"id":8,"op":"execute","sql":"SELECT id FROM docs ORDER BY id"}
{"id":9,"op":"execute","sql":"SELECT id, L2_DISTANCE(v,'[0,0]') AS distance FROM docs ORDER BY distance LIMIT 1","search_mode":"exact"}
{"id":10,"op":"shutdown"}
JSONL
docker run --rm -i --network=none --read-only -v "$volume:/data" minivectordb:stage8-candidate <<'JSONL'
{"id":11,"op":"execute","sql":"SELECT id FROM docs ORDER BY id"}
{"id":12,"op":"shutdown"}
JSONL
```

Both ordinary queries should return only ID 1, and the vector query should
return ID 1 with distance 5. The example volume remains available for
inspection.
