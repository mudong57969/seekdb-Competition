"""Deterministic mutation smoke test for the standalone reader under sanitizers."""
from pathlib import Path
import random
import subprocess
import sys
import tempfile


def mutate(source, rng):
    data = bytearray(source)
    for _ in range(rng.randrange(1, 6)):
        offset = rng.randrange(len(data))
        data[offset] ^= 1 << rng.randrange(8)
    return data


def check(reader, corpus):
    rng = random.Random(1701)
    paths = [p for p in sorted(corpus.glob("arrow-logical-*.parquet"))]
    assert len(paths) >= 4, paths
    samples = [p.read_bytes() for p in paths]
    counts = [0, 0]
    with tempfile.TemporaryDirectory() as scratch:
        candidate = Path(scratch) / "mutated.parquet"
        for index in range(384):
            sample = samples[index % len(samples)]
            if index % 5 == 0:
                altered = sample[:rng.randrange(1, len(sample))]
            else:
                altered = mutate(sample, rng)
            candidate.write_bytes(altered)
            result = subprocess.run([reader, candidate], stdout=subprocess.DEVNULL,
                                    stderr=subprocess.PIPE, timeout=5)
            assert result.returncode in (0, 1), (index, result.returncode, result.stderr)
            assert all(marker not in result.stderr for marker in
                       (b"AddressSanitizer", b"UndefinedBehaviorSanitizer", b"runtime error:")), result.stderr
            counts[result.returncode] += 1
    print(f"PASS 384 deterministic mutations: {counts[0]} accepted, {counts[1]} rejected, no sanitizer diagnostics")


if __name__ == "__main__":
    check(sys.argv[1], Path(sys.argv[2]))
