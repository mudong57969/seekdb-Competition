"""Generate native Parquet bytes and exercise the standalone subset reader."""
from pathlib import Path
import gzip
import resource
import signal
import struct
import subprocess
import sys
import tempfile
import zlib


def varint(value):
    encoded = bytearray()
    while value >= 128:
        encoded.append((value & 127) | 128)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def integer(value):
    return varint(value * 2 if value >= 0 else -value * 2 - 1)


def binary(value):
    return varint(len(value)) + value


def fields(*entries):
    data = bytearray()
    previous = 0
    for field, kind, value in entries:
        gap = field - previous
        assert 0 < gap < 16
        data.append((gap << 4) | kind)
        data.extend(value)
        previous = field
    return bytes(data) + b"\0"


def sequence(kind, values):
    return (bytes([(len(values) << 4) | kind]) if len(values) < 15
            else bytes([0xF0 | kind]) + varint(len(values))) + b"".join(values)


def page(values, *, bitpack=False, nullable=True, encoding=0, page_type=0):
    payload = b""
    if nullable:
        if bitpack:
            assert len(values) <= 8
            levels = bytes([3, sum(1 << index for index, item in enumerate(values) if item is not None)])
        else:
            levels = b"".join(bytes([2, item is not None]) for item in values)
        payload = struct.pack("<I", len(levels)) + levels
    payload += b"".join(struct.pack("<i", item) for item in values if item is not None)
    return (fields((1, 5, integer(page_type)), (2, 5, integer(len(payload))), (3, 5, integer(len(payload))),
                   (5, 12, fields((1, 5, integer(len(values))), (2, 5, integer(encoding)),
                                  (3, 5, integer(3)), (4, 5, integer(3)))))
            + payload)


def write_parquet(path, groups, *, nullable=True, codec=0, decimal=None):
    metadata = []
    total_rows = 0
    with path.open("wb") as output:
        output.write(b"PAR1")
        for count, pages in groups:
            offset = output.tell()
            for data in pages:
                output.write(data)
            size = output.tell() - offset
            chunk_metadata = fields(
                (1, 5, integer(1)), (2, 9, sequence(5, [integer(0), integer(3)])),
                (3, 9, sequence(8, [binary(b"id")])), (4, 5, integer(codec)),
                (5, 6, integer(count)), (6, 6, integer(size)),
                (7, 6, integer(size)), (9, 6, integer(offset)),
            )
            chunk = fields((2, 6, integer(0)), (3, 12, chunk_metadata))
            metadata.append(fields((1, 9, sequence(12, [chunk])),
                                   (2, 6, integer(size)), (3, 6, integer(count))))
            total_rows += count
        root = fields((4, 8, binary(b"schema")), (5, 5, integer(1)))
        leaf_fields = [(1, 5, integer(1)), (3, 5, integer(int(nullable))), (4, 8, binary(b"id"))]
        if decimal is not None:
            precision, scale = decimal
            leaf_fields.extend([(6, 5, integer(5)), (7, 5, integer(scale)), (8, 5, integer(precision))])
        leaf = fields(*leaf_fields)
        footer = fields((1, 5, integer(1)), (2, 9, sequence(12, [root, leaf])),
                        (3, 6, integer(total_rows)), (4, 9, sequence(12, metadata)))
        output.write(footer + struct.pack("<I", len(footer)) + b"PAR1")


def write_small(path):
    write_parquet(path, [(4, [page([1, None]), page([-9, 42], bitpack=True)]),
                         (2, [page([None, 7], bitpack=True)])])


def check(binary_path, fixture, scratch):
    def run(path):
        return subprocess.run([binary_path, path], capture_output=True, text=True, timeout=10)

    def reject(path, message):
        result = run(path)
        assert result.returncode == 1 and message in result.stderr, result

    write_small(fixture)
    result = run(fixture)
    assert result.returncode == 0 and result.stdout == "id\n1\nNULL\n-9\n42\nNULL\n7\n", result
    print("PASS nullable INT32, RLE/bit-packed levels, multiple pages and row groups")

    required = scratch / "required.parquet"
    write_parquet(required, [(2, [page([-(2**31), 2**31 - 1], nullable=False)])], nullable=False)
    result = run(required)
    assert result.returncode == 0 and result.stdout == "id\n-2147483648\n2147483647\n", result
    print("PASS required INT32")

    rejected = scratch / "rejected.parquet"
    write_parquet(rejected, [(1, [page([1])])], codec=4)
    reject(rejected, "unsupported codec: 4")
    write_parquet(rejected, [(1, [page([1], encoding=8)])])
    reject(rejected, "dictionary data page without dictionary")
    write_parquet(rejected, [(1, [page([1], page_type=3)])])
    reject(rejected, "unsupported page type or invalid header")
    write_parquet(rejected, [(1, [page([1])[:-1]])])
    reject(rejected, "truncated data page")
    bad = page([1])
    # The payload is a four-byte length, a two-byte RLE run, and one INT32.
    bad = bad[:-5] + bytes([2]) + bad[-4:]
    write_parquet(rejected, [(1, [bad])])
    reject(rejected, "RLE value exceeds bit width")
    rejected.write_bytes(fixture.read_bytes()[:-5])
    reject(rejected, "Parquet magic")
    rejected.write_bytes(b"PAR1" + b"\0" * 8 + struct.pack("<I", 2**24) + b"PAR1")
    reject(rejected, "oversized footer")
    symlink = scratch / "symlink.parquet"
    symlink.symlink_to(fixture.resolve())
    reject(symlink, "cannot open input")
    print("PASS unsupported codecs/encodings, malformed footer, and final-component symlink")

    def wrapped_page(encoded, size=4):
        return (fields((1, 5, integer(0)), (2, 5, integer(size)), (3, 5, integer(len(encoded))),
                       (5, 12, fields((1, 5, integer(1)), (2, 5, integer(0)),
                                      (3, 5, integer(3)), (4, 5, integer(3))))) + encoded)

    def codec_file(codec, payload, size=4):
        write_parquet(rejected, [(1, [wrapped_page(payload, size)])], codec=codec, nullable=False)

    raw = struct.pack("<i", 42)
    good = b"\x04\x0c" + raw
    codec_file(1, good)
    assert run(rejected).stdout == "id\n42\n"
    for payload, detail in (
        (good[:-1], "truncated Snappy literal"),
        (b"\x05" + good[1:], "Snappy output length mismatch"),
        (b"\x04\x01\x00", "invalid Snappy copy offset"),
        (b"\x04\x01\x01", "invalid Snappy copy offset"),
        (b"\x04\x10", "Snappy literal exceeds output"),
        (b"\x04\x00a\x0e\x01\x00", "Snappy copy exceeds output"),
        (varint(2**32), "Snappy output length mismatch"),
    ):
        codec_file(1, payload)
        reject(rejected, detail)
    codec_file(1, b"\0", 2**20 + 1)
    reject(rejected, "invalid or oversized page")
    print("PASS Snappy rejects truncated blocks, length mismatch, offsets, and output overflow")

    members = gzip.compress(raw[:2]) + gzip.compress(b"") + gzip.compress(raw[2:])
    codec_file(2, members)
    result = run(rejected)
    assert result.returncode == 0 and result.stdout == "id\n42\n", result
    for payload, detail in (
        (zlib.compress(raw), "invalid GZIP member header"),
        (gzip.compress(raw) + zlib.compress(b""), "invalid GZIP member header"),
        (members[:-1], "truncated GZIP member"),
        (gzip.compress(raw + b"x"), "GZIP output exceeds page"),
        (gzip.compress(raw[:3]), "GZIP output length mismatch"),
    ):
        codec_file(2, payload)
        reject(rejected, detail)
    bad_crc = bytearray(gzip.compress(raw))
    bad_crc[-8] ^= 1
    codec_file(2, bytes(bad_crc))
    reject(rejected, "invalid GZIP data or trailer")
    print("PASS concatenated GZIP members, CRC, truncation, overflow and rejection of zlib streams")

    write_parquet(rejected, [(1, [page([1000], nullable=False)])], nullable=False, decimal=(3, 2))
    reject(rejected, "DECIMAL value exceeds precision")
    write_parquet(rejected, [(1, [page([1], nullable=False)])], nullable=False, decimal=(2, 3))
    reject(rejected, "unsupported DECIMAL precision or scale")
    print("PASS DECIMAL rejects values beyond precision and invalid scale")

    large = scratch / "large.parquet"
    values_per_page = 128 * 1024
    data = page([7] * values_per_page, nullable=False)
    write_parquet(large, [(values_per_page * 3, [data] * 3)] * 12, nullable=False)

    def set_memory_limit():
        resource.setrlimit(resource.RLIMIT_AS, (16 * 1024 * 1024,) * 2)

    subprocess.run(["/usr/bin/time", "-f", "reader_peak_rss_KiB=%M", binary_path, large],
                   stdout=subprocess.DEVNULL, check=True,
                   preexec_fn=set_memory_limit, timeout=15)
    print(f"PASS input {large.stat().st_size} bytes under 16777216-byte address-space limit")

    with subprocess.Popen([binary_path, large], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True) as process:
        assert process.stdout.readline() == "id\n"
        process.send_signal(signal.SIGTERM)
        _, errors = process.communicate(timeout=10)
        assert process.returncode == 1 and "scan cancelled" in errors, errors

    with subprocess.Popen([binary_path, large], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True) as process:
        assert process.stdout.readline() == "id\n"
        with large.open("ab") as output:
            output.write(b"x")
        _, errors = process.communicate(timeout=10)
        assert process.returncode == 1 and "file identity changed" in errors, errors
    write_parquet(large, [(values_per_page * 3, [data] * 3)] * 12, nullable=False)
    with subprocess.Popen([binary_path, large], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True) as process:
        assert process.stdout.readline() == "id\n"
        replacement = scratch / "replacement.parquet"
        write_small(replacement)
        replacement.replace(large)
        _, errors = process.communicate(timeout=10)
        assert process.returncode == 1 and "file identity changed" in errors, errors
    print("PASS cancellation, in-place mutation, and replacement during scan")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as temporary:
        check(sys.argv[1], Path(sys.argv[2]), Path(temporary))
