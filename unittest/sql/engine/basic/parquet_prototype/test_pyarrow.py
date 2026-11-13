"""Generate independent Parquet fixtures with PyArrow and compare row values."""
import datetime
from decimal import Decimal
from pathlib import Path
import resource
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def tsv_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, str):
        if value == "NULL":
            return r"\NULL"
        return (value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")
                .replace("\r", "\\r").replace("\0", "\\0"))
    if isinstance(value, float):
        # Fixture fractions are binary-exact and avoid formatting differences.
        return format(value, ".17g")
    return str(value)


def table():
    size = 1024
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("large", pa.int64()),
        pa.field("ratio", pa.float32()),
        pa.field("score", pa.float64()),
        pa.field("flag", pa.bool_()),
        pa.field("name", pa.string()),
    ])
    columns = [
        list(range(size)),
        [None if i % 3 == 0 else 2**50 + i for i in range(size)],
        [None if i % 5 == 0 else (i - 512) / 8 for i in range(size)],
        [None if i % 7 == 0 else (i - 100) / 16 for i in range(size)],
        [None if i % 11 == 0 else i % 3 == 0 for i in range(size)],
        [None if i % 4 == 0 else "NULL" if i % 6 == 0
         else "雪,\n\t\\" + str(i % 17) + "a" * (i % 35) for i in range(size)],
    ]
    return pa.Table.from_arrays(columns, schema=schema)


def check(reader, output_dir):
    def run(path, *args):
        return subprocess.run([reader, path, *args], text=True, capture_output=True, timeout=15)

    def reject(path, text):
        result = run(path)
        assert result.returncode == 1 and text in result.stderr, result

    source = table()
    for page_version, use_dictionary, codec in (
        ("1.0", False, "none"),
        ("1.0", True, "none"),
        ("2.0", False, "none"),
        ("2.0", True, "none"),
        ("1.0", True, "zstd"),
        ("2.0", True, "zstd"),
        ("1.0", False, "snappy"),
        ("2.0", True, "snappy"),
        ("1.0", True, "gzip"),
        ("2.0", False, "gzip"),
        ("2.0", True, "gzip"),
    ):
        fixture = output_dir / f"arrow-v{page_version}-dict{int(use_dictionary)}-{codec}.parquet"
        pq.write_table(source, fixture, row_group_size=513, data_page_size=256, write_batch_size=32,
                       data_page_version=page_version, use_dictionary=use_dictionary, compression=codec)
        parquet = pq.ParquetFile(fixture)
        assert parquet.metadata.num_row_groups == 2
        reference = parquet.read()
        names = reference.schema.names
        lines = ["\t".join(names)] + [
            "\t".join(tsv_value(row[name]) for name in names) for row in reference.to_pylist()
        ]
        result = run(fixture, "--page-boundaries")
        assert result.returncode == 0, (fixture, result.stderr)
        page_boundaries = {name: [] for name in names}
        for line in result.stderr.splitlines():
            marker, name, row = line.split("\t")
            assert marker == "PAGE"
            page_boundaries[name].append(int(row))
        if not use_dictionary:
            assert any(len(offsets) > 2 for offsets in page_boundaries.values()), page_boundaries
            assert len({tuple(offsets) for offsets in page_boundaries.values()}) > 1, page_boundaries
        actual = result.stdout.splitlines()
        assert len(actual) == len(lines), (fixture, len(actual), len(lines))
        for row, (ours, expected) in enumerate(zip(actual, lines)):
            assert ours == expected, (fixture, row, ours, expected)
        print(f"PASS {fixture.name}: {reference.num_rows} rows, {len(names)} columns, 2 row groups")

    all_null = output_dir / "arrow-all-null.parquet"
    pq.write_table(pa.table({"text": pa.array([None] * 9, type=pa.string()),
                             "flag": pa.array([None] * 9, type=pa.bool_())}),
                   all_null, data_page_version="2.0", compression="zstd")
    result = run(all_null)
    assert result.returncode == 0 and result.stdout == "text\tflag\n" + "NULL\tNULL\n" * 9, result
    print("PASS independently encoded all-null dictionary/string and boolean pages")

    large = output_dir / "arrow-large.parquet"
    with pq.ParquetWriter(large, source.schema, compression="none", use_dictionary=False) as writer:
        for _ in range(512):
            writer.write_table(source)
    limit = 16 * 1024 * 1024
    assert large.stat().st_size > limit, large.stat().st_size

    def restrict_memory():
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

    result = subprocess.run(
        ["/usr/bin/time", "-f", "multicolumn_reader_peak_rss_KiB=%M", reader, large],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=30,
        preexec_fn=restrict_memory,
    )
    assert result.returncode == 0, result.stderr
    print(result.stderr.strip())
    print(f"PASS multicolumn scan of {large.stat().st_size} bytes under {limit}-byte address-space limit")

    for version, dictionary, codec in (
        ("1.0", False, "none"),
        ("1.0", True, "gzip"),
        ("2.0", True, "snappy"),
        ("2.0", False, "zstd"),
    ):
        n = 9
        raw = [None, -1, 0, 1, 123456789, -123456789, 123, -123, 1000]
        cols = {
            "date": pa.array([None, datetime.date(1969, 12, 31), datetime.date(1970, 1, 1),
                              datetime.date(2000, 2, 29), datetime.date(1, 1, 1),
                              datetime.date(9999, 12, 31), None, datetime.date(2026, 11, 13),
                              datetime.date(2026, 11, 14)], type=pa.date32()),
            "ms": pa.array(raw, type=pa.int64()).cast(pa.timestamp("ms", tz="UTC")),
            "us": pa.array(raw, type=pa.int64()).cast(pa.timestamp("us")),
            "ns": pa.array(raw, type=pa.int64()).cast(pa.timestamp("ns", tz="UTC")),
            "dec32": pa.array([None, Decimal("-0.01"), Decimal("0.00"), Decimal("1.23")] * 2
                              + [Decimal("-99999.99")], type=pa.decimal128(7, 2)),
            "dec64": pa.array([Decimal("-999999999999.99"), None, Decimal("0.01")] * 3,
                              type=pa.decimal128(14, 2)),
            "dec128": pa.array([Decimal("-999999999999999999999999999999999999.99"), None,
                                Decimal("999999999999999999999999999999999999.99")] * 3,
                               type=pa.decimal128(38, 2)),
        }
        fixture = output_dir / f"arrow-logical-v{version}-dict{int(dictionary)}-{codec}.parquet"
        pq.write_table(pa.table(cols), fixture, version="2.6", row_group_size=4,
                       store_decimal_as_integer=True, use_dictionary=dictionary,
                       data_page_version=version, compression=codec)
        reference = pq.read_table(fixture)
        values = {
            name: (reference[name].cast(pa.int64()).to_pylist()
                   if name in ("ms", "us", "ns") else reference[name].to_pylist())
            for name in cols
        }
        def format_value(name, value):
            if value is None:
                return "NULL"
            if name not in ("ms", "us", "ns"):
                return format(value, "f") if isinstance(value, Decimal) else str(value)
            digits = {"ms": 3, "us": 6, "ns": 9}[name]
            seconds, fraction = divmod(value, 10**digits)
            civil = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
            return civil.isoformat() + f".{fraction:0{digits}d}" + ("" if name == "us" else "Z")
        expected = "\t".join(cols) + "\n" + "\n".join(
            "\t".join(format_value(name, values[name][i]) for name in cols) for i in range(n)
        ) + "\n"
        result = run(fixture)
        assert result.returncode == 0 and result.stdout == expected, (fixture, result, expected)
        print(f"PASS {fixture.name}: DATE, timestamp precision and sign, and DECIMAL INT32/64/fixed")

    for label, values, typ in (
        ("date-overflow", pa.array([2**31 - 1], type=pa.int32()), pa.date32()),
        ("timestamp-overflow", pa.array([2**63 - 1], type=pa.int64()), pa.timestamp("ms")),
    ):
        fixture = output_dir / f"arrow-{label}.parquet"
        pq.write_table(pa.table({"value": values.cast(typ)}), fixture, compression="none")
        reject(fixture, "outside supported calendar years")
        print(f"PASS rejects {label}")

    fixture = output_dir / "arrow-unsupported-time.parquet"
    pq.write_table(pa.table({"value": pa.array([datetime.time(1, 2, 3)], type=pa.time64("us"))}),
                   fixture, compression="none")
    reject(fixture, "unsupported")
    print("PASS rejects unsupported TIME annotation")


if __name__ == "__main__":
    target = Path(sys.argv[2])
    target.mkdir(parents=True, exist_ok=True)
    check(sys.argv[1], target)
