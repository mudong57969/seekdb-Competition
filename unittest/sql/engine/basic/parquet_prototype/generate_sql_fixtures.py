"""Generate the native SQL Parquet fixtures with PyArrow 22.0.0."""
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def generate(output):
    output.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "score": pa.array([90.5, None, 85.25], type=pa.float64()),
        "pad": pa.array(["雪", None, "tail"]),
        "active": pa.array([True, None, False]),
        "day": pa.array([date(2026, 11, 13), None, date(1969, 12, 31)], type=pa.date32()),
        "ts": pa.array([1234567, None, -1], type=pa.int64()).cast(pa.timestamp("us", tz="UTC")),
        "ns": pa.array([1, None, -1], type=pa.int64()).cast(pa.timestamp("ns")),
        "amount": pa.array([Decimal("1234.50"), None, Decimal("-0.25")], type=pa.decimal128(12, 2)),
    })
    for codec in ("snappy", "gzip", "zstd"):
        pq.write_table(table, output / f"native-{codec}.parquet", compression=codec,
                       row_group_size=2, data_page_version="2.0")


if __name__ == "__main__":
    generate(Path(sys.argv[1]))
