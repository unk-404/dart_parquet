#!/usr/bin/env python3
"""Generate small pyarrow-written fixtures for the Dart Parquet interop tests.

These complement the apache/parquet-testing subset with a second writer origin
(pyarrow / parquet-cpp-arrow), multiple row groups and DataPageV2 pages.

Run from the repository root with the interop venv:

    .agents/venv/Scripts/python.exe tool/interop/make_pyarrow_fixture.py

Outputs (committed, small):
    test/fixtures/interop/generated/pyarrow_multi_rowgroup.parquet
    test/fixtures/interop/generated/pyarrow_multi_rowgroup_v2.parquet

The expected values are generated separately by gen_expected.py.
"""

from __future__ import annotations

import datetime as dt
import decimal
import math
import os

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(ROOT, "test", "fixtures", "interop", "generated")

N_ROWS = 11
ROW_GROUP_SIZE = 3


def build_table() -> pa.Table:
    # Deterministic, dependency-free input data.
    ids = [None, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    names = [None, "alpha", "beta", "gamma", "delta", "epsilon", "zeta",
             "eta", "theta", "iota", "kappa"]
    scores = [0.0, -0.0, 1.5, float("nan"), -2.25, 3.75, 1e-9, -1e9,
              2.0, 3.0, 4.0]
    flags = [None, True, False, True, False, True, False, True, False,
             True, False]
    epoch = dt.datetime(1970, 1, 1)
    ts = [None] + [epoch + dt.timedelta(seconds=i * 3600 + 17, microseconds=123)
                   for i in range(N_ROWS - 1)]
    dates = [None] + [dt.date(1970, 1, 1) + dt.timedelta(days=i * 100)
                      for i in range(N_ROWS - 1)]
    decs = [None] + [decimal.Decimal(f"{i * 100 - 250}.25") for i in range(N_ROWS - 1)]
    fixed = [None] + [bytes([i, i + 1, i + 2, i + 3]) for i in range(N_ROWS - 1)]
    binary = [None] + [bytes([0, 255, i]) for i in range(N_ROWS - 1)]
    half = [None] + [float(i) / 2 for i in range(N_ROWS - 1)]
    lst = [None, [], [1], None, [1, 2, 3], [4, 5], [], [6], None, [7, 8], [9]]
    structs = [None] + [
        {"a": i, "b": None if i % 3 == 0 else f"s{i}"} for i in range(N_ROWS - 1)
    ]
    maps = [None, {}, {"a": 1}, {"b": 2, "a": 3}, None,
            {"c": 4}, {"d": 5}, {"e": 6}, {"f": 7}, {"g": 8}, {"h": 9}]
    uuids = [None] + [bytes.fromhex(f"{i:032x}") for i in range(N_ROWS - 1)]

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("flag", pa.bool_()),
        pa.field("ts", pa.timestamp("us")),
        pa.field("d", pa.date32()),
        pa.field("dec", pa.decimal128(10, 2)),
        pa.field("fx", pa.binary(4)),
        pa.field("bin", pa.binary()),
        pa.field("hf", pa.float16()),
        pa.field("lst", pa.list_(pa.int32())),
        pa.field("st", pa.struct([pa.field("a", pa.int32()),
                                  pa.field("b", pa.string())])),
        pa.field("mp", pa.map_(pa.string(), pa.int32())),
        pa.field("u", pa.uuid()),
    ])
    return pa.Table.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(names, type=pa.string()),
            pa.array(scores, type=pa.float64()),
            pa.array(flags, type=pa.bool_()),
            pa.array(ts, type=pa.timestamp("us")),
            pa.array(dates, type=pa.date32()),
            pa.array(decs, type=pa.decimal128(10, 2)),
            pa.array(fixed, type=pa.binary(4)),
            pa.array(binary, type=pa.binary()),
            pa.array(half, type=pa.float16()),
            pa.array(lst, type=pa.list_(pa.int32())),
            pa.array(structs, type=pa.struct([pa.field("a", pa.int32()),
                                              pa.field("b", pa.string())])),
            pa.array(maps, type=pa.map_(pa.string(), pa.int32())),
            pa.array(uuids, type=pa.uuid()),
        ],
        schema=schema,
    )


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    table = build_table()
    # sanity: NaN survives
    assert math.isnan(table.column("score")[3].as_py())
    v1 = os.path.join(OUT, "pyarrow_multi_rowgroup.parquet")
    v2 = os.path.join(OUT, "pyarrow_multi_rowgroup_v2.parquet")
    pq.write_table(table, v1, compression="snappy", row_group_size=ROW_GROUP_SIZE,
                   data_page_version="1.0", write_statistics=True,
                   use_dictionary=True, version="1.0")
    pq.write_table(table, v2, compression="zstd", row_group_size=ROW_GROUP_SIZE,
                   data_page_version="2.0", write_statistics=True,
                   use_dictionary=True, version="1.0")
    for path in (v1, v2):
        pf = pq.ParquetFile(path)
        print(f"{os.path.relpath(path, ROOT)}: {pf.metadata.num_rows} rows, "
              f"{pf.metadata.num_row_groups} row groups, "
              f"{os.path.getsize(path)} bytes, created_by={pf.metadata.created_by}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
