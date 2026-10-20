#!/usr/bin/env python3
"""Generate the tiny, self-contained fixtures used by the Web/Wasm tests.

The browser test suites cannot read files from the package directory (there is
no ``dart:io``), so ``test/web/web_fixtures.dart`` embeds the same bytes as
base64 constants. Keep every fixture here small (a few KiB) and regenerate with:

    python test/fixtures/web/generate_fixtures.py

The committed ``*.parquet`` files are the source of truth for provenance and
Dart VM spot checks; the generated ``test/web/web_fixtures.dart`` is what the
browser tests consume.

Requires pyarrow (test-only; not a package dependency). The fixture values are
asserted by ``test/web/web_read_test.dart`` and ``test/wasm/wasm_read_test.dart``.
"""

from __future__ import annotations

import base64
import hashlib
import textwrap
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
DART_OUT = REPO / "test" / "web" / "web_fixtures.dart"

INT64_VALUES = [
    0,
    1,
    -1,
    42,
    -2,
    9007199254740992,          # 2^53, exact as a double
    9007199254740993,          # 2^53 + 1, only exact via BigInt
    -9223372036854775808,      # int64 min
]

FLAT = pa.table(
    {
        "i64": pa.array(INT64_VALUES, type=pa.int64()),
        "i32": pa.array([0, None, 2, 3, None, 5, 6, -7], type=pa.int32()),
        "f64": pa.array(
            [0.0, 1.5, None, -2.25, 3.75, 5.0, None, -7.5], type=pa.float64()
        ),
        "f32": pa.array(
            [0.5, 1.5, 2.5, None, 4.5, 5.5, 6.5, -7.5], type=pa.float32()
        ),
        "b": pa.array(
            [True, False, None, True, False, None, True, False],
            type=pa.bool_(),
        ),
        "s": pa.array(
            ["alpha", "beta", "", "delta", None, "zeta", "eta", "theta"],
            type=pa.string(),
        ),
        "bin": pa.array(
            [
                b"\x00\x01",
                b"\xff",
                b"",
                b"\x01\x02\x03",
                None,
                b"\x7f",
                b"\x80\x81",
                bytes(range(16)),
            ],
            type=pa.binary(),
        ),
        "fixed4": pa.array(
            [b"abcd", b"efgh", None, b"ijkl", b"mnop", b"qrst", b"uvwx", b"yz01"],
            type=pa.binary(4),
        ),
    }
)

NESTED = pa.table(
    {
        "id": pa.array([0, 1, 2, 3, 4, 5], type=pa.int32()),
        "lst": pa.array(
            [[], [10], [20, 21], [], [30, 31, 32], []],
            type=pa.list_(pa.int32()),
        ),
        "st": pa.array(
            [
                None,
                {"a": 1, "b": "s1"},
                {"a": 2, "b": None},
                None,
                {"a": 4, "b": "s4"},
                {"a": 5, "b": "s5"},
            ],
            type=pa.struct(
                [pa.field("a", pa.int32()), pa.field("b", pa.string())]
            ),
        ),
        "mp": pa.array(
            [
                {},
                {"k0": 0},
                {"k0": 0, "k1": 1},
                {},
                {"x": 9},
                {},
            ],
            type=pa.map_(pa.string(), pa.int32()),
        ),
    }
)

MULTI = pa.table(
    {
        "id": pa.array(list(range(12)), type=pa.int64()),
        "s": pa.array([f"row-{i:02d}" for i in range(12)], type=pa.string()),
        "flag": pa.array([i % 2 == 0 for i in range(12)], type=pa.bool_()),
    }
)

PAGE_INDEX = pa.table(
    {
        "id": pa.array(list(range(200)), type=pa.int64()),
        "s": pa.array([f"s{i:03d}" for i in range(200)], type=pa.string()),
        "flag": pa.array([i % 2 == 0 for i in range(200)], type=pa.bool_()),
    }
)


def write(name: str, table: pa.Table, **kwargs) -> None:
    path = HERE / name
    pq.write_table(table, path, **{**BASE, **kwargs})
    print(f"{name:32s} {path.stat().st_size:6d} bytes")


BASE = dict(compression="NONE", use_dictionary=False, data_page_version="1.0")


def main() -> None:
    # One flat table per writable codec, all PLAIN encoded.
    for codec, suffix in (
        ("NONE", "uncompressed"),
        ("snappy", "snappy"),
        ("gzip", "gzip"),
        ("brotli", "brotli"),
        ("lz4", "lz4"),
        ("zstd", "zstd"),
    ):
        write(f"flat_{suffix}.parquet", FLAT, compression=codec)

    # Dictionary encoding (strings + binary), snappy compressed.
    write(
        "flat_dict_snappy.parquet",
        FLAT,
        compression="snappy",
        use_dictionary=["s", "bin"],
    )

    # DELTA_BINARY_PACKED / DELTA_BYTE_ARRAY and DELTA_LENGTH_BYTE_ARRAY.
    write(
        "flat_delta.parquet",
        FLAT,
        compression="snappy",
        column_encoding={
            "i32": "DELTA_BINARY_PACKED",
            "i64": "DELTA_BINARY_PACKED",
            "s": "DELTA_BYTE_ARRAY",
        },
    )
    write(
        "flat_delta_length.parquet",
        FLAT,
        compression="snappy",
        column_encoding={"s": "DELTA_LENGTH_BYTE_ARRAY"},
    )

    # BYTE_STREAM_SPLIT for float/double, data page v2.
    write(
        "flat_bss_v2.parquet",
        FLAT,
        compression="snappy",
        data_page_version="2.0",
        column_encoding={"f32": "BYTE_STREAM_SPLIT", "f64": "BYTE_STREAM_SPLIT"},
    )

    # Nested list/struct/map: uncompressed, snappy and data page v2.
    write("nested.parquet", NESTED)
    write("nested_snappy.parquet", NESTED, compression="snappy")
    write(
        "nested_v2.parquet",
        NESTED,
        compression="snappy",
        data_page_version="2.0",
    )

    # Three row groups of four rows for readRowGroups()/rowGroupFilter.
    write(
        "multi_rowgroup.parquet",
        MULTI,
        compression="snappy",
        row_group_size=4,
    )

    # Page index / offset index with several data pages per column chunk.
    write(
        "page_index.parquet",
        PAGE_INDEX,
        compression="snappy",
        row_group_size=200,
        data_page_size=64,
        write_batch_size=16,
        write_page_index=True,
    )

    emit_dart()


def emit_dart() -> None:
    names = sorted(p.name for p in HERE.glob("*.parquet"))
    entries = []
    for name in names:
        raw = (HERE / name).read_bytes()
        payload = base64.b64encode(raw).decode("ascii")
        digest = hashlib.sha256(raw).hexdigest()[:16]
        lines = "\n".join(
            f"      '{chunk}'" for chunk in textwrap.wrap(payload, 64)
        )
        entries.append(
            f"  // {name}: {len(raw)} bytes, sha256 {digest}...\n"
            f"  '{name}':\n{lines},"
        )

    body = "\n".join(entries)
    source = f"""// GENERATED FILE - do not edit by hand.
//
// Tiny Parquet fixtures for the browser suites, base64-encoded so the tests
// are self-contained and need no `dart:io`. Regenerate with:
//
//     python test/fixtures/web/generate_fixtures.py
//
// The matching .parquet files live in `test/fixtures/web/`; they are the
// source of truth and are what pyarrow wrote. The values are asserted by
// `test/web/web_read_test.dart` and `test/wasm/wasm_read_test.dart`.
library;

import 'dart:convert';
import 'dart:typed_data';

/// Base64-encoded Parquet fixture bytes keyed by file name.
const Map<String, String> webFixtureBase64 = <String, String>{{
{body}
}};

/// Returns a fresh [Uint8List] with the bytes of fixture [name].
Uint8List webFixture(String name) {{
  final encoded = webFixtureBase64[name];
  if (encoded == null) {{
    throw ArgumentError.value(name, 'name', 'unknown web fixture');
  }}
  return base64Decode(encoded);
}}

/// All fixture names, sorted.
List<String> get webFixtureNames =>
    webFixtureBase64.keys.toList(growable: false);
"""
    DART_OUT.write_text(source, encoding="utf-8", newline="\n")
    print(f"\nwrote {DART_OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
