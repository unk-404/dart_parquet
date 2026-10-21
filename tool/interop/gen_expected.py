#!/usr/bin/env python3
"""Generate pyarrow-derived expected JSON for the Dart interop test suite.

Run from the repository root with the interop venv:

    .agents/venv/Scripts/python.exe tool/interop/gen_expected.py

Reads the committed fixtures under ``test/fixtures/interop/{official,bad_data,
generated}`` and writes:

    test/fixtures/interop/expected/<fixture>.json   one file per fixture
    test/fixtures/interop/expected/manifest.json   index + provenance

The Dart tests under ``test/interop`` only consume the committed JSON; Python
is not required to run the Dart test suite or CI.
"""

from __future__ import annotations

import json
import os
import sys

import pyarrow
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import interop_canon as canon  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FIXTURES = os.path.join(ROOT, "test", "fixtures", "interop")
EXPECTED = os.path.join(FIXTURES, "expected")

# Fixtures the current reader rejects with a typed exception by design.
UNSUPPORTED = {
    "encrypt_columns_and_footer.parquet.encrypted",
    "uniform_encryption.parquet.encrypted",
}

# Files that are malformed Parquet; readers must fail with a typed error.
MALFORMED = {
    "ARROW-GH-45185.parquet",
    "ARROW-RS-GH-11261-FLBA-DICT.parquet",
    "ARROW-RS-GH-6229-DICTHEADER.parquet",
    "ARROW-RS-GH-6229-LEVELS.parquet",
    "PARQUET-1481.parquet",
    "nation.dict-malformed.parquet",
}

# Hand-written expectations for files pyarrow cannot decode (or the reader
# accepts more leniently than pyarrow).
MANUAL = {
    "flba12_timestamp.parquet",
    "incorrect_map_schema.parquet",
}

# Reader tolerates an out-of-spec UUID annotation on INT32; pyarrow reports
# it as Undefined, so restore the declared kind (handled in gen_values).

# File-specific exception overrides.
EXCEPTION_OVERRIDES = {}

# Known accepted-reader gaps: expected JSON is metadata-only and the Dart test
# skips the value assertions with the recorded reason.
KNOWN_GAPS: dict[str, str] = {
    "map_no_value.parquet":
        "pyarrow canonicalizes a MAP without values as a key list",
    "repeated_no_annotation.parquet":
        "legacy FileMetaData.num_rows=0 while the row group has 6 rows",
}

EXCEPTION_BY_KIND = {
    "unsupported": "ParquetUnsupportedException",
    "malformed": "ParquetException",
}


def enumerate_fixtures() -> list[str]:
    files = []
    for sub in ("official", "bad_data", "generated"):
        directory = os.path.join(FIXTURES, sub)
        if not os.path.isdir(directory):
            continue
        for name in sorted(os.listdir(directory)):
            if name.endswith(".parquet") or name.endswith(".parquet.encrypted"):
                files.append(f"{sub}/{name}")
    return files


def pyarrow_probe(path: str) -> dict:
    """Best-effort pyarrow metadata + read status."""
    status = {"ok": False, "metadata_ok": False, "read_ok": False,
              "error": None, "read_error": None}
    try:
        pf = pq.ParquetFile(path)
    except Exception as exc:  # noqa: BLE001 - recorded as provenance
        status["error"] = f"{type(exc).__name__}: {exc}"
        return status
    status["metadata_ok"] = True
    try:
        table = pf.read()
        status["ok"] = True
        status["read_ok"] = True
        status["_table"] = table
        status["_pf"] = pf
    except Exception as exc:  # noqa: BLE001
        status["read_error"] = f"{type(exc).__name__}: {exc}"
    status["_pf"] = pf
    return status


def file_meta(pf) -> dict:
    md = pf.metadata
    return {
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "created_by": md.created_by or None,
    }


def gen_values(rel: str, path: str, probe: dict, out: dict) -> None:
    pf = probe["_pf"]
    table = probe["_table"]
    flat_meta = canon.flat_leaves_from_parquet(pf)
    out["meta"] = file_meta(pf)
    out["leaves"] = canon.leaves_from_parquet(pf)
    out["columns"] = canon.canon_table(table, flat_meta)
    out["pyarrow"] = {"ok": True, "version": pyarrow.__version__}
    # Reader tolerates an out-of-spec UUID annotation on INT32; pyarrow reports
    # it as Undefined, so restore the declared kind.
    if rel.endswith("int32_with_uuid_logical_type.parquet"):
        for leaf in out["leaves"]:
            if leaf["path"] == "int32_uuid":
                leaf["logical"] = "uuid"
    # An unknown LogicalType union id is preserved as UnknownLogicalType by the
    # reader; pyarrow reports the same union as Undefined.
    if rel.endswith("unknown-logical-type.parquet"):
        for leaf in out["leaves"]:
            if leaf["path"] == "column with unknown type":
                leaf["logical"] = "unknown"
    # pyarrow narrows INT96 to int64 nanoseconds, which overflows for the
    # year-290000 value. Rows 0-4 are reconstructed from the documented
    # microsecond values in data/int96_from_spark.md; row 5's raw 96-bit words
    # themselves encode an out-of-range timestamp (negative Julian day and
    # nanos-of-day), so its literal on-disk bytes are pinned as the expectation.
    if rel.endswith("int96_from_spark.parquet"):
        micros = [1704141296123456, 1704070800000000, 253402225200000000,
                  1735599600000000, None]
        day_ns = 86_400 * 1_000_000_000
        trees = []
        for value in micros:
            if value is None:
                trees.append(None)
                continue
            days, remainder = divmod(value * 1000, day_ns)
            julian_day = 2_440_588 + days
            raw = (remainder.to_bytes(8, "little") +
                   julian_day.to_bytes(4, "little")).hex()
            trees.append({"hex": raw})
        trees.append({"hex": "0060b9c76ee2ffffa8abb0f9"})
        out["columns"] = [canon.column_entry("a", "Int96Column", trees)]
        out["note"] = (
            "Rows 0-4 reconstructed from the documented microseconds in "
            "apache/parquet-testing data/int96_from_spark.md; row 5 stores an "
            "out-of-range INT96 (negative Julian day/nanos) that pyarrow, "
            "DuckDB and Polars wrap or saturate, so its literal bytes are "
            "pinned from the fixture")


def gen_metadata_only(rel: str, path: str, probe: dict, out: dict,
                      kind: str) -> None:
    out["pyarrow"] = {
        "ok": probe["metadata_ok"],
        "read_ok": probe["read_ok"],
        "version": pyarrow.__version__,
        "error": probe["error"],
        "read_error": probe["read_error"],
    }
    if probe["metadata_ok"]:
        pf = probe["_pf"]
        out["meta"] = file_meta(pf)
        out["leaves"] = canon.leaves_from_parquet(pf)
    if kind in EXCEPTION_BY_KIND:
        out["exception"] = EXCEPTION_BY_KIND[kind]
    if rel.endswith("int32_with_uuid_logical_type.parquet"):
        for leaf in out.get("leaves", []):
            if leaf["path"] == "int32_uuid":
                leaf["logical"] = "uuid"


def manual_flba12(out: dict) -> None:
    out["meta"] = {
        "rows": 6,
        "row_groups": 1,
        "created_by": "parquet-mr version 1.18.0-SNAPSHOT "
                      "(build f954a72714b9a9c858bea8b016bc189deeb501fc)",
    }
    out["leaves"] = [
        {"path": "timestamp_millis", "physical": "fixedLenByteArray",
         "type_length": 12, "logical": "timestamp", "max_def": 1, "max_rep": 0,
         "scale": None, "precision": None},
        {"path": "timestamp_micros", "physical": "fixedLenByteArray",
         "type_length": 12, "logical": "timestamp", "max_def": 1, "max_rep": 0,
         "scale": None, "precision": None},
        {"path": "timestamp_nanos", "physical": "fixedLenByteArray",
         "type_length": 12, "logical": "timestamp", "max_def": 1, "max_rep": 0,
         "scale": None, "precision": None},
    ]
    # Values documented in apache/parquet-testing data/flba12_timestamp.md:
    # row -> epoch seconds. The 96-bit value is little-endian two's complement.
    epochs = [0, 1, -1, 9_223_372_036, 253_402_300_799, -62_135_596_800]
    units = {"timestamp_millis": 1_000,
             "timestamp_micros": 1_000_000,
             "timestamp_nanos": 1_000_000_000}
    columns = []
    for name, factor in units.items():
        trees = [{"hex": (seconds * factor).to_bytes(12, "little",
                                                     signed=True).hex()}
                 for seconds in epochs]
        columns.append(canon.column_entry(name, "FixedLenColumn", trees))
    out["columns"] = columns


def manual_incorrect_map(out: dict) -> None:
    out["meta"] = {
        "rows": 1,
        "row_groups": 1,
        "created_by": "parquet-mr version 1.12.2 "
                      "(build 77e30c8093386ec52c3cfa6c34b7ef3321322c94)",
    }
    out["leaves"] = [
        {"path": "my_map.key_value.key", "physical": "byteArray",
         "type_length": None, "logical": "string", "max_def": 3, "max_rep": 1,
         "scale": None, "precision": None},
        {"path": "my_map.key_value.value", "physical": "byteArray",
         "type_length": None, "logical": "string", "max_def": 3, "max_rep": 1,
         "scale": None, "precision": None},
    ]
    trees = [[["name", "report"], ["parent", "another"]]]
    out["columns"] = [canon.column_entry("my_map", "MapColumn", trees)]


def main() -> int:
    os.makedirs(EXPECTED, exist_ok=True)
    manifest = []
    for rel in enumerate_fixtures():
        path = os.path.join(FIXTURES, rel)
        name = os.path.basename(path)
        expected_name = name + ".json"
        expected_path = os.path.join(EXPECTED, expected_name)
        probe = pyarrow_probe(path)

        kind = "values"
        if name in UNSUPPORTED:
            kind = "unsupported"
        if name in MALFORMED:
            kind = "malformed"
        if name in MANUAL:
            kind = "manual"
        if name in KNOWN_GAPS:
            kind = "gap"

        entry = {
            "file": rel.replace(os.sep, "/"),
            "expected": "expected/" + expected_name,
            "kind": kind,
            "bytes": os.path.getsize(path),
            "sha256": canon.sha256_file(path),
        }
        out = {
            "file": entry["file"],
            "kind": kind,
            "writer_family": canon.writer_family(
                probe["_pf"].metadata.created_by if probe["metadata_ok"] else None),
        }
        if kind == "values":
            gen_values(rel, path, probe, out)
        elif kind in ("unsupported", "malformed", "gap"):
            gen_metadata_only(rel, path, probe, out, kind)
            if name in EXCEPTION_OVERRIDES:
                out["exception"] = EXCEPTION_OVERRIDES[name]
            if kind == "gap":
                out["gap"] = KNOWN_GAPS[name]
        elif kind == "manual":
            out["pyarrow"] = {
                "ok": probe["ok"],
                "read_ok": probe["read_ok"],
                "version": pyarrow.__version__,
                "error": probe["error"],
                "read_error": probe["read_error"],
            }
            if name == "flba12_timestamp.parquet":
                manual_flba12(out)
            elif name == "incorrect_map_schema.parquet":
                manual_incorrect_map(out)
            else:
                raise SystemExit(f"no manual expectation for {name}")

        if "meta" in out:
            entry.update({
                "rows": out["meta"]["rows"],
                "row_groups": out["meta"]["row_groups"],
                "created_by": out["meta"]["created_by"],
            })
        entry["writer_family"] = out.get("writer_family")
        if kind in ("unsupported", "malformed"):
            entry["exception"] = out["exception"]
        if kind == "gap":
            entry["gap"] = out["gap"]

        with open(expected_path, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(out, handle, ensure_ascii=False, indent=1, sort_keys=False)
            handle.write("\n")
        manifest.append(entry)
        print(f"{kind:12} {rel}")

    manifest.sort(key=lambda item: item["file"])
    doc = {
        "description": "pyarrow-derived interop expectations for dart_parquet",
        "generator": "tool/interop/gen_expected.py",
        "pyarrow_version": pyarrow.__version__,
        "canonical_encoding": (
            "tool/interop/interop_canon.py; FNV-1a 64 over canonical JSON "
            "per row; binary as hex, floats as IEEE bit hex, maps as sorted "
            "key/value pairs, structs as sorted-key objects"
        ),
        "fixtures": manifest,
    }
    with open(os.path.join(EXPECTED, "manifest.json"), "w", encoding="utf-8",
              newline="\n") as handle:
        json.dump(doc, handle, ensure_ascii=False, indent=1, sort_keys=False)
        handle.write("\n")
    counts = {}
    for item in manifest:
        counts[item["kind"]] = counts.get(item["kind"], 0) + 1
    print(f"\n{len(manifest)} fixtures, kinds={counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

