#!/usr/bin/env python3
"""Verify Dart writer output with pyarrow, DuckDB and Polars.

Usage (from the repository root):

    dart run tool/interop/write_matrix.dart
    .agents/venv/Scripts/python.exe tool/interop/verify_writer.py

Reads ``<dir>/writer_matrix.json`` produced by ``write_matrix.dart`` and, for
every Parquet file, checks:

  * row count / row group count and the leaf schema against the Dart manifest;
  * pyarrow, DuckDB and Polars decode identical canonical values (checksums,
    null counts, lengths and sample rows).

Exit code is non-zero on any real failure. LZO is expected to be unsupported
by all three readers (none ship an LZO decoder); those cases are reported as
``unsupported`` rather than failures.

Python is only needed for this optional bidirectional check; Dart CI does not
depend on it.
"""

from __future__ import annotations

import json
import os
import struct
import sys
import traceback

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import interop_canon as canon  # noqa: E402

DEFAULT_DIR = os.path.join(".agents", "interop", "writer_out")

# Canonical names used by the Dart manifest -> pyarrow physical type strings.
PHYSICAL_TO_ARROW = {
    "boolean": "BOOLEAN",
    "int32": "INT32",
    "int64": "INT64",
    "int96": "INT96",
    "float": "FLOAT",
    "double": "DOUBLE",
    "byteArray": "BYTE_ARRAY",
    "fixedLenByteArray": "FIXED_LEN_BYTE_ARRAY",
}


def compare_schema(pf, expected: list[dict]) -> list[str]:
    actual = canon.leaves_from_parquet(pf)
    problems = []
    if len(actual) != len(expected):
        problems.append(f"leaf count: got {len(actual)}, want {len(expected)}")
        return problems
    for index, (got, want) in enumerate(zip(actual, expected)):
        for key in ("path", "physical", "type_length", "logical", "max_def",
                    "max_rep", "scale", "precision"):
            if got[key] != want[key]:
                problems.append(
                    f"leaf[{index}] {got['path']}: {key} got {got[key]!r}, "
                    f"want {want[key]!r}")
    return problems


def compare_columns(name: str, table, expected: list[dict],
                    flat_meta: dict, expected_logical: dict) -> list[str]:
    try:
        actual = canon.canon_table(table, flat_meta)
    except Exception as exc:  # noqa: BLE001 - report engine/type gaps
        return [f"canonicalization failed: {type(exc).__name__}: {exc}"]
    # Some engines widen FLOAT16 (FLBA(2)) to FLOAT. Re-encode the widened
    # values as raw half bytes so the comparison stays value-exact.
    actual = [_normalise_float16(table, column, expected_logical)
              for column in actual]
    problems = []
    if len(actual) != len(expected):
        return [f"column count: got {len(actual)}, want {len(expected)}"]
    for got, want in zip(actual, expected):
        for key in ("name", "length", "null_count", "checksum"):
            if got[key] != want[key]:
                problems.append(
                    f"column {want['name']}: {key} got {got[key]!r}, "
                    f"want {want[key]!r}")
        if got["samples"] != want["samples"]:
            problems.append(f"column {want['name']}: sample values differ")
    return problems


def _normalise_float16(table, column: dict, expected_logical: dict) -> dict:
    if expected_logical.get(column["name"]) != "float16":
        return column
    array = table.column(column["name"]).combine_chunks()
    if pa.types.is_float16(array.type):
        return column
    if not (pa.types.is_float32(array.type) or pa.types.is_float64(array.type)):
        return column
    trees = []
    validity = array.is_valid().to_pylist()
    for i in range(len(array)):
        if not validity[i]:
            trees.append(None)
        else:
            half = struct.pack("<e", float(array[i].as_py()))
            trees.append({"hex": half.hex()})
    return canon.column_entry(column["name"], "FixedLenColumn", trees)


def engine_read(engine: str, path: str):
    if engine == "pyarrow":
        pf = pq.ParquetFile(path)
        return pf.read()
    if engine == "duckdb":
        con = duckdb.connect()
        try:
            return con.execute("SELECT * FROM read_parquet(?)", [path]).to_arrow_table()
        finally:
            con.close()
    if engine == "polars":
        return pl.read_parquet(path).to_arrow()
    raise ValueError(engine)


def engine_error(engine: str, exc: Exception) -> dict:
    text = str(exc).strip().splitlines()[0] if str(exc).strip() else repr(exc)
    return {"status": "error", "type": type(exc).__name__, "error": text[:400]}


def run_case(entry: dict, directory: str) -> dict:
    path = os.path.join(directory, entry["file"])
    result = {
        "file": entry["file"],
        "shape": entry["shape"],
        "compression": entry["compression"],
        "page_version": entry["page_version"],
        "dictionary": entry["dictionary"],
        "rows": entry["rows"],
        "row_groups": entry["row_groups"],
        "engines": {},
    }
    expected_columns = entry["columns"]
    expected_leaves = entry["schema"]
    expected_logical = {leaf["path"]: leaf["logical"]
                        for leaf in expected_leaves if "." not in leaf["path"]}

    # Metadata + file-level schema (all readers share the same file).
    try:
        pf = pq.ParquetFile(path)
        md = pf.metadata
        if md.num_rows != entry["rows"]:
            result["engines"]["file"] = {
                "status": "fail",
                "error": f"num_rows {md.num_rows} != {entry['rows']}",
            }
        elif md.num_row_groups != entry["row_groups"]:
            result["engines"]["file"] = {
                "status": "fail",
                "error": f"num_row_groups {md.num_row_groups} != "
                         f"{entry['row_groups']}",
            }
        else:
            schema_problems = compare_schema(pf, expected_leaves)
            result["engines"]["file"] = (
                {"status": "pass"} if not schema_problems
                else {"status": "fail", "error": "; ".join(schema_problems[:8])})
        flat_meta = canon.flat_leaves_from_parquet(pf)
    except Exception as exc:  # noqa: BLE001
        result["engines"]["file"] = engine_error("file", exc)
        flat_meta = {}

    for engine in ("pyarrow", "duckdb", "polars"):
        try:
            table = engine_read(engine, path)
        except Exception as exc:  # noqa: BLE001
            if entry["compression"] == "lzo":
                result["engines"][engine] = {
                    "status": "unsupported",
                    "error": str(exc).strip().splitlines()[0][:240],
                }
            else:
                result["engines"][engine] = engine_error(engine, exc)
            continue
        # Some engines may return a reader/batch object; normalise to a Table.
        if not hasattr(table, "schema"):
            table = table.read_all()
        problems = compare_columns(engine, table, expected_columns, flat_meta,
                                   expected_logical)
        result["engines"][engine] = (
            {"status": "pass"} if not problems
            else {"status": "fail", "error": "; ".join(problems[:8])})
    return result


def main(argv: list[str]) -> int:
    directory = DEFAULT_DIR
    for i, arg in enumerate(argv):
        if arg == "--dir" and i + 1 < len(argv):
            directory = argv[i + 1]
    manifest_path = os.path.join(directory, "writer_matrix.json")
    if not os.path.isfile(manifest_path):
        print(f"missing writer manifest: {manifest_path}\n"
              "run `dart run tool/interop/write_matrix.dart` first", file=sys.stderr)
        return 2
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)

    results = []
    for entry in manifest["fixtures"]:
        try:
            results.append(run_case(entry, directory))
        except Exception:  # noqa: BLE001 - never abort the whole matrix
            results.append({
                "file": entry["file"],
                "engines": {"harness": {
                    "status": "fail",
                    "error": traceback.format_exc(limit=3)[-400:],
                }},
            })

    engines = ("file", "pyarrow", "duckdb", "polars")
    counts = {engine: {"pass": 0, "fail": 0, "unsupported": 0, "error": 0}
              for engine in engines}
    for result in results:
        for engine in engines:
            status = result["engines"].get(engine, {}).get("status", "error")
            counts[engine][status] += 1

    report = {
        "directory": directory,
        "cases": len(results),
        "counts": counts,
        "results": results,
    }
    report_path = os.path.join(directory, "verify_report.json")
    with open(report_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=1)
        handle.write("\n")

    print(f"verified {len(results)} writer files in {directory}")
    for engine in engines:
        c = counts[engine]
        print(f"  {engine:8} pass={c['pass']:2} fail={c['fail']:2} "
              f"unsupported={c['unsupported']:2} error={c['error']:2}")

    failures = [r for r in results
                if any(r["engines"].get(e, {}).get("status") in ("fail", "error")
                       for e in engines)]
    if failures:
        print("\nfailures:")
        for result in failures:
            for engine in engines:
                status = result["engines"].get(engine, {}).get("status")
                if status in ("fail", "error"):
                    print(f"  {result['file']} [{engine}] "
                          f"{result['engines'][engine].get('error', '')[:240]}")
    else:
        print("\nall engine comparisons passed (LZO expected-unsupported)")

    # A non-LZO engine failure or any metadata/schema mismatch fails the check.
    hard_failures = [r for r in results
                     if any(r["engines"].get(e, {}).get("status") in ("fail", "error")
                            for e in engines)]
    report["hard_failures"] = len(hard_failures)
    with open(report_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=1)
        handle.write("\n")
    return 1 if hard_failures else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
