#!/usr/bin/env python3
"""Generate v0.2.0 interop expectations for Variant and ALP assets.

Run from the repository root:

    .agents/venv/Scripts/python.exe tool/interop/gen_v02_expected.py

Inputs (committed, see ``test/fixtures/interop/README.md``):
  * ``test/fixtures/interop/v0.2/variant/*.parquet`` and ``*.variant.bin``
    (apache/parquet-testing ``shredded_variant``, Apache-2.0, commit
    ``3acde70509f990c4ddfe34bf3ab05890657e766c``)
  * ``test/fixtures/interop/v0.2/alp/alp_extended.zstd.parquet``
  * existing deep-nesting fixtures under ``official/``

Outputs:
  * ``test/fixtures/interop/v0.2/expected/variant/<case>.json``
  * ``test/fixtures/interop/v0.2/expected/alp_extended.zstd.parquet.json``
  * ``test/fixtures/interop/v0.2/manifest.json``

pyarrow 26 has no Variant decoder but reads the physical VARIANT group as a
struct; DuckDB 1.5 and Polars 2.0 reject the VARIANT logical type. pyarrow can
read the PLAIN reference columns of the ALP file while the ALP-encoded columns
are unreadable by all three engines. Those gaps are recorded in the manifest;
the expected JSON for ALP is derived from the PLAIN reference columns, which is
what a future ALP decoder must match bit-for-bit.
"""

from __future__ import annotations

import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import interop_canon as canon  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FIXTURES = os.path.join(ROOT, "test", "fixtures", "interop")
V02 = os.path.join(FIXTURES, "v0.2")
EXPECTED = os.path.join(V02, "expected")

SOURCE_COMMIT = "3acde70509f990c4ddfe34bf3ab05890657e766c"
SOURCE_REPO = "https://github.com/apache/parquet-testing"
LICENSE = "Apache-2.0"

# Deep-nesting fixtures already present in the v0.1.2 subset. v0.2's generic
# Dremel reader is expected to read these; the manifest records the current
# v0.1.3 reader status for regression tracking.
DEEP_NESTING = [
    ("official/nested_lists.snappy.parquet",
     "data/nested_lists.snappy.parquet",
     "LIST<LIST<LIST<STRING>>> (three levels)",
     "ParquetUnsupportedException: list of list is not supported"),
    ("official/nested_maps.snappy.parquet",
     "data/nested_maps.snappy.parquet",
     "MAP<STRING, MAP<INT32, BOOLEAN>> plus leaf columns",
     "ParquetUnsupportedException: MAP with non-leaf key/value is not supported"),
    ("official/nullable.impala.parquet",
     "data/nullable.impala.parquet",
     "nullable Impala schema: LIST<LIST<INT32>>, LIST<MAP<...>>, "
     "STRUCT of LIST/MAP",
     "ParquetUnsupportedException: list of list is not supported"),
    ("official/nonnullable.impala.parquet",
     "data/nonnullable.impala.parquet",
     "required Impala schema: nested LIST/MAP/STRUCT shapes",
     "ParquetUnsupportedException: list of list is not supported"),
    ("official/old_list_structure.parquet",
     "data/old_list_structure.parquet",
     "legacy two-level LIST<LIST<INT32>>",
     "ParquetUnsupportedException: list of list/nested repetition is not "
     "supported"),
]

# Shredding classes for the selected Variant cases. Upstream cases.json is the
# authority for semantic expectations.
VARIANT_SHREDDING = {
    47: "unshredded",
    48: "unshredded",
    49: "unshredded",
    50: "unshredded",
    5: "least-shredded (single top-level typed_value, boolean)",
    6: "least-shredded (single top-level typed_value, int8)",
    130: "shallow shredded object (one level of object fields)",
    44: "nested shredded object (object fields containing objects)",
}

VARIANT_ENGINE_SUPPORT = {
    "pyarrow": {
        "read": "physical struct",
        "semantic_variant": False,
        "note": "pyarrow 26 has no VARIANT decoder; reads metadata/value/"
                "typed_value as a plain struct",
    },
    "duckdb": {
        "read": False,
        "error": "NotImplementedException: Unsupported Arrow type VARIANT",
    },
    "polars": {
        "read": False,
        "error": "ComputeError: parquet: File out of specification: "
                 "LogicalType union has no variant set",
    },
    "dart": {
        "unshredded": {
            "read": "VariantColumn",
            "semantic_variant": True,
            "note": "unshredded metadata+value groups decode via "
                    "lib/src/variant; cases 047-050 produce NULL/true/false/34, "
                    "matching upstream cases.json",
        },
        "shredded": {
            "read": False,
            "error": "ParquetUnsupportedException: shredded VARIANT var is not "
                     "supported in v0.2.0",
            "note": "typed_value shredding reconstruction is still v0.2 work "
                    "at collection time",
        },
    },
}

# Observed current-reader status per selected Variant case (2026-10-21).
VARIANT_DART_STATUS = {
    47: {"read": True, "column": "VariantColumn", "value_json": None},
    48: {"read": True, "column": "VariantColumn", "value_json": True},
    49: {"read": True, "column": "VariantColumn", "value_json": False},
    50: {"read": True, "column": "VariantColumn", "value_json": 34},
    5: {"read": False, "error": "ParquetUnsupportedException: shredded VARIANT "
                                "var is not supported in v0.2.0"},
    6: {"read": False, "error": "ParquetUnsupportedException: shredded VARIANT "
                                "var is not supported in v0.2.0"},
    44: {"read": False, "error": "ParquetUnsupportedException: shredded VARIANT "
                                 "var is not supported in v0.2.0"},
    130: {"read": False, "error": "ParquetUnsupportedException: shredded "
                                  "VARIANT var is not supported in v0.2.0"},
}


def sha256(path: str) -> str:
    return canon.sha256_file(path)


def rel(path: str) -> str:
    return os.path.relpath(path, FIXTURES).replace(os.sep, "/")


def engine_probe_variant(path: str) -> dict:
    support = dict(VARIANT_ENGINE_SUPPORT)
    try:
        table = pq.read_table(path)
        support["pyarrow"] = {
            "read": "physical struct",
            "semantic_variant": False,
            "rows": table.num_rows,
            "columns": table.column_names,
        }
    except Exception as exc:  # noqa: BLE001
        support["pyarrow"] = {"read": False,
                              "error": f"{type(exc).__name__}: {exc}"}
    import duckdb
    try:
        duckdb.connect().execute("SELECT * FROM read_parquet(?)",
                                 [path]).to_arrow_table()
        support["duckdb"] = {"read": True}
    except Exception as exc:  # noqa: BLE001
        support["duckdb"] = {"read": False,
                             "error": f"{type(exc).__name__}: "
                                      f"{str(exc).splitlines()[0]}"}
    import polars as pl
    try:
        pl.read_parquet(path)
        support["polars"] = {"read": True}
    except BaseException as exc:  # noqa: BLE001 - polars may raise PanicException
        support["polars"] = {"read": False,
                             "error": f"{type(exc).__name__}: "
                                      f"{str(exc).splitlines()[0]}"}
    return support


def write_variant(case: dict) -> dict:
    case_number = case["case_number"]
    parquet_name = case["parquet_file"]
    bin_name = case["variant_file"]
    parquet_path = os.path.join(V02, "variant", parquet_name)
    bin_path = os.path.join(V02, "variant", bin_name)
    pf = pq.ParquetFile(parquet_path)
    table = pf.read()
    flat_meta = canon.flat_leaves_from_parquet(pf)
    columns = canon.canon_table(table, flat_meta)
    bin_bytes = open(bin_path, "rb").read()

    expected = {
        "file": rel(parquet_path),
        "asset": "variant",
        "case_number": case_number,
        "test": case.get("test"),
        "shredding": VARIANT_SHREDDING[case_number],
        "variant_text": case.get("variant"),
        "variant_bin": rel(bin_path),
        "variant_bin_bytes": len(bin_bytes),
        "variant_bin_sha256": sha256(bin_path),
        "variant_bin_hex": bin_bytes.hex(),
        "meta": {
            "rows": pf.metadata.num_rows,
            "row_groups": pf.metadata.num_row_groups,
            "created_by": pf.metadata.created_by or None,
        },
        "leaves": canon.leaves_from_parquet(pf),
        "physical_columns": columns,
        "physical_columns_note": (
            "pyarrow-derived physical-struct values; dart_class is the "
            "generic struct mapping, while the current Dart reader exposes "
            "VariantColumn for unshredded VARIANT groups"),
        "engine_support": engine_probe_variant(parquet_path),
        "dart_reader": VARIANT_DART_STATUS[case_number],
        "note": (
            "Expected physical values come from pyarrow 26 (no semantic "
            "Variant decoding). The authoritative Variant semantics are the "
            "upstream cases.json 'variant' text plus the .variant.bin payload."
        ),
    }
    expected_dir = os.path.join(EXPECTED, "variant")
    os.makedirs(expected_dir, exist_ok=True)
    expected_path = os.path.join(expected_dir, parquet_name + ".json")
    with open(expected_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(expected, handle, ensure_ascii=False, indent=1)
        handle.write("\n")
    return {
        "case_number": case_number,
        "test": case.get("test"),
        "shredding": VARIANT_SHREDDING[case_number],
        "variant_text": case.get("variant"),
        "parquet": rel(parquet_path),
        "parquet_bytes": os.path.getsize(parquet_path),
        "parquet_sha256": sha256(parquet_path),
        "variant_bin": rel(bin_path),
        "variant_bin_bytes": len(bin_bytes),
        "variant_bin_sha256": sha256(bin_path),
        "dart_reader": VARIANT_DART_STATUS[case_number],
        "expected": rel(expected_path),
    }


def write_alp() -> dict:
    path = os.path.join(V02, "alp", "alp_extended.zstd.parquet")
    pf = pq.ParquetFile(path)
    # Read only the in-file PLAIN references: the ALP columns make whole-file
    # reads fail in pyarrow, DuckDB and Polars.
    reference = pq.read_table(path,
                              columns=["float_plain", "double_plain"])
    flat_meta = canon.flat_leaves_from_parquet(pf)
    reference_columns = [
        column for column in canon.canon_table(reference, flat_meta)
        if column["name"] in ("float_plain", "double_plain")
    ]

    whole_file = {}
    try:
        table = pf.read()
        whole_file["pyarrow"] = {"read": True, "columns": table.column_names}
    except Exception as exc:  # noqa: BLE001
        whole_file["pyarrow"] = {"read": False,
                                 "error": f"{type(exc).__name__}: {exc}"}
    import duckdb
    try:
        duckdb.connect().execute("SELECT * FROM read_parquet(?)",
                                 [path]).to_arrow_table()
        whole_file["duckdb"] = {"read": True}
    except Exception as exc:  # noqa: BLE001
        whole_file["duckdb"] = {"read": False,
                                "error": f"{type(exc).__name__}: "
                                         f"{str(exc).splitlines()[0]}"}
    import polars as pl
    try:
        pl.read_parquet(path)
        whole_file["polars"] = {"read": True}
    except BaseException as exc:  # noqa: BLE001 - polars raises PanicException
        whole_file["polars"] = {"read": False,
                                "error": f"{type(exc).__name__}: "
                                         f"{str(exc).splitlines()[0]}"}

    column_meta = []
    md = pf.metadata
    rg = md.row_group(0)
    for index in range(rg.num_columns):
        chunk = rg.column(index)
        encodings = list(chunk.encodings)
        column_meta.append({
            "name": chunk.path_in_schema,
            "compression": chunk.compression,
            "encodings": encodings,
            "alp": "UNKNOWN" in encodings,
            "plain_reference": chunk.path_in_schema in
                               ("float_plain", "double_plain"),
        })

    expected = {
        "file": rel(path),
        "asset": "alp",
        "bytes": os.path.getsize(path),
        "sha256": sha256(path),
        "meta": {
            "rows": md.num_rows,
            "row_groups": md.num_row_groups,
            "created_by": md.created_by or None,
        },
        "columns_meta": column_meta,
        "leaves": canon.leaves_from_parquet(pf),
        "reference_columns": reference_columns,
        "engine_support": {
            "whole_file": whole_file,
            "plain_reference_columns": {
                "pyarrow": "readable with columns=['float_plain','double_plain']",
                "duckdb": "not attempted with column projection",
                "polars": "not attempted with column projection",
            },
            "dart": {
                "whole_file": "readable",
                "alp_columns_bit_exact_vs_plain": 6,
                "note": "measured on the v0.2 development reader; all six "
                        "float_alp_*/double_alp_* columns match the PLAIN "
                        "reference columns bit-for-bit, including NaN payloads "
                        "and nulls",
            },
        },
        "note": (
            "ALP (encoding 10) is not decoded by pyarrow 26, DuckDB 1.5.6 or "
            "Polars 2.0.0; they fail on the whole file. The expected values "
            "for the six ALP columns are the in-file PLAIN reference columns "
            "(float_plain / double_plain), which the upstream fixture provides "
            "exactly for bit-comparison."
        ),
    }
    expected_path = os.path.join(EXPECTED, "alp_extended.zstd.parquet.json")
    with open(expected_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(expected, handle, ensure_ascii=False, indent=1)
        handle.write("\n")
    return {
        "file": rel(path),
        "bytes": os.path.getsize(path),
        "sha256": sha256(path),
        "rows": md.num_rows,
        "row_groups": md.num_row_groups,
        "columns": [entry["name"] for entry in column_meta],
        "expected": rel(expected_path),
    }


def main() -> int:
    os.makedirs(EXPECTED, exist_ok=True)
    cases_path = os.path.join(V02, "variant", "variant_cases.json")
    cases = json.load(open(cases_path, encoding="utf-8"))
    variant_entries = [write_variant(case) for case in cases]
    variant_entries.sort(key=lambda entry: entry["case_number"])
    alp_entry = write_alp()

    deep_entries = []
    for fixture, upstream, description, current in DEEP_NESTING:
        path = os.path.join(FIXTURES, fixture)
        deep_entries.append({
            "file": fixture,
            "upstream": f"{SOURCE_REPO}/blob/{SOURCE_COMMIT}/{upstream}",
            "bytes": os.path.getsize(path),
            "sha256": sha256(path),
            "description": description,
            "current_reader_status": current,
            "v0.2_target": "generic Dremel nested read",
        })

    manifest = {
        "description": "v0.2.0 interop assets: Variant, ALP and deep nesting",
        "generator": "tool/interop/gen_v02_expected.py",
        "pyarrow_version": pa.__version__,
        "dart_status_as_of": "2026-10-21",
        "source": {
            "repository": SOURCE_REPO,
            "commit": SOURCE_COMMIT,
            "license": LICENSE,
            "documents": [
                f"{SOURCE_REPO}/tree/{SOURCE_COMMIT}/shredded_variant",
                f"{SOURCE_REPO}/tree/{SOURCE_COMMIT}/variant",
                f"{SOURCE_REPO}/blob/{SOURCE_COMMIT}/data/README.md#alp-encoding",
            ],
        },
        "limits": {
            "max_file_bytes": 1 << 20,
            "largest_file_bytes": max(
                [entry["parquet_bytes"] for entry in variant_entries] +
                [alp_entry["bytes"]]),
        },
        "variant": variant_entries,
        "alp": alp_entry,
        "deep_nesting": deep_entries,
    }
    manifest_path = os.path.join(V02, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=1)
        handle.write("\n")
    print(f"variant cases: {len(variant_entries)}")
    print(f"alp: {alp_entry['file']} ({alp_entry['bytes']} bytes)")
    print(f"deep nesting entries: {len(deep_entries)}")
    print(f"manifest: {rel(manifest_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
