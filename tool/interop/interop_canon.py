#!/usr/bin/env python3
"""Deterministic value canonicalization shared by interop tooling.

Dart and Python must agree byte-for-byte on the canonical JSON form of every
value because the Dart tests and ``verify_writer.py`` compare checksums and
sample strings produced by this module (or its Dart twin in
``test/interop/interop_support.dart``).

Canonical JSON encoding (JSON-compatible tree):
  * null                -> null
  * bool                -> true/false
  * signed integer      -> decimal JSON number (64-bit two's complement)
  * FLOAT   (32-bit)    -> {"f32": "<8 lowercase hex digits>"}  raw IEEE bits
  * DOUBLE  (64-bit)    -> {"f64": "<16 lowercase hex digits>"} raw IEEE bits
  * HALF    (float16)   -> {"hex": "<4 hex digits>"}            raw FLBA bytes
  * binary / FLBA       -> {"hex": "<lowercase hex>"}
  * UTF-8 string        -> JSON string
  * list                -> JSON array
  * map                 -> [[key, value], ...] sorted by canonical key JSON
  * struct              -> JSON object with lexicographically sorted field names

A column checksum is FNV-1a 64 over ``canonical_json(value) + "\\n"`` for every
row, in row order.
"""

from __future__ import annotations

import decimal
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq

FNV_OFFSET = 0xCBF29CE484222325
FNV_PRIME = 0x100000001B3
MASK64 = (1 << 64) - 1


def canonical_json(tree) -> str:
    return json.dumps(tree, ensure_ascii=False, separators=(",", ":"))


def fnv1a64_hex(strings) -> str:
    h = FNV_OFFSET
    for text in strings:
        for byte in text.encode("utf-8") + b"\n":
            h ^= byte
            h = (h * FNV_PRIME) & MASK64
    return f"{h:016x}"


def column_checksum(trees) -> str:
    return fnv1a64_hex(canonical_json(tree) for tree in trees)


def _valid_list(arr):
    # pyarrow Array.is_valid() takes no arguments and returns a BooleanArray.
    return arr.is_valid().to_pylist()


def _signed(value: int, bits: int) -> int:
    value &= (1 << bits) - 1
    if value >= 1 << (bits - 1):
        value -= 1 << bits
    return value


def _float_bits(arr, width: int):
    """Raw IEEE bits for a float array, as big-endian numeric hex.

    The buffers store little-endian bytes; Dart formats the numeric bit pattern
    (e.g. -0.0 -> "8000000000000000"), so convert to an integer first.
    """
    validity = _valid_list(arr)
    data = arr.buffers()[1]
    payload = data.to_pybytes() if data is not None else b""
    tag = {4: "f32", 8: "f64"}[width]
    out = []
    for i, valid in enumerate(validity):
        if not valid:
            out.append(None)
        else:
            start = (arr.offset + i) * width
            chunk = payload[start:start + width]
            if len(chunk) != width:
                raise ValueError(f"float buffer ended at row {i}")
            bits = int.from_bytes(chunk, "little")
            out.append({tag: f"{bits:0{width * 2}x}"})
    return out


def _half_values(arr):
    validity = _valid_list(arr)
    data = arr.buffers()[1]
    payload = data.to_pybytes() if data is not None else b""
    out = []
    for i, valid in enumerate(validity):
        if not valid:
            out.append(None)
        else:
            start = (arr.offset + i) * 2
            out.append({"hex": payload[start:start + 2].hex()})
    return out


def _decimal_tree(value: decimal.Decimal, scale: int, physical: str,
                  width: int | None):
    raw = int(value.scaleb(scale))
    if physical == "INT32":
        return _signed(raw, 32)
    if physical == "INT64":
        return _signed(raw, 64)
    if physical == "FIXED_LEN_BYTE_ARRAY":
        if width is None:
            raise ValueError("FLBA decimal without type length")
        return {"hex": raw.to_bytes(width, "big", signed=True).hex()}
    if physical == "BYTE_ARRAY":
        # Canonical minimal big-endian two's complement representation.
        length = max(1, (raw.bit_length() + 8) // 8)
        return {"hex": raw.to_bytes(length, "big", signed=True).hex()}
    raise ValueError(f"decimal on unexpected physical type {physical}")


def _canon_decimal_column(arr, meta) -> list:
    validity = _valid_list(arr)
    out = []
    for i, valid in enumerate(validity):
        if not valid:
            out.append(None)
        else:
            out.append(_decimal_tree(arr[i].as_py(), arr.type.scale,
                                     meta["physical"], meta["type_length"]))
    return out


def _int96_bytes(nanos: int) -> str:
    day_ns = 86_400 * 1_000_000_000
    days, remainder = divmod(nanos, day_ns)
    julian_day = 2_440_588 + days
    return (remainder.to_bytes(8, "little") +
            julian_day.to_bytes(4, "little")).hex()


def _canon_int96_column(arr) -> list:
    nanos = arr.cast(pa.int64()).to_pylist()
    return [None if value is None else {"hex": _int96_bytes(value)}
            for value in nanos]


def canon_array(arr, flat_meta=None) -> list:
    """Canonical trees for every row of a pyarrow Array / ChunkedArray."""
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()
    kind = arr.type
    flat_meta = flat_meta or {}

    if pa.types.is_dictionary(kind):
        return canon_array(arr.dictionary_decode(), flat_meta)
    if isinstance(arr, pa.ExtensionArray):
        # e.g. arrow.json in pyarrow >= 19; the Dart reader sees the storage
        # type (string for JSON, fixed_size_binary for UUID).
        return canon_array(arr.storage, flat_meta)
    if pa.types.is_null(kind):
        return [None] * len(arr)

    if pa.types.is_struct(kind):
        validity = _valid_list(arr)
        names = sorted(kind.field(i).name for i in range(kind.num_fields))
        fields = {name: canon_array(arr.field(name), flat_meta) for name in names}
        out = []
        for i, valid in enumerate(validity):
            if not valid:
                out.append(None)
            else:
                out.append({name: fields[name][i] for name in names})
        return out

    if pa.types.is_list(kind) or pa.types.is_large_list(kind):
        validity = _valid_list(arr)
        offsets = arr.offsets.to_pylist()
        child = canon_array(arr.values, flat_meta)
        out = []
        for i, valid in enumerate(validity):
            if not valid:
                out.append(None)
            else:
                out.append(child[offsets[i]:offsets[i + 1]])
        return out

    if pa.types.is_fixed_size_list(kind):
        validity = _valid_list(arr)
        width = kind.list_size
        child = canon_array(arr.values, flat_meta)
        out = []
        for i, valid in enumerate(validity):
            if not valid:
                out.append(None)
            else:
                out.append(child[i * width:(i + 1) * width])
        return out

    if pa.types.is_map(kind):
        validity = _valid_list(arr)
        offsets = arr.offsets.to_pylist()
        keys = canon_array(arr.keys, flat_meta)
        items = canon_array(arr.items, flat_meta)
        out = []
        for i, valid in enumerate(validity):
            if not valid:
                out.append(None)
                continue
            pairs = [[keys[j], items[j]] for j in range(offsets[i], offsets[i + 1])]
            pairs.sort(key=lambda pair: canonical_json(pair[0]))
            out.append(pairs)
        return out

    if pa.types.is_boolean(kind):
        return arr.to_pylist()

    if pa.types.is_int8(kind) or pa.types.is_int16(kind) or pa.types.is_int32(kind):
        return arr.to_pylist()
    if pa.types.is_int64(kind):
        return arr.to_pylist()
    if pa.types.is_uint8(kind) or pa.types.is_uint16(kind):
        return arr.to_pylist()
    if pa.types.is_uint32(kind):
        return [_signed(v, 32) if v is not None else None for v in arr.to_pylist()]
    if pa.types.is_uint64(kind):
        return [_signed(v, 64) if v is not None else None for v in arr.to_pylist()]

    if pa.types.is_float32(kind):
        return _float_bits(arr, 4)
    if pa.types.is_float64(kind):
        return _float_bits(arr, 8)
    if pa.types.is_float16(kind):
        return _half_values(arr)

    if pa.types.is_date32(kind):
        return arr.cast(pa.int32()).to_pylist()
    if pa.types.is_date64(kind):
        return arr.cast(pa.int64()).to_pylist()
    if pa.types.is_time32(kind):
        return arr.cast(pa.int32()).to_pylist()
    if pa.types.is_time64(kind):
        return arr.cast(pa.int64()).to_pylist()
    if pa.types.is_timestamp(kind):
        return arr.cast(pa.int64()).to_pylist()

    if pa.types.is_decimal(kind):
        validity = _valid_list(arr)
        meta = flat_meta.get("__self__")
        if meta is None:
            raise ValueError("decimal column without physical metadata")
        out = []
        for i, valid in enumerate(validity):
            if not valid:
                out.append(None)
            else:
                out.append(_decimal_tree(arr[i].as_py(), kind.scale,
                                         meta["physical"], meta["type_length"]))
        return out

    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return arr.to_pylist()
    if (pa.types.is_binary(kind) or pa.types.is_large_binary(kind)
            or pa.types.is_fixed_size_binary(kind)):
        return [None if v is None else {"hex": v.hex()} for v in arr.to_pylist()]

    raise TypeError(f"unsupported arrow type for canonicalization: {kind}")


def canon_table(table: pa.Table, flat_meta: dict | None = None) -> list[dict]:
    """Per-column canonical trees/checksums for a pyarrow Table."""
    flat_meta = flat_meta or {}
    columns = []
    for field in table.schema:
        col = table.column(field.name).combine_chunks()
        meta = flat_meta.get(field.name)
        if pa.types.is_decimal(col.type):
            if meta is None:
                raise ValueError(f"decimal column {field.name} has no flat metadata")
            trees = _canon_decimal_column(col, meta)
        elif meta is not None and meta["physical"] == "INT96":
            trees = _canon_int96_column(col)
        else:
            trees = canon_array(col, flat_meta)
        columns.append(column_entry(field.name, dart_class(field.type, meta), trees))
    return columns


def column_entry(name: str, dclass: str, trees) -> dict:
    null_count = sum(1 for tree in trees if tree is None)
    sample_rows = sorted(set(list(range(min(3, len(trees)))) +
                             list(range(max(0, len(trees) - 3), len(trees)))))
    return {
        "name": name,
        "dart_class": dclass,
        "length": len(trees),
        "null_count": null_count,
        "checksum": column_checksum(trees),
        "sample_rows": sample_rows,
        "samples": [canonical_json(trees[i]) for i in sample_rows],
    }


def leaves_from_parquet(pf: pq.ParquetFile) -> list[dict]:
    out = []
    sch = pf.schema
    for i in range(len(sch.names)):
        col = sch.column(i)
        physical = col.physical_type
        type_length = col.length if col.length and col.length > 0 else None
        logical_text = str(col.logical_type)
        is_decimal = logical_text.startswith("Decimal")
        scale = col.scale if is_decimal and col.scale is not None and col.scale >= 0 else None
        precision = (col.precision if is_decimal and col.precision is not None
                     and col.precision >= 0 else None)
        out.append({
            "path": col.path,
            "physical": physical_name(physical),
            "type_length": type_length,
            "logical": logical_kind(logical_text),
            "max_def": col.max_definition_level,
            "max_rep": col.max_repetition_level,
            "scale": scale,
            "precision": precision,
        })
    return out


def flat_leaves_from_parquet(pf: pq.ParquetFile) -> dict:
    meta = {}
    sch = pf.schema
    for i in range(len(sch.names)):
        col = sch.column(i)
        if "." in col.path:
            continue
        meta[col.path] = {
            "physical": col.physical_type,
            "type_length": (col.length
                            if col.physical_type == "FIXED_LEN_BYTE_ARRAY" else None),
        }
    return meta


PHYSICAL_NAMES = {
    "BOOLEAN": "boolean",
    "INT32": "int32",
    "INT64": "int64",
    "INT96": "int96",
    "FLOAT": "float",
    "DOUBLE": "double",
    "BYTE_ARRAY": "byteArray",
    "FIXED_LEN_BYTE_ARRAY": "fixedLenByteArray",
}

LOGICAL_NAMES = {
    "None": None,
    "String": "string",
    "Map": "map",
    "List": "list",
    "Enum": "enumType",
    "Date": "date",
    "Null": "unknown",
    "Unknown": "unknown",
    "JSON": "json",
    "BSON": "bson",
    "Uuid": "uuid",
    "UUID": "uuid",
    "Float16": "float16",
    "Variant": "variant",
    "Geometry": "geometry",
    "Geography": "geography",
    "File": "file",
    "Undefined": None,
}


def physical_name(name: str) -> str:
    return PHYSICAL_NAMES[name]


def logical_kind(text: str):
    if text in LOGICAL_NAMES:
        return LOGICAL_NAMES[text]
    if text.startswith("Decimal"):
        return "decimal"
    if text.startswith("Time("):
        return "time"
    if text.startswith("Timestamp"):
        return "timestamp"
    if text.startswith("Int("):
        return "integer"
    raise ValueError(f"unmapped logical type {text!r}")


def dart_class(arrow_type, meta) -> str:
    if pa.types.is_struct(arrow_type):
        return "StructColumn"
    if (pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)):
        return "ListColumn"
    if pa.types.is_map(arrow_type):
        return "MapColumn"
    if pa.types.is_null(arrow_type):
        return "NullColumn"
    physical = meta["physical"] if meta else None
    if physical == "INT96":
        return "Int96Column"
    if physical == "INT32":
        return "Int32Column"
    if physical == "INT64":
        return "Int64Column"
    if physical == "BOOLEAN":
        return "BooleanColumn"
    if physical == "FLOAT":
        return "Float32Column"
    if physical == "DOUBLE":
        return "Float64Column"
    if physical == "FIXED_LEN_BYTE_ARRAY":
        return "FixedLenColumn"
    if physical == "BYTE_ARRAY":
        return "BinaryColumn"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "BinaryColumn"
    if (pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type)
            or pa.types.is_fixed_size_binary(arrow_type)):
        return "BinaryColumn"
    if pa.types.is_float16(arrow_type):
        return "FixedLenColumn"
    if (pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type)
            or pa.types.is_int32(arrow_type)):
        return "Int32Column"
    if pa.types.is_int64(arrow_type):
        return "Int64Column"
    if pa.types.is_float32(arrow_type):
        return "Float32Column"
    if pa.types.is_float64(arrow_type):
        return "Float64Column"
    if pa.types.is_boolean(arrow_type):
        return "BooleanColumn"
    if pa.types.is_timestamp(arrow_type):
        return "Int64Column"
    if pa.types.is_date32(arrow_type) or pa.types.is_time32(arrow_type):
        return "Int32Column"
    if pa.types.is_date64(arrow_type) or pa.types.is_time64(arrow_type):
        return "Int64Column"
    raise ValueError(f"unmapped arrow type {arrow_type}")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def writer_family(created_by) -> str:
    if not created_by:
        return "unknown"
    text = created_by.lower()
    if "impala" in text:
        return "impala"
    if "parquet-mr" in text:
        return "parquet-mr"
    if "parquet-cpp-arrow" in text:
        return "arrow-cpp"
    if "parquet-cpp" in text:
        return "parquet-cpp"
    if "parquet-rs" in text or "arrow-rs" in text:
        return "arrow-rs"
    if "urbanlogiq" in text:
        return "rust-urbanlogiq"
    if "hvr" in text:
        return "hvr"
    return "unknown"
