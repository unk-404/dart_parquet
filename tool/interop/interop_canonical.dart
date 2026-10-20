/// Deterministic canonical value encoding shared by the interop tools and tests.
///
/// The Python twin lives in `tool/interop/interop_canon.py`; both sides must
/// produce byte-identical canonical JSON strings. See that file for the format
/// and the FNV-1a checksum definition.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';

/// Canonical JSON-compatible tree for one value of [column].
Object? canonicalColumnValue(ParquetColumn column, int index) {
  if (column.isNull(index)) return null;
  if (column is Int32Column) return column.getInt(index);
  if (column is Int64Column) {
    final value = column.value(index);
    return value is BigInt ? value.toInt() : value;
  }
  if (column is BooleanColumn) return column.getBool(index);
  if (column is Float32Column) {
    return <String, Object?>{'f32': _float32Bits(column.values, index)};
  }
  if (column is Float64Column) {
    return <String, Object?>{'f64': _float64Bits(column.values, index)};
  }
  if (column is BinaryColumn) {
    return column.isUtf8
        ? column.getString(index)
        : <String, Object?>{'hex': _hex(column.getBytes(index))};
  }
  if (column is FixedLenColumn) {
    return <String, Object?>{'hex': _hex(column.getBytes(index))};
  }
  if (column is Int96Column) {
    return <String, Object?>{'hex': _hex(column.getBytes(index))};
  }
  if (column is NullColumn) return null;
  if (column is ListColumn) {
    return <Object?>[
      for (var i = column.offsets[index]; i < column.offsets[index + 1]; i++)
        canonicalColumnValue(column.child, i),
    ];
  }
  if (column is MapColumn) {
    final pairs = <List<Object?>>[
      for (var i = column.offsets[index]; i < column.offsets[index + 1]; i++)
        <Object?>[
          canonicalColumnValue(column.keys, i),
          canonicalColumnValue(column.values, i),
        ],
    ];
    pairs.sort((a, b) => canonicalJson(a[0]).compareTo(canonicalJson(b[0])));
    return pairs;
  }
  if (column is StructColumn) {
    final names = column.fields.keys.toList()..sort();
    return <String, Object?>{
      for (final name in names)
        name: canonicalColumnValue(column.fields[name]!, index),
    };
  }
  throw UnsupportedError(
    'no canonical encoding for ${column.runtimeType} (${column.name})',
  );
}

/// Canonical JSON text for a tree produced by [canonicalColumnValue].
String canonicalJson(Object? tree) => jsonEncode(tree);

/// FNV-1a 64 checksum over `canonicalJson(value) + "\n"` for every row.
String columnChecksum(ParquetColumn column) {
  final prime = BigInt.parse('100000001b3', radix: 16);
  final mask = (BigInt.one << 64) - BigInt.one;
  var hash = BigInt.parse('cbf29ce484222325', radix: 16);
  for (var i = 0; i < column.length; i++) {
    final bytes = utf8.encode(
      '${canonicalJson(canonicalColumnValue(column, i))}\n',
    );
    for (final byte in bytes) {
      hash = ((hash ^ BigInt.from(byte)) * prime) & mask;
    }
  }
  return hash.toRadixString(16).padLeft(16, '0');
}

/// Canonical JSON strings for a deterministic sample of rows.
List<String> sampleValues(ParquetColumn column) {
  final rows = sampleRows(column.length);
  return <String>[
    for (final row in rows) canonicalJson(canonicalColumnValue(column, row)),
  ];
}

/// First and last three row indices (deduplicated for short columns).
List<int> sampleRows(int length) {
  final rows = <int>{
    for (var i = 0; i < 3 && i < length; i++) i,
    for (var i = length - 3 < 0 ? 0 : length - 3; i < length; i++) i,
  }.toList()..sort();
  return rows;
}

/// Canonical JSON for a decoded column's row values (used by the writer tool).
List<String> canonicalColumnRows(ParquetColumn column) => <String>[
  for (var i = 0; i < column.length; i++)
    canonicalJson(canonicalColumnValue(column, i)),
];

String _hex(Uint8List bytes) {
  final buffer = StringBuffer();
  for (final byte in bytes) {
    buffer.write(byte.toRadixString(16).padLeft(2, '0'));
  }
  return buffer.toString();
}

String _float32Bits(Float32List values, int index) {
  final data = ByteData.sublistView(values);
  return data
      .getUint32(index * 4, Endian.little)
      .toRadixString(16)
      .padLeft(8, '0');
}

String _float64Bits(Float64List values, int index) {
  final data = ByteData.sublistView(values);
  final low = data.getUint32(index * 8, Endian.little);
  final high = data.getUint32(index * 8 + 4, Endian.little);
  // Format the two 32-bit halves separately: int.toUnsigned(64) keeps the
  // sign bit on the VM, which would print a leading "-".
  return high.toRadixString(16).padLeft(8, '0') +
      low.toRadixString(16).padLeft(8, '0');
}
