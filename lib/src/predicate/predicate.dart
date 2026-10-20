/// Predicate pushdown over Parquet row-group statistics.
///
/// The v0.1.2 predicate API is intentionally conservative: a returned `true`
/// means the row group *may* contain matching rows; `false` means it can be
/// skipped without reading any pages. Missing/unsupported statistics never
/// produce false negatives.
library;

import 'dart:convert';
import 'dart:typed_data';

import '../metadata/metadata.dart';
import '../table/column.dart';
import '../table/table.dart';
import '../util/errors.dart';
import '../util/int64.dart';

/// Comparison used by [ColumnPredicate].
enum FilterOperator { eq, notEq, lt, ltEq, gt, gtEq, isNull, isNotNull, inSet }

/// A row-group pruning predicate.
abstract class ParquetPredicate {
  const ParquetPredicate();

  /// Whether [rowGroup] may contain a matching row.
  bool mayMatch(RowGroupMetaData rowGroup);

  /// Row-level evaluation used by [ParquetTable.filterRows].
  bool matchesRow(ParquetRow row);
}

/// Predicate on one dotted column path.
final class ColumnPredicate extends ParquetPredicate {
  ColumnPredicate(String path, this.operator, [this.value])
    : path = List<String>.unmodifiable(path.split('.'));

  final List<String> path;
  final FilterOperator operator;
  final Object? value;

  @override
  bool mayMatch(RowGroupMetaData rowGroup) {
    final chunk = rowGroup.columnByPath(path);
    if (chunk == null) return true;
    final stats = chunk.statistics;
    if (stats == null) return true;
    final nullCount = stats.nullCount;
    final numValues = chunk.numValues;
    switch (operator) {
      case FilterOperator.isNull:
        return nullCount == null || nullCount > 0;
      case FilterOperator.isNotNull:
        return nullCount == null || nullCount < numValues;
      default:
        if (nullCount != null && nullCount >= numValues) return false;
    }
    final min = _decode(chunk, stats.minValue ?? stats.min);
    final max = _decode(chunk, stats.maxValue ?? stats.max);
    if (min == null || max == null) return true;
    final target = _coerce(value, min);
    if (target == null) return true;
    final cmpMin = _compareValues(min, target);
    final cmpMax = _compareValues(max, target);
    if (cmpMin == null || cmpMax == null) return true;
    return switch (operator) {
      FilterOperator.eq => cmpMin <= 0 && cmpMax >= 0,
      FilterOperator.notEq => !(cmpMin == 0 && cmpMax == 0),
      FilterOperator.lt => cmpMin < 0,
      FilterOperator.ltEq => cmpMin <= 0,
      FilterOperator.gt => cmpMax > 0,
      FilterOperator.gtEq => cmpMax >= 0,
      FilterOperator.inSet => _inRange(min, max),
      FilterOperator.isNull || FilterOperator.isNotNull => true,
    };
  }

  bool _inRange(Object min, Object max) {
    final values = value;
    if (values is! Iterable) return true;
    for (final item in values) {
      final target = _coerce(item, min);
      if (target != null) {
        final cmpMin = _compareValues(min, target);
        final cmpMax = _compareValues(max, target);
        if (cmpMin != null && cmpMax != null && cmpMin <= 0 && cmpMax >= 0) {
          return true;
        }
      }
    }
    return false;
  }

  @override
  bool matchesRow(ParquetRow row) {
    final column = row.table.column(path.join('.'));
    if (column == null) return true;
    final actual = column.value(row.index);
    return _matchValue(actual);
  }

  bool _matchValue(Object? actual) {
    switch (operator) {
      case FilterOperator.isNull:
        return actual == null;
      case FilterOperator.isNotNull:
        return actual != null;
      default:
        if (actual == null) return false;
    }
    if (operator == FilterOperator.inSet) {
      final values = value;
      if (values is! Iterable) return false;
      return values.any((item) => _equal(actual, item));
    }
    final target = value;
    final cmp = _compareValues(actual, target);
    if (cmp == null) return true;
    return switch (operator) {
      FilterOperator.eq => cmp == 0,
      FilterOperator.notEq => cmp != 0,
      FilterOperator.lt => cmp < 0,
      FilterOperator.ltEq => cmp <= 0,
      FilterOperator.gt => cmp > 0,
      FilterOperator.gtEq => cmp >= 0,
      FilterOperator.inSet => false,
      FilterOperator.isNull || FilterOperator.isNotNull => true,
    };
  }
}

/// Logical AND.
final class AndPredicate extends ParquetPredicate {
  const AndPredicate(this.children);
  final List<ParquetPredicate> children;

  @override
  bool mayMatch(RowGroupMetaData rowGroup) =>
      children.every((child) => child.mayMatch(rowGroup));

  @override
  bool matchesRow(ParquetRow row) =>
      children.every((child) => child.matchesRow(row));
}

/// Logical OR.
final class OrPredicate extends ParquetPredicate {
  const OrPredicate(this.children);
  final List<ParquetPredicate> children;

  @override
  bool mayMatch(RowGroupMetaData rowGroup) =>
      children.any((child) => child.mayMatch(rowGroup));

  @override
  bool matchesRow(ParquetRow row) =>
      children.any((child) => child.matchesRow(row));
}

/// Logical NOT. Unsatisfiable when the child is proven impossible.
final class NotPredicate extends ParquetPredicate {
  const NotPredicate(this.child);
  final ParquetPredicate child;

  @override
  bool mayMatch(RowGroupMetaData rowGroup) => true;

  @override
  bool matchesRow(ParquetRow row) => !child.matchesRow(row);
}

Object? _decode(ColumnChunkMetaData chunk, Uint8List? raw) {
  if (raw == null) return null;
  final descriptor = chunk.descriptor;
  if (descriptor == null) return null;
  final data = ByteData.sublistView(raw);
  try {
    switch (descriptor.physicalType) {
      case PhysicalType.boolean:
        return raw.isNotEmpty && raw[0] != 0;
      case PhysicalType.int32:
        return raw.length >= 4 ? data.getInt32(0, Endian.little) : null;
      case PhysicalType.int64:
        if (raw.length < 8) return null;
        final lo = data.getUint32(0, Endian.little);
        final hi = data.getUint32(4, Endian.little);
        return _Int64Value(lo, hi);
      case PhysicalType.float:
        return raw.length >= 4 ? data.getFloat32(0, Endian.little) : null;
      case PhysicalType.double:
        return raw.length >= 8 ? data.getFloat64(0, Endian.little) : null;
      case PhysicalType.byteArray:
        return raw;
      case PhysicalType.fixedLenByteArray:
        return raw;
      case PhysicalType.int96:
        return null;
    }
  } on RangeError {
    return null;
  }
}

Object? _coerce(Object? value, Object min) {
  if (value is int && min is _Int64Value) return _Int64Value.fromInt(value);
  return value;
}

int? _compareValues(Object? a, Object? b) {
  if (a == null || b == null) return null;
  if (a is _Int64Value) {
    if (b is _Int64Value) return a.compareTo(b);
    if (b is int) return a.compareTo(_Int64Value.fromInt(b));
  }
  if (b is _Int64Value && a is int) {
    return _Int64Value.fromInt(a).compareTo(b);
  }
  if (a is BigInt && b is BigInt) return a.compareTo(b);
  if (a is BigInt && b is int) return a.compareTo(BigInt.from(b));
  if (a is int && b is BigInt) return BigInt.from(a).compareTo(b);
  if (a is num && b is num) return a.compareTo(b);
  if (a is String && b is String) return a.compareTo(b);
  if (a is bool && b is bool) {
    if (a == b) return 0;
    return a ? 1 : -1;
  }
  if (a is Uint8List) {
    final bytes = b is String ? utf8.encode(b) : (b is List<int> ? b : null);
    if (bytes == null) return null;
    return _bytesCompare(a, bytes);
  }
  return null;
}

bool _equal(Object? a, Object? b) => _compareValues(a, b) == 0;

int _bytesCompare(List<int> a, List<int> b) {
  final n = a.length < b.length ? a.length : b.length;
  for (var i = 0; i < n; i++) {
    final d = a[i] - b[i];
    if (d != 0) return d;
  }
  return a.length.compareTo(b.length);
}

/// Row-level filtering for flat tables.
extension ParquetTablePredicate on ParquetTable {
  /// Returns a table containing only rows matching [predicate].
  ///
  /// Nested columns are not filtered in v0.1.2; use the columnar API plus a
  /// row mask if you need nested row selection.
  ParquetTable filterRows(ParquetPredicate predicate) {
    final rows = <int>[
      for (var i = 0; i < numRows; i++)
        if (predicate.matchesRow(ParquetRow(this, i))) i,
    ];
    return ParquetTable(
      schema: schema,
      columns: [for (final column in columns) _take(column, rows)],
      numRows: rows.length,
    );
  }
}

ParquetColumn _take(ParquetColumn column, List<int> rows) {
  final validity = Validity(rows.length);
  for (var i = 0; i < rows.length; i++) {
    if (column.isNull(rows[i])) validity.setNull(i);
  }
  if (column is Int32Column) {
    return Int32Column(
      name: column.name,
      values: Int32List.fromList([for (final row in rows) column.values[row]]),
      validity: validity,
    );
  }
  if (column is Int64Column) {
    final values = Int64Buffer(rows.length);
    for (var i = 0; i < rows.length; i++) {
      final limbs = column.values.getLimbs(rows[i]);
      values.setLimbs(i, limbs.$1, limbs.$2);
    }
    return Int64Column(name: column.name, values: values, validity: validity);
  }
  if (column is Float32Column) {
    return Float32Column(
      name: column.name,
      values: Float32List.fromList([
        for (final row in rows) column.values[row],
      ]),
      validity: validity,
    );
  }
  if (column is Float64Column) {
    return Float64Column(
      name: column.name,
      values: Float64List.fromList([
        for (final row in rows) column.values[row],
      ]),
      validity: validity,
    );
  }
  if (column is BooleanColumn) {
    final bits = Uint8List((rows.length + 7) >> 3);
    for (var i = 0; i < rows.length; i++) {
      if (column.getBool(rows[i])) bits[i >> 3] |= 1 << (i & 7);
    }
    return BooleanColumn(name: column.name, bits: bits, validity: validity);
  }
  if (column is BinaryColumn) {
    final data = <int>[];
    final offsets = Uint32List(rows.length + 1);
    for (var i = 0; i < rows.length; i++) {
      final row = rows[i];
      final start = column.offsets[row];
      final end = column.offsets[row + 1];
      for (var j = start; j < end; j++) {
        data.add(column.data[j]);
      }
      offsets[i + 1] = data.length;
    }
    return BinaryColumn(
      name: column.name,
      data: Uint8List.fromList(data),
      offsets: offsets,
      validity: validity,
      isUtf8: column.isUtf8,
    );
  }
  if (column is FixedLenColumn) {
    final data = <int>[];
    for (final row in rows) {
      for (var j = 0; j < column.width; j++) {
        data.add(column.data[row * column.width + j]);
      }
    }
    return FixedLenColumn(
      name: column.name,
      data: Uint8List.fromList(data),
      width: column.width,
      validity: validity,
    );
  }
  if (column is Int96Column) {
    final data = <int>[];
    for (final row in rows) {
      for (var j = 0; j < 12; j++) {
        data.add(column.data[row * 12 + j]);
      }
    }
    return Int96Column(
      name: column.name,
      data: Uint8List.fromList(data),
      validity: validity,
    );
  }
  if (column is NullColumn) {
    return NullColumn(name: column.name, length: rows.length);
  }
  throw ParquetUnsupportedException(
    'row filtering of ${column.runtimeType} is not supported in v0.1.2',
  );
}

final class _Int64Value implements Comparable<_Int64Value> {
  const _Int64Value(this.lo, this.hi);
  factory _Int64Value.fromInt(int value) {
    final big = BigInt.from(value);
    final unsigned = big < BigInt.zero ? big + (BigInt.one << 64) : big;
    return _Int64Value(
      (unsigned & BigInt.from(0xFFFFFFFF)).toInt(),
      ((unsigned >> 32) & BigInt.from(0xFFFFFFFF)).toInt(),
    );
  }

  final int lo;
  final int hi;

  bool get isNegative => (hi & 0x80000000) != 0;

  BigInt get big {
    final unsigned =
        BigInt.from(hi) * BigInt.from(0x100000000) + BigInt.from(lo);
    return isNegative ? unsigned - (BigInt.one << 64) : unsigned;
  }

  @override
  int compareTo(_Int64Value other) => big.compareTo(other.big);
}
