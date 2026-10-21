/// Change-data-capture helpers for Parquet tables.
///
/// Parquet has no dedicated CDC physical type; Delta/Iceberg/Spark CDC writers
/// represent changes with reserved columns such as `_change_type`,
/// `_commit_version` and `_commit_timestamp`. This library treats those
/// conventions as first-class metadata without removing them from the table.
library;

import 'dart:typed_data';

import '../table/column.dart';
import '../table/table.dart';
import '../util/errors.dart';
import '../util/int64.dart';

/// Row-level change kind in a CDC table.
enum ChangeType {
  /// Row was inserted.
  insert,

  /// Row is the before-image of an update.
  updatePreimage,

  /// Row is the after-image of an update.
  updatePostimage,

  /// Row was deleted.
  delete,
}

/// A [ParquetTable] annotated with CDC column names.
final class CdcTable {
  /// Wraps [table], using the conventional CDC column names by default.
  CdcTable(
    this.table, {
    this.changeTypeColumn = '_change_type',
    this.commitVersionColumn = '_commit_version',
    this.commitTimestampColumn = '_commit_timestamp',
  });

  /// Underlying decoded table, including the CDC metadata columns.
  final ParquetTable table;

  /// Name of the change-kind column.
  final String changeTypeColumn;

  /// Name of the commit-version column.
  final String commitVersionColumn;

  /// Name of the commit-timestamp column.
  final String commitTimestampColumn;

  /// Rows in the table.
  int get numRows => table.numRows;

  /// Whether the change-kind column exists.
  bool get hasChangeType => table.hasColumn(changeTypeColumn);

  /// Change kind at [row], or `null` when the column/value is absent/unknown.
  ChangeType? changeType(int row) {
    if (!hasChangeType) return null;
    return parseChangeType(table[changeTypeColumn].value(row));
  }

  /// Commit version at [row], or `null` when absent/non-integer.
  int? commitVersion(int row) {
    if (!table.hasColumn(commitVersionColumn)) return null;
    final value = table[commitVersionColumn].value(row);
    if (value is int) return value;
    if (value is BigInt) return value.toInt();
    return null;
  }

  /// Commit timestamp at [row] as the stored integer, when present.
  int? commitTimestamp(int row) {
    if (!table.hasColumn(commitTimestampColumn)) return null;
    final value = table[commitTimestampColumn].value(row);
    if (value is int) return value;
    if (value is BigInt) return value.toInt();
    return null;
  }

  /// Applies changes in input order and returns the current rows.
  ///
  /// [keyColumns] identify a row. A later insert/post-image replaces the
  /// earlier row; a delete removes it. The output preserves every column,
  /// including the CDC metadata columns, and keeps the original row order.
  ParquetTable materialize({required List<String> keyColumns}) {
    if (keyColumns.isEmpty) {
      throw ArgumentError.value(keyColumns, 'keyColumns', 'must not be empty');
    }
    final keep = <String, int>{};
    for (var row = 0; row < numRows; row++) {
      final key = <String>[
        for (final name in keyColumns) table[name].value(row).toString(),
      ].join('\u0000');
      switch (changeType(row)) {
        case ChangeType.delete:
          keep.remove(key);
        case ChangeType.insert:
        case ChangeType.updatePreimage:
        case ChangeType.updatePostimage:
        case null:
          keep[key] = row;
      }
    }
    final rows = keep.values.toList()..sort();
    return ParquetTable(
      schema: table.schema,
      columns: [for (final column in table.columns) _slice(column, rows)],
      numRows: rows.length,
    );
  }

  /// Parses a CDC marker used by Delta/Iceberg/Spark.
  static ChangeType? parseChangeType(Object? value) {
    if (value is! String) return null;
    return switch (value.trim().toLowerCase()) {
      'insert' || 'append' || 'i' => ChangeType.insert,
      'update_preimage' ||
      'update_before' ||
      'u_before' => ChangeType.updatePreimage,
      'update_postimage' ||
      'update_after' ||
      'u_after' => ChangeType.updatePostimage,
      'delete' || 'remove' || 'd' => ChangeType.delete,
      _ => null,
    };
  }

  ParquetColumn _slice(ParquetColumn column, List<int> rows) {
    final validity = Validity(rows.length);
    if (column is Int32Column) {
      final values = Int32List(rows.length);
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else {
          values[i] = column.getInt(row);
        }
      }
      return Int32Column(name: column.name, values: values, validity: validity);
    }
    if (column is Int64Column) {
      final values = Int64Buffer(rows.length);
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else {
          final limbs = column.values.getLimbs(row);
          values.setLimbs(i, limbs.$1, limbs.$2);
        }
      }
      return Int64Column(name: column.name, values: values, validity: validity);
    }
    if (column is Float32Column) {
      final values = Float32List(rows.length);
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else {
          values[i] = column.values[row];
        }
      }
      return Float32Column(
        name: column.name,
        values: values,
        validity: validity,
      );
    }
    if (column is Float64Column) {
      final values = Float64List(rows.length);
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else {
          values[i] = column.values[row];
        }
      }
      return Float64Column(
        name: column.name,
        values: values,
        validity: validity,
      );
    }
    if (column is BooleanColumn) {
      final bits = Uint8List((rows.length + 7) >> 3);
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else if (column.getBool(row)) {
          bits[i >> 3] |= 1 << (i & 7);
        }
      }
      return BooleanColumn(name: column.name, bits: bits, validity: validity);
    }
    if (column is BinaryColumn) {
      var total = 0;
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (!column.validity.isNull(row)) {
          total += column.offsets[row + 1] - column.offsets[row];
        }
      }
      final data = Uint8List(total);
      final offsets = Uint32List(rows.length + 1);
      var target = 0;
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else {
          final start = column.offsets[row];
          final length = column.offsets[row + 1] - start;
          data.setRange(target, target + length, column.data, start);
          target += length;
        }
        offsets[i + 1] = target;
      }
      return BinaryColumn(
        name: column.name,
        data: data,
        offsets: offsets,
        validity: validity,
        isUtf8: column.isUtf8,
      );
    }
    if (column is FixedLenColumn) {
      final data = Uint8List(rows.length * column.width);
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (column.validity.isNull(row)) {
          validity.setNull(i);
        } else {
          data.setRange(
            i * column.width,
            (i + 1) * column.width,
            column.data,
            row * column.width,
          );
        }
      }
      return FixedLenColumn(
        name: column.name,
        data: data,
        width: column.width,
        validity: validity,
      );
    }
    throw ParquetUnsupportedException(
      'CDC materialization does not support ${column.runtimeType}',
    );
  }
}
