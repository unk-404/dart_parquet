/// Change-data-capture helpers for Parquet tables.
///
/// Parquet has no dedicated CDC physical type; Delta/Iceberg/Spark CDC writers
/// represent changes with reserved columns such as `_change_type`,
/// `_commit_version` and `_commit_timestamp`. This library treats those
/// conventions as first-class metadata without removing them from the table.
library;

import '../table/table.dart';

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
}
