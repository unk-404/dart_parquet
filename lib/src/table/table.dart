/// Columnar table and row view exposed by the reader.
library;

import 'column.dart';
import '../schema/schema.dart';

/// One row group (or a concatenation of row groups) of decoded columns.
final class ParquetTable {
  ParquetTable({
    required this.schema,
    required this.columns,
    required this.numRows,
  }) : _byName = {for (final c in columns) c.name: c};

  final ParquetSchema schema;
  final List<ParquetColumn> columns;
  final int numRows;
  final Map<String, ParquetColumn> _byName;

  ParquetColumn? column(String name) => _byName[name];

  ParquetColumn operator [](String name) {
    final c = _byName[name];
    if (c == null) {
      throw ArgumentError.value(name, 'name', 'no such column');
    }
    return c;
  }

  bool hasColumn(String name) => _byName.containsKey(name);

  ParquetRow row(int index) {
    if (index < 0 || index >= numRows) {
      throw RangeError.range(index, 0, numRows - 1, 'index');
    }
    return ParquetRow(this, index);
  }

  Iterable<ParquetRow> get rows sync* {
    for (var i = 0; i < numRows; i++) {
      yield ParquetRow(this, i);
    }
  }

  Map<String, ParquetColumn> get columnMap => Map.unmodifiable(_byName);
}

/// One row of a [ParquetTable].
final class ParquetRow {
  ParquetRow(this.table, this.index);

  final ParquetTable table;
  final int index;

  Object? operator [](String name) => table[name].value(index);

  Map<String, Object?> toMap() {
    final out = <String, Object?>{};
    for (final c in table.columns) {
      out[c.name] = c.value(index);
    }
    return out;
  }

  @override
  String toString() => 'ParquetRow($index, ${toMap()})';
}
