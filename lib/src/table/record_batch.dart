/// Columnar batch API for incremental reads.
library;

import '../schema/schema.dart';
import 'column.dart';
import 'table.dart';

/// One column vector inside a [RecordBatch].
///
/// This is an alias of [ParquetColumn]; the batch API keeps the same zero-copy
/// vectors while giving scanners a batch-oriented name.
typedef ColumnVector = ParquetColumn;

/// A bounded set of rows with column vectors.
///
/// A [RecordBatch] is the unit produced by [ParquetReader.readBatch] and
/// [ParquetReader.readBatches]. It is immutable and can be converted to a
/// [ParquetTable] without copying.
final class RecordBatch {
  /// Creates a batch from [columns].
  RecordBatch({
    required this.schema,
    required this.columns,
    required this.numRows,
  }) : _byName = {for (final column in columns) column.name: column};

  /// Creates a batch from an already decoded [table].
  factory RecordBatch.fromTable(ParquetTable table) => RecordBatch(
    schema: table.schema,
    columns: table.columns,
    numRows: table.numRows,
  );

  /// Schema of the projected columns.
  final ParquetSchema schema;

  /// Column vectors, in schema order.
  final List<ParquetColumn> columns;

  /// Number of rows in this batch.
  final int numRows;

  final Map<String, ParquetColumn> _byName;

  /// Looks up a column by name.
  ParquetColumn? column(String name) => _byName[name];

  /// Looks up a column by name or throws.
  ParquetColumn operator [](String name) {
    final column = _byName[name];
    if (column == null) {
      throw ArgumentError.value(name, 'name', 'no such column');
    }
    return column;
  }

  /// Whether [name] is present.
  bool hasColumn(String name) => _byName.containsKey(name);

  /// Converts this batch to the table API without copying vectors.
  ParquetTable toTable() =>
      ParquetTable(schema: schema, columns: columns, numRows: numRows);
}
