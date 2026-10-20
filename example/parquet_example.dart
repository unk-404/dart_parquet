import 'dart:io';

import 'package:parquet/parquet.dart';

/// Reads a Parquet file and prints its schema, metadata and first row.
///
/// Usage: `dart run example/parquet_example.dart path/to/file.parquet`
void main(List<String> args) {
  final path = args.isEmpty ? 'test/fixtures/plain_snappy.parquet' : args.first;
  final reader = ParquetReader.fromBytes(File(path).readAsBytesSync());
  stdout.writeln('file: $path');
  stdout.writeln('rows: ${reader.numRows}, row groups: ${reader.numRowGroups}');
  stdout.writeln('created by: ${reader.createdBy ?? '(unknown)'}');
  stdout.writeln('schema:');
  for (final column in reader.schema.columns) {
    stdout.writeln(
      '  ${column.dotPath}: ${column.physicalType.name}'
      ' logical=${column.logicalType?.kind.name ?? '-'}'
      ' def=${column.maxDefLevel} rep=${column.maxRepLevel}',
    );
  }
  final table = reader.readTableSync();
  if (table.numRows > 0) {
    stdout.writeln('first row: ${table.row(0).toMap()}');
  }
}
