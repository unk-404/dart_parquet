/// Native implementation of [writeParquetTableToFile].
library;

import 'dart:io';

import '../table/table.dart';
import 'writer.dart';

/// Writes [table] to [path] as a single-row-group Parquet file.
///
/// Uses `dart:io`'s `File.writeAsBytes(..., flush: true)` and truncates any
/// previous contents.
///
/// When [create] is `false`, [path] must already exist; otherwise a
/// [FileSystemException] is thrown. With the default `create: true` the file is
/// created or replaced.
Future<void> writeParquetTableToFile(
  ParquetTable table,
  String path, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
  bool create = true,
}) async {
  final bytes = writeParquetTable(table, options: options);
  final file = File(path);
  if (!create && !await file.exists()) {
    throw FileSystemException(
      'Parquet output file does not exist (create: false)',
      path,
    );
  }
  await file.writeAsBytes(bytes, flush: true);
}
