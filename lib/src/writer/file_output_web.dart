/// Web implementation of [writeParquetTableToFile] using blit's OPFS backend.
library;

import 'package:blit/blit.dart' show FileMode, FileOptions, blit;

import '../table/table.dart';
import 'writer.dart';

/// Writes [table] to an OPFS logical file at [path].
///
/// The file is opened with `create: create, readOnly: false`, written at offset
/// 0, truncated to the encoded length, flushed and closed (the handle is owned
/// by this function).
///
/// When [create] is `false`, [path] must already exist; otherwise blit throws a
/// web file-system `NotFoundError`.
Future<void> writeParquetTableToFile(
  ParquetTable table,
  String path, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
  bool create = true,
}) async {
  final bytes = writeParquetTable(table, options: options);
  final file = await blit.open(
    path,
    options: FileOptions(mode: FileMode.readWrite, create: create),
  );
  try {
    await file.writeAt(0, bytes);
    await file.truncate(bytes.length);
    await file.flush();
  } finally {
    await file.close();
  }
}
