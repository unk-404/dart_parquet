/// Web implementation of [writeParquetTableToFile] using blit's OPFS backend.
library;

import 'package:blit/blit.dart' show openBytesFile;

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
  final file = await openBytesFile(path, create: create, readOnly: false);
  try {
    var offset = 0;
    while (offset < bytes.length) {
      final written = await file.writeAt(offset, bytes, start: offset);
      if (written <= 0) {
        throw StateError(
          'OPFS writeAt made no progress at offset $offset of '
          '${bytes.length}',
        );
      }
      offset += written;
    }
    await file.truncate(bytes.length);
    await file.flush();
  } finally {
    await file.close();
  }
}
