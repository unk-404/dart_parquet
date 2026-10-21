/// File output helpers for the Parquet writer.
///
/// The shared function [writeParquetTableToBytesFile] works with blit's
/// cross-platform [BlitFile]. The path-based [writeParquetTableToFile] is
/// selected at compile time: `dart:io` on the VM and OPFS through blit on the
/// web.
library;

import 'package:blit/blit.dart' show BlitFile;

import '../table/table.dart';
import 'writer.dart';

export 'file_output_native.dart'
    if (dart.library.js_interop) 'file_output_web.dart';

/// Writes [table] to an already-open [file] at offset 0.
///
/// The file is truncated to the encoded length and flushed. The caller owns
/// [file]: it is **not** closed, so it can be reused (for example to write
/// several files into a container format).
Future<void> writeParquetTableToBytesFile(
  ParquetTable table,
  BlitFile file, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
}) async {
  final bytes = writeParquetTable(table, options: options);
  await file.writeAt(0, bytes);
  await file.truncate(bytes.length);
  await file.flush();
}
