/// `ParquetSource` adapter for blit's [BytesFile].
///
/// `BytesFile` is blit's cross-platform random-access file abstraction: a native
/// file on the VM and a chunked OPFS file on the web. This adapter therefore
/// gives the reader async file access on both platforms without any
/// `dart:io`/`dart:ffi` dependency in the shared graph.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes, BytesFile, openBytesFile;

import '../util/errors.dart';
import 'source.dart';

/// Asynchronous [ParquetSource] over a blit [BytesFile].
final class BlitSource implements ParquetSource {
  BlitSource(this.file);

  /// Opens [path] with blit's platform backend.
  ///
  /// Defaults are tuned for reading Parquet files: the file must already exist
  /// (`create: false`) and writes are rejected (`readOnly: true`).
  static Future<BlitSource> open(
    String path, {
    bool create = false,
    bool readOnly = true,
  }) async =>
      BlitSource(await openBytesFile(path, create: create, readOnly: readOnly));

  /// The underlying file handle, exposed for callers that need its extras
  /// (e.g. `SyncBytesFile` on native).
  final BytesFile file;

  @override
  Future<int> get length => file.length;

  @override
  Future<Bytes> read(int offset, int length) async {
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    final fileLength = await file.length;
    RangeError.checkValidRange(offset, offset + length, fileLength);
    if (length == 0) return Bytes.empty;

    final buffer = Uint8List(length);
    var filled = 0;
    while (filled < length) {
      final read = await file.readAt(offset + filled, buffer, start: filled);
      if (read <= 0) {
        throw ParquetEofException(
          'short read from blit file at $offset: '
          'got $filled of $length bytes',
        );
      }
      filled += read;
    }
    return Bytes.fromUint8List(buffer);
  }

  @override
  Future<void> close() async {
    if (!file.isClosed) await file.close();
  }
}

/// Opens [path] through blit (native file or web OPFS logical path).
Future<ParquetSource> openBlitSource(
  String path, {
  bool create = false,
  bool readOnly = true,
}) => BlitSource.open(path, create: create, readOnly: readOnly);
