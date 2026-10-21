/// `ParquetSource` adapter for blit's [BlitFile].
///
/// `BlitFile` is blit's cross-platform random-access file abstraction: a native
/// file on the VM and a chunked OPFS file on the web. This adapter therefore
/// gives the reader async file access on both platforms without any
/// `dart:io`/`dart:ffi` dependency in the shared graph.
library;

import 'package:blit/blit.dart'
    show BlitFile, Bytes, FileMode, FileOptions, blit;

import '../util/errors.dart';
import 'blit_web.dart';
import 'source.dart';

/// Asynchronous [ParquetSource] over a blit [BlitFile].
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
  }) async => BlitSource(
    await blit.open(
      path,
      options: FileOptions(
        mode: readOnly ? FileMode.read : FileMode.readWrite,
        create: create,
      ),
    ),
  );

  /// Opens a read-only blit [BlitFile] over a JavaScript `Blob`/`File`.
  ///
  /// Requires blit >= 0.1.6; only available on the web. Small blobs are cached
  /// in memory once, larger ones use `Blob.slice()` per read.
  static Future<BlitSource> openBlob(
    Object blob, {
    int cacheLimit = 8 << 20,
  }) async =>
      BlitSource(await openBlobBytesFileWeb(blob, cacheLimit: cacheLimit));

  /// Opens a blit IndexedDB-backed [BlitFile].
  ///
  /// Requires blit >= 0.1.6; only available on the web. IndexedDB is an
  /// explicit fallback for contexts where OPFS sync handles are unavailable.
  /// Readers normally want [create] `false` and [readOnly] `true`.
  static Future<BlitSource> openIndexedDb(
    String path, {
    bool create = false,
    bool readOnly = true,
    int chunkSize = 64 * 1024,
    String? databaseName,
    String? storeName,
  }) async => BlitSource(
    await openIdbBytesFileWeb(
      path,
      create: create,
      readOnly: readOnly,
      chunkSize: chunkSize,
      databaseName: databaseName,
      storeName: storeName,
    ),
  );

  /// The underlying file handle, exposed for callers that need its extras
  /// (e.g. `SyncBytesFile` on native).
  final BlitFile file;

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

    final bytes = await file.readAt(offset, length);
    if (bytes.length != length) {
      throw ParquetEofException(
        'short read from blit file at $offset: '
        'got ${bytes.length} of $length bytes',
      );
    }
    return bytes;
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

/// Opens a web IndexedDB-backed Parquet source (blit >= 0.1.6).
Future<ParquetSource> openIdbBlitSource(
  String path, {
  bool create = false,
  bool readOnly = true,
  int chunkSize = 64 * 1024,
  String? databaseName,
  String? storeName,
}) => BlitSource.openIndexedDb(
  path,
  create: create,
  readOnly: readOnly,
  chunkSize: chunkSize,
  databaseName: databaseName,
  storeName: storeName,
);
