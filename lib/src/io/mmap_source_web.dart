/// Web stub for the native mmap source.
///
/// Browsers have no memory-mapped files. Every entry point throws a clear
/// [UnsupportedError]; use `BlitSource` (OPFS/IndexedDB) or a Blob source
/// instead.
library;

import 'package:blit/blit.dart' show Bytes;

import 'source.dart';

/// Unavailable on the web.
final class MmapSource implements SyncParquetSource {
  MmapSource._();

  static MmapSource open(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) => throw UnsupportedError(
    'MmapSource is native-only (dart:ffi); the web has no mmap. '
    'Use BlitSource (OPFS/IndexedDB) or openBlobSource instead.',
  );

  static Future<MmapSource> openAsync(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) async => throw UnsupportedError(
    'MmapSource is native-only (dart:ffi); the web has no mmap. '
    'Use BlitSource (OPFS/IndexedDB) or openBlobSource instead.',
  );

  String get path => throw UnsupportedError('MmapSource is native-only');

  int get fileOffset => throw UnsupportedError('MmapSource is native-only');

  int get fileLength => throw UnsupportedError('MmapSource is native-only');

  bool get isMapped => false;

  bool get isWritable => false;

  bool get isClosed => true;

  @override
  int get length => throw UnsupportedError('MmapSource is native-only');

  @override
  Bytes read(int offset, int length) =>
      throw UnsupportedError('MmapSource is native-only');

  void prefetch({int offset = 0, int? length}) =>
      throw UnsupportedError('MmapSource is native-only');

  @override
  void close() {}
}

/// Unavailable on the web.
MmapSource openMmapSourceSync(
  String path, {
  int offset = 0,
  int? length,
  bool prefetch = false,
}) => throw UnsupportedError(
  'MmapSource is native-only (dart:ffi); the web has no mmap. '
  'Use BlitSource (OPFS/IndexedDB) or openBlobSource instead.',
);

/// Unavailable on the web.
Future<MmapSource> openMmapSource(
  String path, {
  int offset = 0,
  int? length,
  bool prefetch = false,
}) async => throw UnsupportedError(
  'MmapSource is native-only (dart:ffi); the web has no mmap. '
  'Use BlitSource (OPFS/IndexedDB) or openBlobSource instead.',
);
