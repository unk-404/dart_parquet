/// Web stub for the native parallel source.
///
/// Browsers cannot fan random access across worker isolates: OPFS sync access
/// handles are exclusive per file. Every entry point throws a clear
/// [UnsupportedError]; use `BlitSource` (OPFS/IndexedDB) instead.
library;

import 'package:blit/blit.dart' show Bytes;

import 'source.dart';

/// Unavailable on the web.
final class ParallelBytesFileSource
    implements ParquetSource, BatchedParquetSource {
  ParallelBytesFileSource._();

  static Future<ParallelBytesFileSource> open(
    String path, {
    bool create = false,
    bool readOnly = true,
    int? workers,
    bool useIocp = false,
    bool direct = false,
    int mmapReadCacheLimit = 1 << 30,
    int maxQueue = 0,
  }) async => throw UnsupportedError(
    'ParallelBytesFileSource is native-only (isolate pool); the web has no '
    'parallel file handles. Use BlitSource (OPFS/IndexedDB) instead.',
  );

  int get workers => 0;

  bool get isClosed => true;

  @override
  Future<int> get length =>
      throw UnsupportedError('ParallelBytesFileSource is native-only');

  @override
  Future<Bytes> read(int offset, int length) async =>
      throw UnsupportedError('ParallelBytesFileSource is native-only');

  @override
  Future<List<Bytes>> readMany(List<int> offsets, int length) async =>
      throw UnsupportedError('ParallelBytesFileSource is native-only');

  @override
  Future<void> close() async {}
}

/// Unavailable on the web.
Future<ParquetSource> openParallelSource(
  String path, {
  bool create = false,
  bool readOnly = true,
  int? workers,
  bool useIocp = false,
  bool direct = false,
  int mmapReadCacheLimit = 1 << 30,
  int maxQueue = 0,
}) async => throw UnsupportedError(
  'ParallelBytesFileSource is native-only (isolate pool); the web has no '
  'parallel file handles. Use BlitSource (OPFS/IndexedDB) instead.',
);
