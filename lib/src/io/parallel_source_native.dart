/// Native parallel random-access source over `blit`'s isolate pool.
///
/// [ParallelBytesFileSource] dispatches independent reads across a pool of
/// worker isolates (one file handle each). blit 0.2 moved the isolate pool
/// behind its internal native I/O API; this adapter imports that engine and
/// keeps the Parquet source contract.
///
/// ## Limitations
///
/// * **Native only.** The web stub throws [UnsupportedError]; use
///   [BlitSource] there.
/// * **Keep it open.** Worker startup costs a few milliseconds; open one source
///   per reader rather than per row group.
/// * **Read-only by default.**
library;

import 'package:blit/blit.dart' show Bytes;
// ignore: implementation_imports
import 'package:blit/src/io/native/isolate_pool.dart' as blit_pool;

import '../util/errors.dart';
import 'source.dart';

/// Asynchronous [ParquetSource] backed by `blit`'s parallel file pool.
final class ParallelBytesFileSource
    implements ParquetSource, BatchedParquetSource {
  ParallelBytesFileSource(this.file);

  /// Opens [path] and spawns the worker pool.
  static Future<ParallelBytesFileSource> open(
    String path, {
    bool create = false,
    bool readOnly = true,
    int? workers,
    bool useIocp = false,
    bool direct = false,
    int mmapReadCacheLimit = 1 << 30,
    int maxQueue = 0,
  }) async => ParallelBytesFileSource(
    await blit_pool.ParallelBytesFile.open(
      path,
      create: create,
      readOnly: readOnly,
      maxWorkers: workers,
      useIocp: useIocp,
      direct: direct,
      mmapReadCacheLimit: mmapReadCacheLimit,
      maxQueue: maxQueue,
    ),
  );

  /// The underlying worker-pool file handle.
  final blit_pool.ParallelBytesFile file;

  /// Worker count selected at open time.
  int get workers => file.maxWorkers;

  /// Whether the source has been closed.
  bool get isClosed => file.isClosed;

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

    final data = await file.read(offset, length);
    if (data.length != length) {
      throw ParquetEofException(
        'short parallel read at $offset: got ${data.length} of $length bytes',
      );
    }
    return Bytes.fromUint8List(data);
  }

  /// Reads one [length]-byte block at each offset in [offsets].
  @override
  Future<List<Bytes>> readMany(List<int> offsets, int length) async {
    if (length < 0) throw RangeError.value(length, 'length');
    final fileLength = await file.length;
    for (final offset in offsets) {
      if (offset < 0) throw RangeError.value(offset, 'offset');
      RangeError.checkValidRange(offset, offset + length, fileLength);
    }
    if (offsets.isEmpty) return const <Bytes>[];
    if (length == 0) {
      return List<Bytes>.generate(offsets.length, (_) => Bytes.empty);
    }

    final buffers = await file.readMany(offsets, length);
    return [for (final buffer in buffers) Bytes.fromUint8List(buffer)];
  }

  @override
  Future<void> close() async {
    if (!file.isClosed) await file.close();
  }

  @override
  String toString() =>
      'ParallelBytesFileSource(${file.path}, workers: $workers)';
}

/// Opens [path] with a `blit` isolate-pool reader.
Future<ParquetSource> openParallelSource(
  String path, {
  bool create = false,
  bool readOnly = true,
  int? workers,
  bool useIocp = false,
  bool direct = false,
  int mmapReadCacheLimit = 1 << 30,
  int maxQueue = 0,
}) => ParallelBytesFileSource.open(
  path,
  create: create,
  readOnly: readOnly,
  workers: workers,
  useIocp: useIocp,
  direct: direct,
  mmapReadCacheLimit: mmapReadCacheLimit,
  maxQueue: maxQueue,
);
