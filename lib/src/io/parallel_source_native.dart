/// Native parallel random-access source over `blit`'s isolate pool.
///
/// ## When to use it
///
/// [ParallelBytesFileSource] dispatches independent reads across a pool of
/// worker isolates (one file handle each). That turns single-threaded,
/// latency-bound page reads into a queue-driven workload: useful for cold or
/// wide scans, where many column chunks are read before they are decoded.
///
/// ## Limitations
///
/// * **Native only.** It spawns isolates and uses `dart:io`/`dart:ffi`; the web
///   stub throws [UnsupportedError] (OPFS sync handles are exclusive per file,
///   so parallel workers would fight over one lock; use `BlitSource` instead).
/// * **Keep it open.** Worker startup costs a few milliseconds; open one source
///   per reader rather than per row group.
/// * **Read-only by default.** `create: false, readOnly: true` unless overridden.
/// * **No ordering between writes and reads.** This adapter is read-only; the
///   underlying blit pool documents write/read ordering rules.
library;

import 'package:blit/blit.dart' show Bytes;
import 'package:blit/bytes_ffi.dart' as blit_ffi;

import '../util/errors.dart';
import 'source.dart';

/// Asynchronous [ParquetSource] backed by `blit`'s [ParallelBytesFile].
final class ParallelBytesFileSource
    implements ParquetSource, BatchedParquetSource {
  ParallelBytesFileSource(this.file);

  /// Opens [path] and spawns the worker pool.
  ///
  /// [workers] overrides blit's CPU-derived pool size; [mmapReadCacheLimit] is
  /// the largest file each worker maps read-only instead of issuing syscalls
  /// (`0` disables the cache); [maxQueue] bounds queued + in-flight jobs (`0`
  /// leaves the queue unbounded). [useIocp] selects the Windows
  /// IOCP batch engine for very large files (mutually exclusive with [direct]).
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
    await blit_ffi.ParallelBytesFile.open(
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
  final blit_ffi.ParallelBytesFile file;

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
  ///
  /// Returns exactly one buffer per offset, in the same order. Ranges are
  /// validated against the file length first, so EOF is never silently
  /// zero-filled.
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
///
/// See [ParallelBytesFileSource.open] for the tuning parameters.
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
