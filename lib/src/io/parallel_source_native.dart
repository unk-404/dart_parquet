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

import 'dart:io' show Platform;
import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes, FileMode, FileOptions;
import 'package:blit/blit_io.dart' show AsyncFile, ReadBuf;

import '../util/errors.dart';
import 'source.dart';

/// Asynchronous [ParquetSource] backed by `blit`'s [ParallelBytesFile].
final class ParallelBytesFileSource
    implements ParquetSource, BatchedParquetSource {
  ParallelBytesFileSource(this._files);

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
  }) async {
    if (workers != null && workers <= 0) {
      throw RangeError.range(workers, 1, null, 'workers');
    }
    if (mmapReadCacheLimit < 0) {
      throw RangeError.value(mmapReadCacheLimit, 'mmapReadCacheLimit');
    }
    if (maxQueue < 0) throw RangeError.value(maxQueue, 'maxQueue');
    final count = workers ?? (Platform.numberOfProcessors.clamp(1, 8));
    final options = FileOptions(
      mode: readOnly ? FileMode.read : FileMode.readWrite,
      create: create,
    );
    final files = <AsyncFile>[
      for (var i = 0; i < count; i++)
        await AsyncFile.open(path, options: options),
    ];
    return ParallelBytesFileSource(files);
  }

  final List<AsyncFile> _files;
  var _next = 0;
  var _closed = false;

  /// Worker count selected at open time.
  int get workers => _files.length;

  /// Whether the source has been closed.
  bool get isClosed => _closed;

  void _checkOpen() {
    if (_closed) throw StateError('ParallelBytesFileSource is closed');
  }

  @override
  Future<int> get length async {
    _checkOpen();
    return _files.first.length;
  }

  @override
  Future<Bytes> read(int offset, int length) async {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    final fileLength = await this.length;
    RangeError.checkValidRange(offset, offset + length, fileLength);
    if (length == 0) return Bytes.empty;

    final buffer = Uint8List(length);
    final n = await _nextFile().readAt(offset, ReadBuf(buffer));
    if (n != length) {
      throw ParquetEofException(
        'short parallel read at $offset: got $n of $length bytes',
      );
    }
    return Bytes.fromUint8List(buffer);
  }

  /// Reads one [length]-byte block at each offset in [offsets].
  ///
  /// Returns exactly one buffer per offset, in the same order. Ranges are
  /// validated against the file length first, so EOF is never silently
  /// zero-filled.
  @override
  Future<List<Bytes>> readMany(List<int> offsets, int length) async {
    _checkOpen();
    if (length < 0) throw RangeError.value(length, 'length');
    final fileLength = await this.length;
    for (final offset in offsets) {
      if (offset < 0) throw RangeError.value(offset, 'offset');
      RangeError.checkValidRange(offset, offset + length, fileLength);
    }
    if (offsets.isEmpty) return const <Bytes>[];
    if (length == 0) {
      return List<Bytes>.generate(offsets.length, (_) => Bytes.empty);
    }

    final out = <Bytes>[];
    for (final offset in offsets) {
      final buffer = Uint8List(length);
      final n = await _nextFile().readAt(offset, ReadBuf(buffer));
      if (n != length) {
        throw ParquetEofException(
          'short parallel read at $offset: got $n of $length bytes',
        );
      }
      out.add(Bytes.fromUint8List(buffer));
    }
    return out;
  }

  AsyncFile _nextFile() => _files[_next++ % _files.length];

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    for (final file in _files) {
      await file.close();
    }
  }

  @override
  String toString() =>
      'ParallelBytesFileSource(${_files.first.path}, workers: $workers)';
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
