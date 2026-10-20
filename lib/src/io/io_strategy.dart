/// I/O strategy selection for path-based opens.
///
/// The reader can be handed an already-created [ParquetSource]; this library is
/// the convenience layer for callers that just have a path. It maps an
/// [IoStrategy] to a concrete source on the current platform:
///
/// | Strategy   | Native VM                                             | Web              |
/// |------------|-------------------------------------------------------|------------------|
/// | `auto`     | `mmap` for files ≤ [autoMmapThreshold], else `parallel` | `batched` (blit) |
/// | `sync`     | `SyncNativeFileSource` (plain `dart:io` reads)         | unsupported      |
/// | `mmap`     | [MmapSource] (zero-copy, read-only)                    | unsupported      |
/// | `batched`  | [ParallelBytesFileSource] with a small worker pool     | `batched` (blit) |
/// | `parallel` | [ParallelBytesFileSource] with blit's CPU-derived pool | `batched` (blit) |
///
/// On the web `auto`/`batched`/`parallel` open a blit [BytesFile]
/// (`opfs://...`, `idb://...`, or an OPFS logical path). [IoStrategy.sync] and
/// [IoStrategy.mmap] throw [ParquetUnsupportedException]: browsers have no
/// synchronous filesystem or mmap.
///
/// These defaults favour correctness and a clear mapping; sizing the pools and
/// threshold is deliberately left to v0.1.3 benchmarking.
library;

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import 'blit_source.dart';
import 'file_source.dart';
import 'mmap_source.dart';
import 'open_source.dart';
import 'parallel_source.dart';
import 'source.dart';

/// Above this size [IoStrategy.auto] picks a worker pool instead of mapping the
/// whole file.
///
/// Mapping is virtual (no eager read), but a whole-file map reserves address
/// space and pins the mapping for the source lifetime. 64 MiB keeps that
/// comfortable for typical embedded/desktop readers.
const int autoMmapThreshold = 64 << 20;

/// Default worker count for [IoStrategy.batched].
const int batchedWorkers = 4;

/// How a path is opened by [openSourceWithStrategy].
enum IoStrategy {
  /// Platform-appropriate default: mmap for small native files, the parallel
  /// pool for large ones, blit `BytesFile` on the web.
  auto,

  /// Synchronous plain-file reads (`SyncNativeFileSource`); async-only on the
  /// reader side, but exposes a [OpenedParquetSource.sync] view.
  sync,

  /// Whole-file (or windowed) read-only mmap; zero-copy sync reads.
  mmap,

  /// Worker-pool reads with a small pool, for batched page fetches.
  batched,

  /// Worker-pool reads with blit's CPU-derived pool, for cold/wide scans.
  parallel,
}

/// Resolves [requested] to the concrete strategy used on this platform.
///
/// Only [IoStrategy.auto] depends on the file/platform; all other values are
/// returned unchanged. [nativeFileSystem] defaults to the current platform and
/// exists for tests and diagnostics.
IoStrategy resolveIoStrategy(
  IoStrategy requested, {
  required int fileLength,
  bool nativeFileSystem = fileSourcesSupported,
}) {
  if (fileLength < 0) {
    throw RangeError.value(fileLength, 'fileLength');
  }
  if (requested != IoStrategy.auto) return requested;
  if (!nativeFileSystem) return IoStrategy.batched;
  return fileLength <= autoMmapThreshold
      ? IoStrategy.mmap
      : IoStrategy.parallel;
}

/// A source opened from a path plus the strategy that produced it.
///
/// Implements [ParquetSource] so it can be handed straight to
/// `ParquetReader.open`. When the concrete source is synchronous ([sync] or
/// [mmap]), [sync] exposes it for the synchronous reader API; otherwise [sync]
/// is `null`.
final class OpenedParquetSource implements ParquetSource {
  OpenedParquetSource._({
    required this.path,
    required this.requested,
    required this.resolved,
    required this.source,
    this.sync,
  });

  /// The path or location that was opened.
  final String path;

  /// The strategy the caller asked for.
  final IoStrategy requested;

  /// The concrete strategy used (see [resolveIoStrategy]).
  final IoStrategy resolved;

  /// The async source. For `sync`/`mmap` these are wrapped in a
  /// [SyncSourceAdapter].
  final ParquetSource source;

  /// The synchronous view, when [resolved] is [IoStrategy.sync] or
  /// [IoStrategy.mmap]; otherwise `null`.
  final SyncParquetSource? sync;

  /// Whether [sync] is available.
  bool get canReadSync => sync != null;

  @override
  Future<int> get length => source.length;

  @override
  Future<Bytes> read(int offset, int length) => source.read(offset, length);

  @override
  Future<void> close() => source.close();

  @override
  String toString() =>
      'OpenedParquetSource($path, requested: ${requested.name}, '
      'resolved: ${resolved.name})';
}

/// Opens [path] using [strategy].
///
/// Accepts a native filesystem path or `file://` URI on the VM, and an OPFS
/// (`opfs://...`), IndexedDB (`idb://...`) or plain OPFS logical path on the
/// web. HTTP(S) is rejected: construct a `HttpRangeSource` with a range-capable
/// client instead.
///
/// The synchronous strategies (`sync`, `mmap`) open the file synchronously
/// inside this async function; avoid calling it on a UI isolate.
Future<OpenedParquetSource> openSourceWithStrategy(
  String path, {
  IoStrategy strategy = IoStrategy.auto,
  int? workers,
  int mmapReadCacheLimit = 1 << 30,
  int maxQueue = 0,
  bool prefetch = false,
}) async {
  if (workers != null && workers < 1) {
    throw RangeError.value(workers, 'workers');
  }
  if (mmapReadCacheLimit < 0) {
    throw RangeError.value(mmapReadCacheLimit, 'mmapReadCacheLimit');
  }
  if (maxQueue < 0) throw RangeError.value(maxQueue, 'maxQueue');

  if (_isHttp(path)) {
    throw ParquetUnsupportedException(
      'HTTP(S) locations are not opened by openSourceWithStrategy; construct '
      'HttpRangeSource with a range-capable client for $path',
    );
  }

  if (!fileSourcesSupported) {
    if (strategy == IoStrategy.sync || strategy == IoStrategy.mmap) {
      throw ParquetUnsupportedException(
        'IoStrategy.${strategy.name} requires native dart:io and is not '
        'available on the web; use auto/batched/parallel (blit BytesFile) '
        'instead',
      );
    }
    final source = await _openWebLocation(path);
    return OpenedParquetSource._(
      path: path,
      requested: strategy,
      resolved: IoStrategy.batched,
      source: source,
    );
  }

  if (path.startsWith('opfs://') || path.startsWith('idb://')) {
    if (strategy == IoStrategy.sync || strategy == IoStrategy.mmap) {
      throw ParquetUnsupportedException(
        'IoStrategy.${strategy.name} requires a native filesystem path; '
        '$path is a blit web/bytes-file location',
      );
    }
    final source = await openSource(path);
    return OpenedParquetSource._(
      path: path,
      requested: strategy,
      resolved: IoStrategy.batched,
      source: source,
    );
  }

  final localPath = path.startsWith('file://')
      ? Uri.parse(path).toFilePath()
      : path;

  final IoStrategy resolved;
  if (strategy == IoStrategy.auto) {
    final probe = openFileSourceSync(localPath);
    final int length;
    try {
      length = probe.length;
    } finally {
      probe.close();
    }
    resolved = resolveIoStrategy(
      IoStrategy.auto,
      fileLength: length,
      nativeFileSystem: true,
    );
  } else {
    resolved = strategy;
  }

  switch (resolved) {
    case IoStrategy.sync:
      final sync = openFileSourceSync(localPath);
      return OpenedParquetSource._(
        path: path,
        requested: strategy,
        resolved: resolved,
        source: SyncSourceAdapter(sync),
        sync: sync,
      );
    case IoStrategy.mmap:
      final mmap = MmapSource.open(localPath, prefetch: prefetch);
      return OpenedParquetSource._(
        path: path,
        requested: strategy,
        resolved: resolved,
        source: SyncSourceAdapter(mmap),
        sync: mmap,
      );
    case IoStrategy.batched:
    case IoStrategy.parallel:
      final pool = await ParallelBytesFileSource.open(
        localPath,
        workers:
            workers ?? (resolved == IoStrategy.batched ? batchedWorkers : null),
        mmapReadCacheLimit: mmapReadCacheLimit,
        maxQueue: maxQueue,
      );
      return OpenedParquetSource._(
        path: path,
        requested: strategy,
        resolved: resolved,
        source: pool,
      );
    case IoStrategy.auto:
      // resolveIoStrategy never returns auto.
      throw StateError('unreachable IoStrategy.auto resolution');
  }
}

bool _isHttp(String location) =>
    location.startsWith('http://') || location.startsWith('https://');

/// Web fallback: blit `BytesFile` (OPFS/IndexedDB) for every async strategy.
Future<ParquetSource> _openWebLocation(String location) {
  const opfs = 'opfs://';
  const idb = 'idb://';
  if (location.startsWith(opfs)) {
    return openBlitSource(location.substring(opfs.length));
  }
  if (location.startsWith(idb)) {
    return openIdbBlitSource(location.substring(idb.length));
  }
  if (location.startsWith('file://')) {
    throw ParquetUnsupportedException(
      'file:// URIs are not available on the web; pass an OPFS path, an '
      'idb:// location, or wrap a Blob with BlitSource.openBlob',
    );
  }
  // A plain path on the web is an OPFS logical path.
  return openBlitSource(location);
}
