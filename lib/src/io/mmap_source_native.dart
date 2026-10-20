/// Native read-only whole-file (or windowed) mmap source.
///
/// ## Limitations
///
/// * **Native only.** Windows, Linux, macOS and other `dart:ffi` targets with
///   the `blit` mmap backend. Browsers have no mmap; the web stub throws
///   [UnsupportedError] (use `BlitSource`/OPFS or a Blob source instead).
/// * **Read-only.** The file is opened with `create: false, readOnly: true` and
///   the mapping is `writable: false`. Writing through the view is a
///   use-after-unmap-class bug / access violation; do not do it.
/// * **Lifetime.** A [Bytes] returned by [read] aliases the mapping. It is valid
///   only until [close]; keeping it afterwards is a use-after-unmap. Copy with
///   `Bytes.toUint8List()` if you need to outlive the source.
/// * **Address space.** The mapping is virtual and does not eagerly read the
///   file, but it does reserve `length` bytes of address space. Very large files
///   should use a window ([MmapSource.open] with `offset`/`length`) or the
///   parallel source instead.
/// * **File stability.** Do not truncate, rewrite or delete the file while it is
///   mapped. On Windows a mapping can keep the file locked against deletion
///   until [MmapSource.close]; on POSIX the mapping keeps the old contents
///   alive. Parquet files are immutable, so this matches normal usage.
/// * The mmap engine is synchronous; do not call [read] on a UI isolate.
library;

import 'package:blit/blit.dart' show Bytes;
import 'package:blit/bytes_ffi.dart' as blit_ffi;

import 'source.dart';

/// Read-only [SyncParquetSource] over a `blit` file mapping.
///
/// Reads are zero-copy slices of the mapped memory: no syscall and no buffer
/// copy per page. The whole file is mapped by default; pass `offset`/`length`
/// to [open] to map only a window (useful for huge files).
final class MmapSource implements SyncParquetSource {
  MmapSource._(
    this.path,
    this._file,
    this._region,
    this._bytes,
    this.fileOffset,
    this.fileLength,
    this._windowLength,
  );

  /// The path this source was opened from.
  final String path;

  /// File offset of the mapped window.
  final int fileOffset;

  /// Total length of the underlying file (not just the mapped window).
  final int fileLength;

  final blit_ffi.MmapFile _file;
  final blit_ffi.MappedRegion? _region;
  final Bytes _bytes;
  final int _windowLength;
  bool _closed = false;

  /// Maps a read-only view of [path].
  ///
  /// [offset] and [length] select a window of the file; the default maps the
  /// whole file. [prefetch] hints the OS to page the mapping in eagerly
  /// (`PrefetchVirtualMemory` on Windows, `madvise(MADV_WILLNEED)` elsewhere).
  ///
  /// Throws a [FileSystemException](https://api.dart.dev/dart-io/FileSystemException-class.html)
  /// (or a blit native exception) when the file cannot be opened or mapped, and
  /// [RangeError] when the window lies outside the file.
  static MmapSource open(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) {
    if (offset < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    final file = blit_ffi.openMmapFileSync(path, create: false, readOnly: true);
    try {
      final total = file.length;
      if (offset > total) {
        throw RangeError.range(offset, 0, total, 'offset');
      }
      final window = length ?? (total - offset);
      if (window < 0 || offset + window > total) {
        throw RangeError.range(window, 0, total - offset, 'length');
      }
      final region = window == 0
          ? null
          : file.map(offset: offset, length: window, writable: false);
      final bytes = region == null
          ? Bytes.empty
          : Bytes.fromUint8List(region.bytes);
      final source = MmapSource._(
        path,
        file,
        region,
        bytes,
        offset,
        total,
        window,
      );
      if (prefetch) region?.prefetch();
      return source;
    } catch (_) {
      file.close();
      rethrow;
    }
  }

  /// Asynchronous convenience wrapper around [open].
  ///
  /// The mapping itself is synchronous; this only matches the async source
  /// factories.
  static Future<MmapSource> openAsync(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) async => open(path, offset: offset, length: length, prefetch: prefetch);

  /// Whether a mapping exists (false for an empty file or a zero-length window).
  bool get isMapped => _region != null;

  /// Whether the mapping is writable; always false for this source.
  bool get isWritable => _region?.isWritable ?? false;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  void _checkOpen() {
    if (_closed) throw StateError('MmapSource($path) is closed');
  }

  /// Length of the mapped window.
  @override
  int get length {
    _checkOpen();
    return _windowLength;
  }

  /// Returns a zero-copy slice of the mapping.
  @override
  Bytes read(int offset, int length) {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    RangeError.checkValidRange(offset, offset + length, _windowLength);
    if (length == 0) return Bytes.empty;
    return _bytes.slice(offset, offset + length);
  }

  /// Hints the OS to page in the mapped window (or a sub-range of it).
  void prefetch({int offset = 0, int? length}) {
    _checkOpen();
    final region = _region;
    if (region == null) return;
    RangeError.checkValidRange(
      offset,
      offset + (length ?? _windowLength - offset),
      _windowLength,
    );
    region.prefetch(offset: offset, length: length);
  }

  /// Closes the mapping and the file handle. Idempotent.
  ///
  /// Any [Bytes] previously returned by [read] becomes invalid.
  @override
  void close() {
    if (_closed) return;
    _closed = true;
    _region?.close();
    _file.close();
  }

  @override
  String toString() =>
      'MmapSource($path, offset: $fileOffset, length: $_windowLength)';
}

/// Opens a read-only mmap source, synchronously.
MmapSource openMmapSourceSync(
  String path, {
  int offset = 0,
  int? length,
  bool prefetch = false,
}) => MmapSource.open(path, offset: offset, length: length, prefetch: prefetch);

/// Opens a read-only mmap source asynchronously (the mapping is synchronous).
Future<MmapSource> openMmapSource(
  String path, {
  int offset = 0,
  int? length,
  bool prefetch = false,
}) => MmapSource.openAsync(
  path,
  offset: offset,
  length: length,
  prefetch: prefetch,
);
