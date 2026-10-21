/// Native read-only whole-file (or windowed) mmap source.
///
/// Uses blit's native mmap engine directly. blit 0.2 removed the old public
/// `bytes_ffi.dart` export, so this adapter imports the stable native mmap
/// interface from `src/io/native/mmap.dart`.
library;

import 'package:blit/blit.dart' show Bytes;
// ignore: implementation_imports
import 'package:blit/src/io/native/mmap.dart' as blit_mmap;

import 'source.dart';

/// Read-only [SyncParquetSource] over a blit file mapping.
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

  /// Total length of the underlying file.
  final int fileLength;

  final blit_mmap.MmapFile _file;
  final blit_mmap.MappedRegion? _region;
  final Bytes _bytes;
  final int _windowLength;
  bool _closed = false;

  /// Maps a read-only view of [path].
  static MmapSource open(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) {
    if (offset < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    final file = blit_mmap.openMmapFileSync(
      path,
      create: false,
      readOnly: true,
    );
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
  static Future<MmapSource> openAsync(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) async => open(path, offset: offset, length: length, prefetch: prefetch);

  /// Whether a mapping exists.
  bool get isMapped => _region != null;

  /// Whether the mapping is writable; always false.
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

  /// Hints the OS to page in the mapped window.
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

/// Opens a read-only mmap source synchronously.
MmapSource openMmapSourceSync(
  String path, {
  int offset = 0,
  int? length,
  bool prefetch = false,
}) => MmapSource.open(path, offset: offset, length: length, prefetch: prefetch);

/// Opens a read-only mmap source asynchronously.
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
