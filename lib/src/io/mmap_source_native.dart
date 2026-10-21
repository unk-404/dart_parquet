/// Native read-only whole-file (or windowed) random-access source.
///
/// This source keeps a synchronous `dart:io` handle open. The historical blit
/// mmap backend was removed in blit 0.2; the public [MmapSource] contract is
/// preserved (read-only, synchronous, windowed) while reads go through the
/// platform file handle.
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import 'source.dart';

/// Read-only [SyncParquetSource] over a native file window.
final class MmapSource implements SyncParquetSource {
  MmapSource._(
    this.path,
    this._file,
    this.fileOffset,
    this.fileLength,
    this._windowLength,
  );

  /// The path this source was opened from.
  final String path;

  /// File offset of the window.
  final int fileOffset;

  /// Total length of the underlying file.
  final int fileLength;

  final RandomAccessFile _file;
  final int _windowLength;
  bool _closed = false;

  /// Opens a read-only view of [path].
  ///
  /// [offset] and [length] select a window of the file; the default covers the
  /// whole file. [prefetch] is accepted for API compatibility.
  static MmapSource open(
    String path, {
    int offset = 0,
    int? length,
    bool prefetch = false,
  }) {
    if (offset < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    final file = File(path).openSync(mode: FileMode.read);
    try {
      final total = file.lengthSync();
      if (offset > total) {
        throw RangeError.range(offset, 0, total, 'offset');
      }
      final window = length ?? (total - offset);
      if (window < 0 || offset + window > total) {
        throw RangeError.range(window, 0, total - offset, 'length');
      }
      return MmapSource._(path, file, offset, total, window);
    } catch (_) {
      file.closeSync();
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

  /// Whether a non-empty window exists.
  bool get isMapped => _windowLength > 0;

  /// This source never maps writable memory.
  bool get isWritable => false;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  void _checkOpen() {
    if (_closed) throw StateError('MmapSource($path) is closed');
  }

  /// Length of the window.
  @override
  int get length {
    _checkOpen();
    return _windowLength;
  }

  /// Returns a copy of the window slice.
  @override
  Bytes read(int offset, int length) {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    RangeError.checkValidRange(offset, offset + length, _windowLength);
    if (length == 0) return Bytes.empty;
    final buffer = Uint8List(length);
    _file.setPositionSync(fileOffset + offset);
    var filled = 0;
    while (filled < length) {
      final n = _file.readIntoSync(buffer, filled, length);
      if (n <= 0) {
        throw StateError(
          'short read from $path at ${fileOffset + offset + filled}',
        );
      }
      filled += n;
    }
    return Bytes.fromUint8List(buffer);
  }

  /// Accepted for API compatibility.
  void prefetch({int offset = 0, int? length}) {
    _checkOpen();
  }

  /// Closes the file handle. Idempotent.
  @override
  void close() {
    if (_closed) return;
    _closed = true;
    _file.closeSync();
  }

  @override
  String toString() =>
      'MmapSource($path, offset: $fileOffset, length: $_windowLength)';
}

/// Opens a read-only source synchronously.
MmapSource openMmapSourceSync(
  String path, {
  int offset = 0,
  int? length,
  bool prefetch = false,
}) => MmapSource.open(path, offset: offset, length: length, prefetch: prefetch);

/// Opens a read-only source asynchronously.
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
