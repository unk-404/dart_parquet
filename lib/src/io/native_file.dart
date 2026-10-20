/// Native (`dart:io`) random-access file sources.
///
/// This library is only selected through `file_source.dart`'s conditional
/// export; the shared I/O import graph never sees `dart:io`.
library;

import 'dart:async';
import 'dart:io';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import 'source.dart';

/// Whether this platform has direct filesystem paths. True on the VM.
const bool fileSourcesSupported = true;

/// Asynchronous [ParquetSource] backed by a native [`RandomAccessFile`].
///
/// Reads are serialized through an internal queue because `RandomAccessFile`
/// has one file position: concurrent `read` calls are safe and correct, they
/// simply execute one at a time.
final class NativeFileSource implements ParquetSource {
  NativeFileSource._(this.path, this._file);

  /// Opens [path] for reading. Throws [FileSystemException] if it is missing or
  /// cannot be opened.
  static Future<NativeFileSource> open(String path) async {
    final file = await File(path).open(mode: FileMode.read);
    return NativeFileSource._(path, file);
  }

  /// The path this source was opened from.
  final String path;

  final RandomAccessFile _file;
  Future<void> _queue = Future<void>.value();
  bool _closed = false;

  void _checkOpen() {
    if (_closed) throw StateError('NativeFileSource($path) is closed');
  }

  Future<T> _serialize<T>(Future<T> Function() action) {
    final completer = Completer<T>();
    _queue = _queue.then((_) async {
      try {
        completer.complete(await action());
      } catch (e, stackTrace) {
        completer.completeError(e, stackTrace);
      }
    });
    return completer.future;
  }

  @override
  Future<int> get length async {
    _checkOpen();
    return _file.length();
  }

  @override
  Future<Bytes> read(int offset, int length) async {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    return _serialize(() async {
      final fileLength = await _file.length();
      RangeError.checkValidRange(offset, offset + length, fileLength);
      if (length == 0) return Bytes.empty;
      await _file.setPosition(offset);
      final data = await _file.read(length);
      if (data.length != length) {
        throw ParquetEofException(
          'short read from $path at $offset: '
          'got ${data.length} of $length bytes',
        );
      }
      return Bytes.fromUint8List(data);
    });
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    await _queue;
    await _file.close();
  }
}

/// Synchronous [SyncParquetSource] backed by a native [`RandomAccessFile`].
final class SyncNativeFileSource implements SyncParquetSource {
  SyncNativeFileSource._(this.path, this._file, this._length);

  /// Opens [path] for reading, synchronously.
  static SyncNativeFileSource open(String path) {
    final file = File(path).openSync(mode: FileMode.read);
    return SyncNativeFileSource._(path, file, file.lengthSync());
  }

  /// The path this source was opened from.
  final String path;

  final RandomAccessFile _file;
  final int _length;
  bool _closed = false;

  void _checkOpen() {
    if (_closed) throw StateError('SyncNativeFileSource($path) is closed');
  }

  @override
  int get length {
    _checkOpen();
    return _length;
  }

  @override
  Bytes read(int offset, int length) {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    RangeError.checkValidRange(offset, offset + length, _length);
    if (length == 0) return Bytes.empty;
    _file.setPositionSync(offset);
    final data = _file.readSync(length);
    if (data.length != length) {
      throw ParquetEofException(
        'short read from $path at $offset: '
        'got ${data.length} of $length bytes',
      );
    }
    return Bytes.fromUint8List(data);
  }

  @override
  void close() {
    if (_closed) return;
    _closed = true;
    _file.closeSync();
  }
}

/// Opens [path] as an asynchronous native file source.
Future<ParquetSource> openFileSource(String path) =>
    NativeFileSource.open(path);

/// Opens [path] as a synchronous native file source.
SyncParquetSource openFileSourceSync(String path) =>
    SyncNativeFileSource.open(path);
