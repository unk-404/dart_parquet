/// Callback-backed source for Blob/File-like slices.
///
/// This is the web-safe half of Blob support: the platform adapter only has to
/// know how to produce `length` bytes starting at `offset`. `openBlobSource` in
/// `blob_source.dart` wires it to a real JavaScript `Blob`/`File` on the web;
/// tests and non-browser callers can use it directly.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import 'source.dart';

/// Returns exactly `length` bytes of the underlying object starting at `offset`.
typedef BlobRangeReader = Future<Uint8List> Function(int offset, int length);

/// Asynchronous [ParquetSource] over any object exposing length + ranges.
///
/// The source is read-only; [close] only flips a flag (the caller owns the
/// underlying object).
final class BlobSliceSource implements ParquetSource {
  BlobSliceSource({required int length, required this.readRange})
    : _length = length {
    if (length < 0) {
      throw ArgumentError.value(length, 'length', 'must be non-negative');
    }
  }

  final int _length;

  /// Fetches exactly `length` bytes starting at `offset`.
  final BlobRangeReader readRange;
  bool _closed = false;

  void _checkOpen() {
    if (_closed) throw StateError('BlobSliceSource is closed');
  }

  @override
  Future<int> get length async {
    _checkOpen();
    return _length;
  }

  @override
  Future<Bytes> read(int offset, int length) async {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    RangeError.checkValidRange(offset, offset + length, _length);
    if (length == 0) return Bytes.empty;

    final data = await readRange(offset, length);
    if (data.length != length) {
      throw ParquetEofException(
        'blob range $offset+$length returned ${data.length} bytes',
      );
    }
    return Bytes.fromUint8List(data);
  }

  @override
  Future<void> close() async {
    _closed = true;
  }
}

/// Alias matching the "blob slice reader" name used by callers.
typedef BlobSliceReader = BlobSliceSource;
