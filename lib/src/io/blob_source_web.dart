/// Web implementation of the `Blob`/`File` source.
///
/// Only compiled for JavaScript targets; `blob_source.dart` selects it
/// conditionally.
library;

import 'dart:js_interop';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import 'source.dart';

/// Minimal view of the JavaScript `Blob` interface (a `File` is a `Blob`).
extension type _JSBlob._(JSObject _) implements JSObject {
  external int get size;

  external _JSBlob slice(int start, int end);

  external JSPromise<JSArrayBuffer> arrayBuffer();
}

/// Asynchronous [ParquetSource] over a JavaScript `Blob` or `File`.
///
/// `read` slices the blob and awaits `arrayBuffer()`; the browser owns the
/// returned memory, so the resulting [Bytes] needs no copy.
final class WebBlobSource implements ParquetSource {
  WebBlobSource._(this._blob);

  /// Wraps a JavaScript `Blob`/`File` handle.
  factory WebBlobSource.fromJs(JSObject blob) =>
      WebBlobSource._(_JSBlob._(blob));

  final _JSBlob _blob;
  bool _closed = false;

  void _checkOpen() {
    if (_closed) throw StateError('WebBlobSource is closed');
  }

  @override
  Future<int> get length async {
    _checkOpen();
    return _blob.size;
  }

  @override
  Future<Bytes> read(int offset, int length) async {
    _checkOpen();
    if (offset < 0 || length < 0) {
      throw RangeError.range(offset, 0, null, 'offset');
    }
    RangeError.checkValidRange(offset, offset + length, _blob.size);
    if (length == 0) return Bytes.empty;

    final buffer = await _blob
        .slice(offset, offset + length)
        .arrayBuffer()
        .toDart;
    final data = buffer.toDart.asUint8List();
    if (data.length != length) {
      throw ParquetEofException(
        'blob slice $offset+$length returned ${data.length} bytes',
      );
    }
    return Bytes.fromUint8List(data);
  }

  @override
  Future<void> close() async {
    _closed = true;
  }
}

/// Wraps the JavaScript `Blob`/`File` [blob] as a [ParquetSource].
///
/// On the web every Dart value is represented by a JavaScript value, so the
/// cast to [JSObject] is effectively a no-op; the external `Blob` members then
/// perform the real check when they are used.
Future<ParquetSource> openBlobSource(Object blob) async =>
    WebBlobSource.fromJs(blob as JSObject);
