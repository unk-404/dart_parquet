/// Pluggable byte sources and sinks.
///
/// The reader never assumes a file: a source only has to expose its length and
/// random-access reads. Native files, blit `BytesFile`, browser OPFS, Blob
/// slices, HTTP Range requests and in-memory buffers are all adapters.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

/// Asynchronous random-access source. `read` returns exactly [length] bytes or
/// throws if the range is out of bounds / short.
abstract interface class ParquetSource {
  Future<int> get length;

  Future<Bytes> read(int offset, int length);

  Future<void> close();
}

/// Synchronous random-access source (memory, mmap, sync file). Enables the
/// synchronous reader API.
abstract interface class SyncParquetSource {
  int get length;

  Bytes read(int offset, int length);

  void close();
}

/// A synchronous source that can also be used through the async API.
class SyncSourceAdapter implements ParquetSource {
  SyncSourceAdapter(this._source);

  final SyncParquetSource _source;

  @override
  Future<int> get length async => _source.length;

  @override
  Future<Bytes> read(int offset, int length) async =>
      _source.read(offset, length);

  @override
  Future<void> close() async => _source.close();
}

/// In-memory source. Zero-copy reads over the given [Bytes].
final class MemorySource implements SyncParquetSource {
  MemorySource(Bytes bytes) : _bytes = bytes;

  MemorySource.fromUint8List(Uint8List bytes)
    : _bytes = Bytes.fromUint8List(bytes);

  final Bytes _bytes;

  @override
  int get length => _bytes.length;

  @override
  Bytes read(int offset, int length) {
    if (offset < 0 || length < 0 || offset + length > _bytes.length) {
      throw RangeError.range(
        offset,
        0,
        _bytes.length,
        'offset',
        'invalid range $offset+$length over ${_bytes.length} bytes',
      );
    }
    return _bytes.slice(offset, offset + length);
  }

  @override
  void close() {}
}
