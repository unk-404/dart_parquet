/// Optional HTTP Range source.
///
/// The package deliberately depends on no HTTP client: this adapter takes a
/// length provider and a range fetcher, so any client can drive it:
///
/// ```dart
/// final source = HttpRangeSource(
///   lengthProvider: () async {
///     final response = await client.head(url);
///     return int.parse(response.headers.value('content-length')!);
///   },
///   fetchRange: (offset, length) async {
///     final response = await client.get(
///       url,
///       headers: {'range': 'bytes=$offset-${offset + length - 1}'},
///     );
///     return Bytes.fromUint8List(response.bodyBytes);
///   },
/// );
/// ```
///
/// For real use prefer a client that enforces TLS, retries and `206 Partial
/// Content` handling. This class only validates that each fetch returned the
/// requested number of bytes.
library;

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import 'source.dart';

/// Returns the total size of the remote object (one HTTP `HEAD`, for example).
typedef HttpLengthProvider = Future<int> Function();

/// Returns exactly `length` bytes starting at `offset` (one HTTP Range request).
typedef HttpRangeFetcher = Future<Bytes> Function(int offset, int length);

/// Asynchronous [ParquetSource] over HTTP Range requests.
///
/// [length] is fetched lazily once and cached. Reads are not coalesced or
/// cached; wrap the fetcher if you need that.
final class HttpRangeSource implements ParquetSource {
  HttpRangeSource({required this.lengthProvider, required this.fetchRange});

  /// Provides the remote object's total length; called at most once.
  final HttpLengthProvider lengthProvider;

  /// Fetches `[offset, offset + length)` from the remote object.
  final HttpRangeFetcher fetchRange;

  int? _cachedLength;
  bool _closed = false;

  void _checkOpen() {
    if (_closed) throw StateError('HttpRangeSource is closed');
  }

  @override
  Future<int> get length async {
    _checkOpen();
    final cached = _cachedLength;
    if (cached != null) return cached;
    final value = await lengthProvider();
    if (value < 0) {
      throw ParquetFormatException('HTTP source returned a negative length');
    }
    _cachedLength = value;
    return value;
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

    final data = await fetchRange(offset, length);
    if (data.length != length) {
      throw ParquetEofException(
        'HTTP range $offset+$length returned ${data.length} bytes',
      );
    }
    return data;
  }

  @override
  Future<void> close() async {
    _closed = true;
  }
}
