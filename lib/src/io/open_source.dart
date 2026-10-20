/// Opens a `ParquetSource` from a path or URI with platform capability
/// detection.
///
/// Supported locations:
///
///  * `opfs://dir/file.parquet` — blit's Web OPFS backend (async only);
///  * `idb://dir/file.parquet` — blit's Web IndexedDB backend (async only);
///  * `file:///C:/dir/file.parquet` — native filesystem URI;
///  * anything else — a native filesystem path (VM only).
///
/// HTTP(S) is intentionally not built in: use `HttpRangeSource` with your own
/// client. Unknown schemes and web file paths fail with a clear error instead of
/// a confusing filesystem exception.
library;

import '../util/errors.dart';
import 'blit_source.dart';
import 'file_source.dart';
import 'source.dart';

const String _opfsPrefix = 'opfs://';
const String _idbPrefix = 'idb://';

/// Whether this platform has direct filesystem paths (`dart:io`).
bool get nativeFileSourcesSupported => fileSourcesSupported;

/// Opens [location] asynchronously.
///
/// Files, OPFS and HTTP behave as described in the library documentation.
Future<ParquetSource> openSource(String location) async {
  if (location.startsWith(_opfsPrefix)) {
    return openBlitSource(location.substring(_opfsPrefix.length));
  }
  if (location.startsWith(_idbPrefix)) {
    return openIdbBlitSource(location.substring(_idbPrefix.length));
  }
  if (location.startsWith('http://') || location.startsWith('https://')) {
    throw ParquetUnsupportedException(
      'HTTP(S) locations are not opened directly; construct HttpRangeSource '
      'with a range-capable client for $location',
    );
  }
  if (location.startsWith('file://')) {
    return openFileSource(Uri.parse(location).toFilePath());
  }
  return openFileSource(location);
}

/// Opens [location] synchronously.
///
/// OPFS and HTTP are async-only and throw [ParquetUnsupportedException];
/// `file://` URIs and plain paths return a synchronous filesystem source.
SyncParquetSource openSourceSync(String location) {
  if (location.startsWith(_opfsPrefix)) {
    throw ParquetUnsupportedException(
      'OPFS locations are asynchronous only; use openSource for $location',
    );
  }
  if (location.startsWith(_idbPrefix)) {
    throw ParquetUnsupportedException(
      'IndexedDB locations are asynchronous only; use openSource for $location',
    );
  }
  if (location.startsWith('http://') || location.startsWith('https://')) {
    throw ParquetUnsupportedException(
      'HTTP(S) locations are asynchronous only; use HttpRangeSource for '
      '$location',
    );
  }
  if (location.startsWith('file://')) {
    return openFileSourceSync(Uri.parse(location).toFilePath());
  }
  return openFileSourceSync(location);
}
