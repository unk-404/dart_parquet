/// Web stub for `file_source.dart`.
///
/// Browsers have no plain filesystem paths. OPFS is reached through
/// `BlitSource` (`opfs://...` via `openSourceFromLocation`), and user-selected
/// files through `openBlobSource`.
library;

import 'source.dart';

/// Whether this platform has direct filesystem paths. False on the web.
const bool fileSourcesSupported = false;

/// Not available on the web: there are no filesystem paths.
Future<ParquetSource> openFileSource(String path) async {
  throw UnsupportedError(
    'Opening the path "$path" is not supported on the web; '
    'use BlitSource (OPFS) or openBlobSource(File) instead.',
  );
}

/// Not available on the web: there are no filesystem paths.
SyncParquetSource openFileSourceSync(String path) {
  throw UnsupportedError(
    'Opening the path "$path" synchronously is not supported on the web; '
    'use BlitSource (OPFS) instead.',
  );
}
