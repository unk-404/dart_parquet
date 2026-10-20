/// Native stubs for blit's Web-only Blob and IndexedDB backends.
library;

import 'package:blit/blit.dart' show BytesFile;

Future<BytesFile> openBlobBytesFileWeb(
  Object blob, {
  int cacheLimit = 8 << 20,
}) async {
  throw UnsupportedError('blit Blob bytes files are only available on the web');
}

Future<BytesFile> openIdbBytesFileWeb(
  String path, {
  bool create = false,
  bool readOnly = true,
  int chunkSize = 64 * 1024,
  String? databaseName,
  String? storeName,
}) async {
  throw UnsupportedError(
    'blit IndexedDB bytes files are only available on the web',
  );
}
