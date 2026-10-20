/// Browser implementation of the blit Web Blob and IndexedDB adapters.
library;

import 'dart:js_interop';

import 'package:blit/blit.dart' show BytesFile;
// ignore: implementation_imports
import 'package:blit/src/io/web/web.dart' as web;

Future<BytesFile> openBlobBytesFileWeb(
  Object blob, {
  int cacheLimit = 8 << 20,
}) => web.openBlobBytesFile(blob as JSObject, cacheLimit: cacheLimit);

Future<BytesFile> openIdbBytesFileWeb(
  String path, {
  bool create = false,
  bool readOnly = true,
  int chunkSize = 64 * 1024,
  String? databaseName,
  String? storeName,
}) => web.openIdbBytesFile(
  path,
  create: create,
  readOnly: readOnly,
  chunkSize: chunkSize,
  databaseName: databaseName ?? web.kIdbDatabaseName,
  storeName: storeName ?? web.kIdbStoreName,
);
