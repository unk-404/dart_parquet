/// Browser implementation of the blit Web Blob and IndexedDB adapters.
library;

import 'dart:js_interop';

import 'package:blit/blit.dart' show BlitFile;
import 'package:blit/blit_web.dart' as bw;
// ignore: implementation_imports
import 'package:blit/src/io/web/web.dart' as web;

Future<BlitFile> openBlobBytesFileWeb(
  Object blob, {
  int cacheLimit = 8 << 20,
}) => bw.openBlob(blob as JSObject, cacheLimit: cacheLimit);

Future<BlitFile> openIdbBytesFileWeb(
  String path, {
  bool create = false,
  bool readOnly = true,
  int chunkSize = 64 * 1024,
  String? databaseName,
  String? storeName,
}) => bw.openIndexedDb(
  path,
  create: create,
  readOnly: readOnly,
  chunkSize: chunkSize,
  databaseName: databaseName ?? web.kIdbDatabaseName,
  storeName: storeName ?? web.kIdbStoreName,
);
