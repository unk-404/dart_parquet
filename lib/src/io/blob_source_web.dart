/// Web implementation of the `Blob`/`File` source via blit's Blob backend.
///
/// Only compiled for JavaScript targets; `blob_source.dart` selects it
/// conditionally.
library;

import 'dart:js_interop';

import 'blit_source.dart';
import 'source.dart';

/// Wraps the JavaScript `Blob`/`File` [blob] as a [ParquetSource].
///
/// Uses blit's read-only `BytesFile` Blob backend (blit >= 0.1.6): small blobs
/// are cached once, larger ones are served through `Blob.slice()`.
Future<ParquetSource> openBlobSource(Object blob) async =>
    BlitSource.openBlob(blob as JSObject);
