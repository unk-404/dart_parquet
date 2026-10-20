/// Native stub for `openBlobSource`.
///
/// Browsers hand `Blob`/`File` objects to the page; native Dart has no such
/// type. `blob_source.dart` selects this file on the VM so the shared import
/// graph stays web-safe.
library;

import 'source.dart';

/// Not available outside the web; pass a path (or use a callback source).
Future<ParquetSource> openBlobSource(Object blob) async {
  throw UnsupportedError(
    'Blob/File sources are only available on the web; '
    'on the VM use openFileSource(path) or BlobSliceSource instead.',
  );
}
