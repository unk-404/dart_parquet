/// Public entry point for the Parquet I/O adapters.
///
/// Every export here is web-safe: native filesystem access lives behind
/// `file_source.dart`'s conditional export, and Blob support behind
/// `blob_source.dart`'s.
///
/// ```dart
/// import 'package:parquet/src/io/io.dart';
///
/// final source = await openSource('data.parquet'); // or opfs://... on web
/// final file = File('data.parquet');               // VM only
/// final source2 = openSource(file.uri.toString());
/// ```
library;

export 'blit_source.dart';
export 'blob_source.dart';
export 'file_source.dart';
export 'http_range_example.dart';
export 'open_source.dart';
export 'source.dart';
