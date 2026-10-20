/// Public entry point for the Parquet I/O adapters.
///
/// Every export here is web-safe: native filesystem access lives behind
/// `file_source.dart`'s conditional export, mmap and the parallel pool behind
/// `mmap_source.dart` / `parallel_source.dart`, and Blob support behind
/// `blob_source.dart`'s. On the web those native-only modules are stubs that
/// throw a clear `UnsupportedError`.
///
/// ```dart
/// import 'package:parquet/src/io/io.dart';
///
/// final source = await openSource('data.parquet'); // or opfs://... on web
/// final mmap = openMmapSourceSync('data.parquet'); // VM only
/// final opened = await openSourceWithStrategy('data.parquet');
/// ```
library;

export 'blit_source.dart';
export 'blob_source.dart';
export 'file_source.dart';
export 'http_range_example.dart';
export 'io_strategy.dart';
export 'mmap_source.dart';
export 'open_source.dart';
export 'parallel_source.dart';
export 'source.dart';
