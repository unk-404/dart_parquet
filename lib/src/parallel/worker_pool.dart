/// Persistent, reusable worker isolates for Parquet computations.
///
/// The VM implementation keeps a fixed pool of worker isolates alive between
/// jobs; the web stub runs work on the current event loop. Import this library,
/// never the native/stub file directly.
library;

export 'worker_pool_stub.dart' if (dart.library.io) 'worker_pool_native.dart';
