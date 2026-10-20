/// Web fallback: no isolates, run on the current event loop.
library;

import 'dart:async';

/// Runs [computation] on the current isolate.
Future<T> runParquetIsolate<T>(FutureOr<T> Function() computation) async =>
    computation();

/// Web is deliberately single-threaded for v0.1.x.
int get defaultParquetParallelism => 1;
