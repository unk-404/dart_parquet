/// VM worker execution via [Isolate.run].
library;

import 'dart:async';
import 'dart:io';
import 'dart:isolate';

/// Runs [computation] on a separate isolate and returns its result.
Future<T> runParquetIsolate<T>(FutureOr<T> Function() computation) =>
    Isolate.run(computation);

/// Number of worker isolates to use for `auto` parallelism.
int get defaultParquetParallelism {
  final count = Platform.numberOfProcessors;
  if (count < 2) return 1;
  return count > 8 ? 8 : count;
}
