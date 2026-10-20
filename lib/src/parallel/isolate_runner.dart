/// Platform-selected worker execution for parallel decode/encode.
library;

export 'isolate_runner_stub.dart'
    if (dart.library.io) 'isolate_runner_native.dart';
