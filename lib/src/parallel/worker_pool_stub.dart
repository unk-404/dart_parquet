/// Web implementation: no worker isolates, work runs on the current event loop.
///
/// The API mirrors `worker_pool_native.dart` so reader code can import
/// `worker_pool.dart` on every platform. Browsers are single-threaded for this
/// package (see `isolate_runner_stub.dart`); a `WorkerPool` is therefore a
/// logical pool of one. CPU-bound computations still block the event loop.
library;

import 'dart:async';

/// Web stays on the current event loop, so the default pool is one logical
/// worker.
int get defaultWorkerPoolSize => 1;

/// Thrown when a worker job fails remotely, cannot be sent, or its isolate
/// dies.
///
/// On the web work runs locally, so native errors propagate unchanged; this
/// type remains for API parity and future worker implementations.
final class WorkerPoolException implements Exception {
  WorkerPoolException(
    this.message, {
    this.cause,
    this.remoteStackTrace,
    this.stackTrace,
  });

  /// Human-readable description.
  final String message;

  /// Original send failure, when this exception wraps one.
  final Object? cause;

  /// Remote isolate stack trace, when the worker provided one.
  final String? remoteStackTrace;

  /// Local stack trace at the failure point.
  final StackTrace? stackTrace;

  @override
  String toString() {
    final buffer = StringBuffer('WorkerPoolException: $message');
    if (cause != null) buffer.write(' (cause: $cause)');
    if (remoteStackTrace != null && remoteStackTrace!.isNotEmpty) {
      buffer.write('\nRemote stack:\n$remoteStackTrace');
    }
    return buffer.toString();
  }
}

/// A single-threaded stand-in for the native worker pool.
final class WorkerPool {
  WorkerPool._(this._debugName);

  /// Starts a logical pool. [workers] is accepted for API parity but the web
  /// implementation always executes on the current isolate.
  static Future<WorkerPool> start({
    int? workers,
    String debugName = 'parquet-worker',
  }) async {
    final count = workers ?? defaultWorkerPoolSize;
    if (count < 1) throw RangeError.value(count, 'workers');
    return WorkerPool._(debugName);
  }

  final String _debugName;
  int _pending = 0;
  bool _closing = false;
  bool _closed = false;
  Future<void>? _closeFuture;

  /// Logical worker count (always 1 on the web).
  int get workers => 1;

  /// Idle workers: 1 when no job is running.
  int get idleWorkers => _pending == 0 ? 1 : 0;

  /// Jobs queued or running.
  int get pendingJobs => _pending;

  /// Whether [close] has completed.
  bool get isClosed => _closed;

  /// Whether [close] has been called.
  bool get isClosing => _closing;

  void _checkOpen() {
    if (_closed || _closing) throw StateError('WorkerPool is closed');
  }

  /// Runs [computation] on the current event loop.
  Future<T> run<T>(FutureOr<T> Function() computation) async {
    _checkOpen();
    _pending++;
    try {
      return await computation();
    } finally {
      _pending--;
    }
  }

  /// Runs a [task] with [arguments] on the current event loop.
  Future<T> runTask<T>(
    FutureOr<T> Function(List<Object?> arguments) task, [
    List<Object?> arguments = const <Object?>[],
  ]) => run<T>(() => task(arguments));

  /// Runs every computation, preserving result order.
  Future<List<T>> runAll<T>(Iterable<FutureOr<T> Function()> computations) {
    _checkOpen();
    return Future.wait<T>([
      for (final computation in computations) run(computation),
    ]);
  }

  /// Runs every `(task, arguments)` pair, preserving result order.
  Future<List<T>> runAllTasks<T>(
    Iterable<(FutureOr<T> Function(List<Object?>), List<Object?>)> tasks,
  ) {
    _checkOpen();
    return Future.wait<T>([
      for (final task in tasks) runTask<T>(task.$1, task.$2),
    ]);
  }

  /// Marks the pool closed. Idempotent; work already submitted still runs.
  Future<void> close() {
    final existing = _closeFuture;
    if (existing != null) return existing;
    _closing = true;
    final completer = Completer<void>();
    _closeFuture = completer.future;
    _closed = true;
    completer.complete();
    return completer.future;
  }

  @override
  String toString() =>
      'WorkerPool($_debugName, local: true, pending: $pendingJobs, '
      'closed: $_closed)';
}
