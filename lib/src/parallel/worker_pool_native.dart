/// VM implementation: a persistent pool of reusable worker isolates.
///
/// Unlike [Isolate.run], which spawns and tears down an isolate per call, a
/// [WorkerPool] keeps its isolates alive and dispatches jobs to idle workers.
/// That removes isolate startup (roughly a millisecond each, plus group setup)
/// from every row-group decode.
///
/// ## Closures vs. the task protocol
///
/// [run] sends a Dart closure to a worker isolate. That works for closures
/// whose captured context is sendable (numbers, strings, typed data, plain
/// model objects), but the VM captures variables by context, not by variable:
/// a closure created in a scope that also captures a receive port, a file, or a
/// test framework object can fail with "Illegal argument in isolate message".
/// When that happens the failure is reported as a [WorkerPoolException].
///
/// For code that must always work, [runTask] sends a top-level/static function
/// plus an explicit argument list instead. The function and all arguments must
/// still be sendable.
///
/// ## Lifetime
///
/// Workers are spawned by [WorkerPool.start]. [close] drains already-submitted
/// jobs, shuts the workers down and releases the ports; it is idempotent, and
/// submitting work after it starts throws [StateError].
library;

import 'dart:async';
import 'dart:io' show Platform;
import 'dart:isolate';
import 'dart:math' as math;

/// Default number of worker isolates for a [WorkerPool].
///
/// Matches the reader's `auto` parallelism: `min(8, processors - 1)`, with a
/// floor of 1 on single-core machines.
int get defaultWorkerPoolSize {
  final processors = Platform.numberOfProcessors;
  if (processors <= 1) return 1;
  return math.min(8, processors - 1);
}

/// Thrown when a worker job fails remotely, cannot be sent, or its isolate
/// dies.
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

/// A fixed pool of reusable worker isolates.
final class WorkerPool {
  WorkerPool._(this._debugName);

  /// Starts [workers] isolates (defaults to [defaultWorkerPoolSize]).
  ///
  /// Throws [RangeError] if [workers] is less than 1.
  static Future<WorkerPool> start({
    int? workers,
    String debugName = 'parquet-worker',
  }) async {
    final count = workers ?? defaultWorkerPoolSize;
    if (count < 1) throw RangeError.value(count, 'workers');

    final pool = WorkerPool._(debugName);
    pool._resultPort.listen(pool._onResult);
    final started = <_PoolWorker>[];
    try {
      for (var index = 0; index < count; index++) {
        final readyPort = ReceivePort();
        final exitPort = ReceivePort();
        final isolate = await Isolate.spawn(
          _workerMain,
          <Object?>[pool._resultPort.sendPort, readyPort.sendPort],
          onExit: exitPort.sendPort,
          errorsAreFatal: true,
          debugName: '$debugName-$index',
        );
        final commands = await readyPort.first.timeout(
          const Duration(seconds: 30),
        ) as SendPort;
        readyPort.close();
        final worker = _PoolWorker(index, isolate, commands, exitPort);
        exitPort.listen((_) => pool._onWorkerExit(worker));
        started.add(worker);
        pool._workers.add(worker);
        pool._idle.add(worker);
      }
    } catch (_) {
      for (final worker in started) {
        worker.commands.send(const <Object?>[-1]);
        worker.exitPort.close();
      }
      pool._resultPort.close();
      rethrow;
    }
    return pool;
  }

  final String _debugName;
  final ReceivePort _resultPort = ReceivePort();
  final List<_PoolWorker> _workers = <_PoolWorker>[];
  final List<_PoolWorker> _idle = <_PoolWorker>[];
  final List<_Job> _queue = <_Job>[];
  final Map<int, _Job> _inflight = <int, _Job>{};
  final List<Completer<void>> _drainWaiters = <Completer<void>>[];
  int _nextJobId = 1;
  bool _closing = false;
  bool _closed = false;
  Future<void>? _closeFuture;

  /// Number of live worker isolates.
  int get workers => _workers.length;

  /// Number of currently idle workers.
  int get idleWorkers => _idle.length;

  /// Jobs queued or running.
  int get pendingJobs => _queue.length + _inflight.length;

  /// Whether [close] has completed.
  bool get isClosed => _closed;

  /// Whether [close] has been called (jobs may still be draining).
  bool get isClosing => _closing;

  /// Runs [computation] on an idle worker.
  ///
  /// The closure is sent to the worker, so its captured context must be
  /// sendable; otherwise this throws a [WorkerPoolException] without killing
  /// the pool. Use [runTask] when that is not practical.
  Future<T> run<T>(FutureOr<T> Function() computation) {
    final completer = Completer<T>();
    _submit(
      _Job(
        id: _nextJobId++,
        kind: _JobKind.closure,
        payload: computation,
        onValue: (value) {
          if (!completer.isCompleted) completer.complete(value as T);
        },
        onError: (error, stackTrace) {
          if (!completer.isCompleted) {
            completer.completeError(error, stackTrace);
          }
        },
      ),
    );
    return completer.future;
  }

  /// Runs a top-level/static [task] with [arguments] on an idle worker.
  ///
  /// This is the batch protocol: both [task] and [arguments] must be sendable,
  /// but no closure context is involved, so it cannot accidentally capture an
  /// unsendable scope.
  Future<T> runTask<T>(
    FutureOr<T> Function(List<Object?> arguments) task, [
    List<Object?> arguments = const <Object?>[],
  ]) {
    final completer = Completer<T>();
    _submit(
      _Job(
        id: _nextJobId++,
        kind: _JobKind.task,
        payload: task,
        arguments: arguments,
        onValue: (value) {
          if (!completer.isCompleted) completer.complete(value as T);
        },
        onError: (error, stackTrace) {
          if (!completer.isCompleted) {
            completer.completeError(error, stackTrace);
          }
        },
      ),
    );
    return completer.future;
  }

  /// Runs every computation, preserving result order.
  ///
  /// Up to [workers] jobs run concurrently; the rest queue. A failing job fails
  /// the returned future (the remaining jobs are still allowed to finish;
  /// call [close] to drain them).
  Future<List<T>> runAll<T>(Iterable<FutureOr<T> Function()> computations) {
    final list = computations.toList(growable: false);
    return Future.wait<T>([for (final computation in list) run(computation)]);
  }

  /// Runs every `(task, arguments)` pair with [runTask], preserving order.
  Future<List<T>> runAllTasks<T>(
    Iterable<(FutureOr<T> Function(List<Object?>), List<Object?>)> tasks,
  ) {
    final list = tasks.toList(growable: false);
    return Future.wait<T>([
      for (final task in list) runTask<T>(task.$1, task.$2),
    ]);
  }

  void _submit(_Job job) {
    if (_closed || _closing) {
      throw StateError('WorkerPool is closed');
    }
    _queue.add(job);
    _drain();
  }

  void _drain() {
    while (_queue.isNotEmpty && _idle.isNotEmpty) {
      final worker = _idle.removeLast();
      final job = _queue.removeAt(0);
      worker.current = job;
      job.worker = worker;
      _inflight[job.id] = job;
      try {
        worker.commands.send(<Object?>[
          job.id,
          job.kind.index,
          job.payload,
          if (job.arguments != null) job.arguments,
        ]);
      } catch (error, stackTrace) {
        _inflight.remove(job.id);
        worker.current = null;
        job.worker = null;
        if (!worker.dead) _idle.add(worker);
        job.fail(
          WorkerPoolException(
            job.kind == _JobKind.closure
                ? 'The computation closure is not sendable to a worker. '
                      'Use WorkerPool.runTask with a top-level function.'
                : 'The task function or its arguments are not sendable to a '
                      'worker.',
            cause: error,
            stackTrace: stackTrace,
          ),
          stackTrace,
        );
      }
    }
    _checkDrained();
  }

  void _onResult(Object? message) {
    if (message is! List<Object?>) return;
    if (message.length < 3 || message[0] is! int) return;
    final id = message[0] as int;
    final job = _inflight.remove(id);
    if (job == null) return;

    final worker = job.worker;
    job.worker = null;
    if (worker != null) {
      worker.current = null;
      if (!worker.dead) _idle.add(worker);
    }

    final ok = message[1] == true;
    if (ok) {
      job.succeed(message[2]);
    } else {
      final error = message[2];
      final remoteStack = message.length > 3 && message[3] is String
          ? message[3] as String
          : null;
      final stringFallback = message.length > 4 && message[4] == true;
      if (stringFallback || error is String) {
        job.fail(
          WorkerPoolException(
            'Remote computation failed: $error',
            remoteStackTrace: remoteStack,
            stackTrace: StackTrace.current,
          ),
          StackTrace.current,
        );
      } else {
        job.fail(
          error ?? WorkerPoolException('Remote computation failed'),
          remoteStack == null
              ? StackTrace.current
              : StackTrace.fromString(remoteStack),
        );
      }
    }
    _drain();
  }

  void _onWorkerExit(_PoolWorker worker) {
    if (worker.dead) return;
    worker.dead = true;
    worker.exitPort.close();
    _idle.remove(worker);
    _workers.remove(worker);

    final job = worker.current;
    worker.current = null;
    if (job != null && !job.done) {
      _inflight.remove(job.id);
      final stackTrace = StackTrace.current;
      job.fail(
        WorkerPoolException(
          'Worker ${worker.index} exited before completing the job',
          stackTrace: stackTrace,
        ),
        stackTrace,
      );
    }
    if (!worker.exited.isCompleted) worker.exited.complete();

    if (_workers.isEmpty && (_queue.isNotEmpty || _inflight.isNotEmpty)) {
      // Queued jobs can no longer run even if a close() is waiting for them.
      _failEverything();
    }
    _checkDrained();
  }

  void _failEverything() {
    final stackTrace = StackTrace.current;
    final error = WorkerPoolException(
      'No worker isolates are left',
      stackTrace: stackTrace,
    );
    final queued = List<_Job>.of(_queue);
    _queue.clear();
    for (final job in queued) {
      job.fail(error, stackTrace);
    }
    final running = List<_Job>.of(_inflight.values);
    _inflight.clear();
    for (final job in running) {
      job.fail(error, stackTrace);
    }
    _checkDrained();
  }

  void _checkDrained() {
    if (_queue.isNotEmpty || _inflight.isNotEmpty) return;
    if (_drainWaiters.isEmpty) return;
    final waiters = List<Completer<void>>.of(_drainWaiters);
    _drainWaiters.clear();
    for (final waiter in waiters) {
      if (!waiter.isCompleted) waiter.complete();
    }
  }

  Future<void> _whenDrained() {
    if (_queue.isEmpty && _inflight.isEmpty) return Future<void>.value();
    final completer = Completer<void>();
    _drainWaiters.add(completer);
    return completer.future;
  }

  /// Drains submitted jobs, shuts every worker down and releases the ports.
  ///
  /// Idempotent. Submitting a job after this has been called throws
  /// [StateError].
  Future<void> close() {
    final existing = _closeFuture;
    if (existing != null) return existing;
    _closing = true;
    final completer = Completer<void>();
    _closeFuture = completer.future;
    unawaited(_closeImpl(completer));
    return completer.future;
  }

  Future<void> _closeImpl(Completer<void> completer) async {
    try {
      await _whenDrained();
      final workers = List<_PoolWorker>.of(_workers);
      for (final worker in workers) {
        if (!worker.dead) {
          try {
            worker.commands.send(const <Object?>[-1]);
          } on Object {
            // The isolate may already be gone; the exit port still fires.
          }
        }
      }
      await Future.wait<void>([
        for (final worker in workers) worker.exited.future,
      ]);
      _resultPort.close();
      _closed = true;
      completer.complete();
    } catch (error, stackTrace) {
      _closed = true;
      completer.completeError(error, stackTrace);
    }
  }

  @override
  String toString() =>
      'WorkerPool($_debugName, workers: $workers, idle: $idleWorkers, '
      'pending: $pendingJobs, closed: $_closed)';
}

enum _JobKind { closure, task }

final class _Job {
  _Job({
    required this.id,
    required this.kind,
    required this.payload,
    required this.onValue,
    required this.onError,
    this.arguments,
  });

  final int id;
  final _JobKind kind;
  final Object payload;
  final List<Object?>? arguments;
  final void Function(Object? value) onValue;
  final void Function(Object error, StackTrace stackTrace) onError;
  _PoolWorker? worker;
  bool done = false;

  void succeed(Object? value) {
    if (done) return;
    done = true;
    onValue(value);
  }

  void fail(Object error, StackTrace stackTrace) {
    if (done) return;
    done = true;
    onError(error, stackTrace);
  }
}

final class _PoolWorker {
  _PoolWorker(this.index, this.isolate, this.commands, this.exitPort);

  final int index;
  final Isolate isolate;
  final SendPort commands;
  final ReceivePort exitPort;
  final Completer<void> exited = Completer<void>();
  _Job? current;
  bool dead = false;
}

// ---------------------------------------------------------------------------
// Worker isolate entry point
// ---------------------------------------------------------------------------

/// Worker loop: receive jobs on a private port, execute, send one reply.
///
/// This must stay a top-level function; it is the isolate entry point.
void _workerMain(List<Object?> args) async {
  final results = args[0] as SendPort;
  final ready = args[1] as SendPort;
  final commands = ReceivePort();
  ready.send(commands.sendPort);

  await for (final message in commands) {
    if (message is! List<Object?> || message.isEmpty) continue;
    final id = message[0] as int;
    if (id == -1) break;

    final kind = message[1] as int;
    try {
      final Object? value;
      if (kind == _JobKind.closure.index) {
        final computation = message[2] as Function;
        final result = Function.apply(computation, const <Object?>[]);
        value = result is Future ? await result : result;
      } else {
        final task = message[2] as Function;
        final arguments = message.length > 3
            ? (message[3] as List<Object?>)
            : const <Object?>[];
        final result = Function.apply(task, <Object?>[arguments]);
        value = result is Future ? await result : result;
      }
      _sendValue(results, id, value);
    } catch (error, stackTrace) {
      _sendError(results, id, error, stackTrace);
    }
  }
  commands.close();
}

void _sendValue(SendPort results, int id, Object? value) {
  try {
    results.send(<Object?>[id, true, value]);
  } catch (error) {
    _sendStringError(
      results,
      id,
      'The computation result is not sendable: $error',
      '',
    );
  }
}

void _sendError(SendPort results, int id, Object error, StackTrace stackTrace) {
  try {
    results.send(<Object?>[id, false, error, stackTrace.toString()]);
  } catch (_) {
    // Error objects with captures can themselves be unsendable.
    _sendStringError(results, id, error.toString(), stackTrace.toString());
  }
}

void _sendStringError(
  SendPort results,
  int id,
  String error,
  String stackTrace,
) {
  try {
    results.send(<Object?>[id, false, error, stackTrace, true]);
  } on Object {
    // The isolate is likely being torn down; nothing else can be reported.
  }
}
