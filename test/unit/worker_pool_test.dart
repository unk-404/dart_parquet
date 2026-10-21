@TestOn('vm')
library;

import 'dart:async';
import 'dart:developer' as developer show Service;
import 'dart:isolate' show Isolate, ReceivePort;
import 'dart:typed_data';

import 'package:parquet/src/parallel/worker_pool.dart';
import 'package:test/test.dart';

/// Creates a closure in its own scope so the captured context only contains
/// [value] (Dart captures whole contexts, not single variables).
FutureOr<int> Function() _captureInt(int value) =>
    () => value * 2;

FutureOr<int> Function() _captureBytesLength(Uint8List bytes) =>
    () => bytes.length;

int _add(List<Object?> args) => (args[0] as int) + (args[1] as int);

String _describe(List<Object?> args) => '${args[0]}:${args[1]}';

int _failTask(List<Object?> args) => throw StateError('task boom ${args[0]}');

String _isolateId() {
  final id = developer.Service.getIsolateId(Isolate.current);
  if (id == null) throw StateError('no isolate id');
  return id;
}

Future<int> _delayedValue() async {
  await Future<void>.delayed(const Duration(milliseconds: 20));
  return 7;
}

Uint8List _makeBytes(List<Object?> args) =>
    Uint8List.fromList([1, 2, 3, args.length]);

void main() {
  group('WorkerPool', () {
    late WorkerPool pool;

    setUp(() async {
      pool = await WorkerPool.start(workers: 2);
    });

    tearDown(() async {
      await pool.close();
    });

    test('runs sync and async closures on workers', () async {
      expect(await pool.run(() => 21 * 2), 42);
      expect(
        await pool.run(() async {
          await Future<void>.delayed(const Duration(milliseconds: 1));
          return 'async-ok';
        }),
        'async-ok',
      );
      await pool.run<void>(() {});
    });

    test('closures capture sendable local state via a helper scope', () async {
      expect(await pool.run(_captureInt(21)), 42);
      expect(
        await pool.run(_captureBytesLength(Uint8List.fromList([1, 2, 3]))),
        3,
      );
    });

    test('defaultWorkerPoolSize is at least one', () {
      expect(defaultWorkerPoolSize, greaterThanOrEqualTo(1));
      expect(pool.workers, 2);
      expect(pool.idleWorkers, 2);
    });

    test('reuses the same persistent isolate', () async {
      final first = await pool.run(_isolateId);
      final second = await pool.run(_isolateId);
      expect(second, first);
      final current = developer.Service.getIsolateId(Isolate.current);
      expect(first, isNot(current));
    });

    test('runAll preserves order and uses the queue', () async {
      final values = await pool.runAll<int>([
        for (var i = 0; i < 24; i++) _captureInt(i),
      ]);
      expect(values, [for (var i = 0; i < 24; i++) i * 2]);
      expect(pool.pendingJobs, 0);
    });

    test('runTask is the explicit top-level protocol', () async {
      expect(await pool.runTask(_add, [2, 3]), 5);
      expect(await pool.runTask(_describe, ['a', 7]), 'a:7');
      final bytes = await pool.runTask(_makeBytes);
      expect(bytes, [1, 2, 3, 0]);
    });

    test('runAllTasks preserves order', () async {
      final values = await pool.runAllTasks<int>([
        (_add, [1, 2]),
        (_add, [10, 20]),
        (_add, [-1, 1]),
      ]);
      expect(values, [3, 30, 0]);
    });

    test('remote errors keep their type and the pool stays usable', () async {
      await expectLater(
        pool.run<int>(() => throw StateError('closure boom')),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            'message',
            contains('closure boom'),
          ),
        ),
      );
      await expectLater(
        pool.runTask(_failTask, ['x']),
        throwsA(
          isA<StateError>().having(
            (error) => error.message,
            'message',
            contains('task boom x'),
          ),
        ),
      );
      expect(await pool.run(() => 1), 1);
    });

    test('unsendable closures fail clearly without killing the pool', () async {
      final port = ReceivePort();
      addTearDown(port.close);
      await expectLater(
        pool.run<int>(() => port.hashCode),
        throwsA(isA<WorkerPoolException>()),
      );
      expect(await pool.run(() => 5), 5);
      expect(pool.workers, 2);
    });

    test('runs many jobs across both workers', () async {
      final futures = [
        for (var i = 0; i < 100; i++) pool.runTask(_add, [i, i]),
      ];
      final values = await Future.wait(futures);
      expect(values, [for (var i = 0; i < 100; i++) i * 2]);
    });

    test(
      'close drains submitted jobs, is idempotent and rejects new work',
      () async {
        final pending = pool.run(_delayedValue);
        await pool.close();
        await pool.close();
        expect(pool.isClosed, isTrue);
        expect(await pending, 7);
        expect(() => pool.run(() => 1), throwsStateError);
        expect(() => pool.runTask(_add, [1, 2]), throwsStateError);
      },
    );
  });
}
