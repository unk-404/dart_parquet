@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/src/io/io_strategy.dart';
import 'package:parquet/src/io/mmap_source.dart';
import 'package:parquet/src/io/native_file.dart';
import 'package:parquet/src/io/parallel_source.dart';
import 'package:parquet/src/io/source.dart';
import 'package:parquet/src/util/errors.dart';
import 'package:test/test.dart';

Uint8List patternBytes(int length) {
  final out = Uint8List(length);
  for (var i = 0; i < length; i++) {
    out[i] = (i * 37 + i ~/ 13 + 11) & 0xff;
  }
  return out;
}

void main() {
  late Directory tempDir;
  late File sampleFile;
  late Uint8List sampleData;

  setUp(() {
    tempDir = Directory.systemTemp.createTempSync('parquet_strategy_test_');
    sampleData = patternBytes(30000);
    sampleFile = File('${tempDir.path}${Platform.pathSeparator}sample.bin')
      ..writeAsBytesSync(sampleData);
  });

  tearDown(() {
    try {
      tempDir.deleteSync(recursive: true);
    } on FileSystemException {
      // A leaked handle can keep the directory busy on Windows; the affected
      // test will already have failed.
    }
  });

  group('resolveIoStrategy', () {
    test('explicit strategies pass through unchanged', () {
      for (final strategy in const [
        IoStrategy.sync,
        IoStrategy.mmap,
        IoStrategy.batched,
        IoStrategy.parallel,
      ]) {
        expect(
          resolveIoStrategy(strategy, fileLength: 1),
          strategy,
          reason: '${strategy.name} must not depend on file length',
        );
        expect(
          resolveIoStrategy(
            strategy,
            fileLength: autoMmapThreshold * 2,
            nativeFileSystem: false,
          ),
          strategy,
        );
      }
    });

    test('auto picks mmap for small native files', () {
      expect(
        resolveIoStrategy(IoStrategy.auto, fileLength: 0),
        IoStrategy.mmap,
      );
      expect(
        resolveIoStrategy(IoStrategy.auto, fileLength: autoMmapThreshold),
        IoStrategy.mmap,
      );
    });

    test('auto picks the parallel pool for large native files', () {
      expect(
        resolveIoStrategy(IoStrategy.auto, fileLength: autoMmapThreshold + 1),
        IoStrategy.parallel,
      );
    });

    test('auto falls back to batched on the web', () {
      expect(
        resolveIoStrategy(
          IoStrategy.auto,
          fileLength: 0,
          nativeFileSystem: false,
        ),
        IoStrategy.batched,
      );
      expect(
        resolveIoStrategy(
          IoStrategy.auto,
          fileLength: autoMmapThreshold * 4,
          nativeFileSystem: false,
        ),
        IoStrategy.batched,
      );
    });

    test('rejects a negative file length', () {
      expect(
        () => resolveIoStrategy(IoStrategy.auto, fileLength: -1),
        throwsRangeError,
      );
    });
  });

  group('openSourceWithStrategy', () {
    test('auto maps a small file and exposes a sync view', () async {
      final opened = await openSourceWithStrategy(sampleFile.path);
      addTearDown(opened.close);

      expect(opened.requested, IoStrategy.auto);
      expect(opened.resolved, IoStrategy.mmap);
      expect(opened.canReadSync, isTrue);
      expect(opened.sync, isA<MmapSource>());
      expect(opened.source, isA<SyncSourceAdapter>());
      expect(await opened.length, sampleData.length);
      expect(
        (await opened.read(10, 20)).asUint8List(),
        sampleData.sublist(10, 30),
      );
      expect(
        opened.sync!.read(10, 20).asUint8List(),
        sampleData.sublist(10, 30),
      );
    });

    test('sync uses plain RandomAccessFile reads', () async {
      final opened = await openSourceWithStrategy(
        sampleFile.path,
        strategy: IoStrategy.sync,
      );
      addTearDown(opened.close);

      expect(opened.resolved, IoStrategy.sync);
      expect(opened.sync, isA<SyncNativeFileSource>());
      expect(
        (await opened.read(100, 50)).asUint8List(),
        sampleData.sublist(100, 150),
      );
      expect(
        opened.sync!.read(100, 50).asUint8List(),
        sampleData.sublist(100, 150),
      );
    });

    test('mmap accepts a file:// URI', () async {
      final opened = await openSourceWithStrategy(
        sampleFile.uri.toString(),
        strategy: IoStrategy.mmap,
        prefetch: true,
      );
      addTearDown(opened.close);

      expect(opened.resolved, IoStrategy.mmap);
      expect(
        (await opened.read(0, 32)).asUint8List(),
        sampleData.sublist(0, 32),
      );
    });

    test('batched uses a worker pool and has no sync view', () async {
      final opened = await openSourceWithStrategy(
        sampleFile.path,
        strategy: IoStrategy.batched,
        workers: 2,
      );
      addTearDown(opened.close);

      expect(opened.resolved, IoStrategy.batched);
      expect(opened.canReadSync, isFalse);
      expect(opened.sync, isNull);
      expect(opened.source, isA<ParallelBytesFileSource>());
      expect(
        (await opened.read(200, 300)).asUint8List(),
        sampleData.sublist(200, 500),
      );
    });

    test('parallel honors an explicit worker count', () async {
      final opened = await openSourceWithStrategy(
        sampleFile.path,
        strategy: IoStrategy.parallel,
        workers: 1,
      );
      addTearDown(opened.close);

      expect(opened.resolved, IoStrategy.parallel);
      final pool = opened.source as ParallelBytesFileSource;
      expect(pool.workers, 1);
      expect(
        (await opened.read(500, 100)).asUint8List(),
        sampleData.sublist(500, 600),
      );
    });

    test('rejects HTTP(S) locations', () async {
      await expectLater(
        openSourceWithStrategy('https://example.com/data.parquet'),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('rejects invalid tuning parameters', () async {
      await expectLater(
        openSourceWithStrategy(sampleFile.path, workers: 0),
        throwsRangeError,
      );
      await expectLater(
        openSourceWithStrategy(
          sampleFile.path,
          strategy: IoStrategy.parallel,
          mmapReadCacheLimit: -1,
        ),
        throwsRangeError,
      );
      await expectLater(
        openSourceWithStrategy(
          sampleFile.path,
          strategy: IoStrategy.parallel,
          maxQueue: -1,
        ),
        throwsRangeError,
      );
    });

    test('missing files surface filesystem errors', () async {
      await expectLater(
        openSourceWithStrategy(
          '${tempDir.path}${Platform.pathSeparator}missing.bin',
        ),
        throwsA(isA<FileSystemException>()),
      );
    });

    test('close is idempotent and closes the sync view', () async {
      final opened = await openSourceWithStrategy(
        sampleFile.path,
        strategy: IoStrategy.mmap,
      );
      await opened.close();
      await opened.close();

      expect(opened.canReadSync, isTrue);
      expect(() => opened.sync!.read(0, 1), throwsStateError);
      await expectLater(opened.read(0, 1), throwsStateError);
    });
  });
}
