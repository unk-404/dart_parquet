@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/src/io/parallel_source.dart';
import 'package:parquet/src/io/source.dart';
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
    tempDir = Directory.systemTemp.createTempSync('parquet_parallel_test_');
    sampleData = patternBytes(40000);
    sampleFile = File('${tempDir.path}${Platform.pathSeparator}sample.bin')
      ..writeAsBytesSync(sampleData);
  });

  tearDown(() {
    try {
      tempDir.deleteSync(recursive: true);
    } on FileSystemException {
      // Isolate teardown on Windows may briefly keep the file open; the
      // affected test will already have failed.
    }
  });

  group('ParallelBytesFileSource', () {
    test('reads ranges and batched blocks', () async {
      final source = await ParallelBytesFileSource.open(
        sampleFile.path,
        workers: 2,
      );
      addTearDown(source.close);

      expect(source.workers, 2);
      expect(source.isClosed, isFalse);
      expect(await source.length, sampleData.length);
      expect((await source.read(0, 0)).length, 0);
      expect(
        (await source.read(1234, 4321)).asUint8List(),
        sampleData.sublist(1234, 1234 + 4321),
      );
      expect(
        (await source.read(0, sampleData.length)).asUint8List(),
        sampleData,
      );

      expect(source, isA<BatchedParquetSource>());
      final offsets = [0, 100, 20000, sampleData.length - 64];
      final batch = await source.readMany(offsets, 64);
      expect(batch.length, offsets.length);
      for (var i = 0; i < offsets.length; i++) {
        expect(
          batch[i].asUint8List(),
          sampleData.sublist(offsets[i], offsets[i] + 64),
          reason: 'batch[$i] at ${offsets[i]}',
        );
      }

      final empty = await source.readMany(const <int>[], 8);
      expect(empty, isEmpty);
      final zero = await source.readMany([0, sampleData.length], 0);
      expect(zero.map((b) => b.length), [0, 0]);
    });

    test('serializes nothing: concurrent reads are independent', () async {
      final source = await ParallelBytesFileSource.open(
        sampleFile.path,
        workers: 4,
      );
      addTearDown(source.close);

      final reads = await Future.wait([
        for (var i = 0; i < 24; i++) source.read(i * 211, 97),
      ]);
      for (var i = 0; i < reads.length; i++) {
        expect(
          reads[i].asUint8List(),
          sampleData.sublist(i * 211, i * 211 + 97),
        );
      }
    });

    test('validates ranges against the file length', () async {
      final source = await ParallelBytesFileSource.open(
        sampleFile.path,
        workers: 1,
      );
      addTearDown(source.close);

      await expectLater(source.read(-1, 1), throwsRangeError);
      await expectLater(source.read(0, -1), throwsRangeError);
      await expectLater(
        source.read(sampleData.length - 1, 2),
        throwsRangeError,
      );
      await expectLater(source.readMany([-1], 4), throwsRangeError);
      await expectLater(
        source.readMany([sampleData.length - 2], 4),
        throwsRangeError,
      );
      await expectLater(source.readMany([0], -1), throwsRangeError);
    });

    test('close is idempotent and invalidates the source', () async {
      final source = await ParallelBytesFileSource.open(
        sampleFile.path,
        workers: 1,
      );
      await source.close();
      await source.close();

      expect(source.isClosed, isTrue);
      await expectLater(source.length, throwsStateError);
      await expectLater(source.read(0, 1), throwsStateError);
      await expectLater(source.readMany([0], 1), throwsStateError);
    });

    test('openParallelSource factory returns a usable source', () async {
      final source = await openParallelSource(sampleFile.path, workers: 1);
      addTearDown(source.close);
      expect(
        (await source.read(5, 10)).asUint8List(),
        sampleData.sublist(5, 15),
      );
      expect(source, isA<BatchedParquetSource>());
    });
  });
}
