@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/src/io/mmap_source.dart';
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
    tempDir = Directory.systemTemp.createTempSync('parquet_mmap_test_');
    sampleData = patternBytes(30000);
    sampleFile = File('${tempDir.path}${Platform.pathSeparator}sample.bin')
      ..writeAsBytesSync(sampleData);
  });

  tearDown(() {
    try {
      tempDir.deleteSync(recursive: true);
    } on FileSystemException {
      // A leaked mapping can keep the directory busy on Windows; the leaking
      // test will already have failed on its reads.
    }
  });

  group('MmapSource', () {
    test('maps the whole file and reads zero-copy ranges', () {
      final source = MmapSource.open(sampleFile.path);
      addTearDown(source.close);

      expect(source.path, sampleFile.path);
      expect(source.length, sampleData.length);
      expect(source.fileOffset, 0);
      expect(source.fileLength, sampleData.length);
      expect(source.isMapped, isTrue);
      expect(source.isWritable, isFalse);
      expect(source.isClosed, isFalse);

      expect(source.read(0, 0).length, 0);
      expect(source.read(0, 4).asUint8List(), sampleData.sublist(0, 4));
      expect(
        source.read(1234, 4321).asUint8List(),
        sampleData.sublist(1234, 1234 + 4321),
      );
      expect(source.read(0, sampleData.length).asUint8List(), sampleData);
    });

    test('rejects out-of-range reads', () {
      final source = MmapSource.open(sampleFile.path);
      addTearDown(source.close);
      expect(() => source.read(-1, 1), throwsRangeError);
      expect(() => source.read(0, -1), throwsRangeError);
      expect(() => source.read(sampleData.length - 1, 2), throwsRangeError);
      expect(() => source.read(sampleData.length, 1), throwsRangeError);
    });

    test('maps a window and reads relative to its start', () {
      final source = MmapSource.open(
        sampleFile.path,
        offset: 1000,
        length: 5000,
      );
      addTearDown(source.close);

      expect(source.length, 5000);
      expect(source.fileOffset, 1000);
      expect(source.fileLength, sampleData.length);
      expect(source.read(0, 10).asUint8List(), sampleData.sublist(1000, 1010));
      expect(
        source.read(4900, 100).asUint8List(),
        sampleData.sublist(5900, 6000),
      );
      expect(
        source.read(0, 5000).asUint8List(),
        sampleData.sublist(1000, 6000),
      );

      expect(() => source.read(4999, 2), throwsRangeError);
      expect(() => source.read(-1, 1), throwsRangeError);
    });

    test('validates window bounds', () {
      expect(
        () => MmapSource.open(sampleFile.path, offset: -1),
        throwsRangeError,
      );
      expect(
        () => MmapSource.open(
          sampleFile.path,
          offset: 10,
          length: sampleData.length,
        ),
        throwsRangeError,
      );
      expect(
        () => MmapSource.open(
          sampleFile.path,
          offset: sampleData.length,
          length: 1,
        ),
        throwsRangeError,
      );

      // A zero-length window at EOF is legal.
      final empty = MmapSource.open(sampleFile.path, offset: sampleData.length);
      addTearDown(empty.close);
      expect(empty.length, 0);
      expect(empty.isMapped, isFalse);
      expect(empty.read(0, 0).length, 0);
    });

    test('close is idempotent and invalidates the source', () {
      final source = MmapSource.open(sampleFile.path);
      expect(source.read(0, 100).length, 100);
      source.close();
      source.close();

      expect(source.isClosed, isTrue);
      expect(() => source.length, throwsStateError);
      expect(() => source.read(0, 1), throwsStateError);
      expect(() => source.prefetch(), throwsStateError);
    });

    test('prefetch and the top-level factories work', () async {
      final sync = openMmapSourceSync(sampleFile.path, prefetch: true);
      expect(sync.read(10, 20).asUint8List(), sampleData.sublist(10, 30));
      sync.close();

      final async = await openMmapSource(sampleFile.path);
      expect(async.read(20, 20).asUint8List(), sampleData.sublist(20, 40));
      expect(async, isA<MmapSource>());
      async.close();
    });

    test('maps an empty file as a zero-length source', () {
      final emptyFile = File(
        '${tempDir.path}${Platform.pathSeparator}empty.bin',
      )..writeAsBytesSync(const <int>[]);
      final source = MmapSource.open(emptyFile.path);
      addTearDown(source.close);

      expect(source.length, 0);
      expect(source.fileLength, 0);
      expect(source.isMapped, isFalse);
      expect(source.read(0, 0).length, 0);
      expect(() => source.read(0, 1), throwsRangeError);
    });

    test('missing files throw', () {
      expect(
        () => MmapSource.open(
          '${tempDir.path}${Platform.pathSeparator}missing.bin',
        ),
        throwsA(isA<Exception>()),
      );
    });
  });
}
