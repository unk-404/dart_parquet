@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:parquet/src/io/blit_source.dart';
import 'package:parquet/src/io/blob_slice_source.dart';
import 'package:parquet/src/io/file_source.dart';
import 'package:parquet/src/io/http_range_example.dart';
import 'package:parquet/src/io/native_file.dart';
import 'package:parquet/src/io/open_source.dart';
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
    tempDir = Directory.systemTemp.createTempSync('parquet_io_test_');
    sampleData = patternBytes(30000);
    sampleFile = File('${tempDir.path}${Platform.pathSeparator}sample.bin')
      ..writeAsBytesSync(sampleData);
  });

  tearDown(() {
    try {
      tempDir.deleteSync(recursive: true);
    } on FileSystemException {
      // Windows can report the directory as busy if a handle leaked; the test
      // that leaked it will already have failed on content or timing.
    }
  });

  group('MemorySource', () {
    test('exposes length, slices and end-of-file reads', () {
      final source = MemorySource.fromUint8List(sampleData);
      expect(source.length, sampleData.length);
      expect(source.read(0, 4).asUint8List(), sampleData.sublist(0, 4));
      expect(
        source.read(1234, 4321).asUint8List(),
        sampleData.sublist(1234, 1234 + 4321),
      );
      expect(source.read(sampleData.length, 0).length, 0);
      source.close();
    });

    test('rejects out-of-range reads', () {
      final source = MemorySource.fromUint8List(sampleData);
      expect(() => source.read(-1, 1), throwsRangeError);
      expect(() => source.read(0, -1), throwsRangeError);
      expect(() => source.read(sampleData.length - 1, 2), throwsRangeError);
    });
  });

  group('SyncSourceAdapter', () {
    test('adapts a sync source to the async interface', () async {
      final adapter = SyncSourceAdapter(MemorySource.fromUint8List(sampleData));
      expect(await adapter.length, sampleData.length);
      expect(
        (await adapter.read(100, 200)).asUint8List(),
        sampleData.sublist(100, 300),
      );
      await adapter.close();
    });
  });

  group('native file source (async)', () {
    test('reads random ranges and closes idempotently', () async {
      final source = await NativeFileSource.open(sampleFile.path);
      addTearDown(source.close);

      expect(await source.length, sampleData.length);
      expect((await source.read(0, 0)).length, 0);
      expect(
        (await source.read(1000, 2500)).asUint8List(),
        sampleData.sublist(1000, 3500),
      );
      expect(
        (await source.read(0, sampleData.length)).asUint8List(),
        sampleData,
      );

      await source.close();
      await source.close();
      await expectLater(source.length, throwsStateError);
      await expectLater(source.read(0, 1), throwsStateError);
    });

    test('serializes concurrent reads', () async {
      final source = await NativeFileSource.open(sampleFile.path);
      addTearDown(source.close);
      final reads = await Future.wait([
        for (var i = 0; i < 32; i++) source.read(i * 137, 91),
      ]);
      for (var i = 0; i < reads.length; i++) {
        expect(
          reads[i].asUint8List(),
          sampleData.sublist(i * 137, i * 137 + 91),
        );
      }
    });

    test('rejects out-of-range reads', () async {
      final source = await NativeFileSource.open(sampleFile.path);
      addTearDown(source.close);
      await expectLater(source.read(-1, 1), throwsRangeError);
      await expectLater(source.read(0, -1), throwsRangeError);
      await expectLater(
        source.read(sampleData.length - 1, 2),
        throwsRangeError,
      );
    });

    test('missing files throw FileSystemException', () async {
      await expectLater(
        openFileSource(
          '${tempDir.path}${Platform.pathSeparator}missing.parquet',
        ),
        throwsA(isA<FileSystemException>()),
      );
    });
  });

  group('native file source (sync)', () {
    test('reads random ranges and closes idempotently', () {
      final source = openFileSourceSync(sampleFile.path);
      expect(source.length, sampleData.length);
      expect(source.read(0, 0).length, 0);
      expect(
        source.read(500, 900).asUint8List(),
        sampleData.sublist(500, 1400),
      );
      expect(source.read(0, sampleData.length).asUint8List(), sampleData);
      source.close();
      source.close();
      expect(() => source.length, throwsStateError);
      expect(() => source.read(0, 1), throwsStateError);
    });

    test('rejects out-of-range reads', () {
      final source = SyncNativeFileSource.open(sampleFile.path);
      addTearDown(source.close);
      expect(() => source.read(-1, 1), throwsRangeError);
      expect(() => source.read(0, -1), throwsRangeError);
      expect(() => source.read(sampleData.length - 1, 2), throwsRangeError);
    });
  });

  group('file_source conditional export', () {
    test('resolves to the native backend on the VM', () async {
      expect(fileSourcesSupported, isTrue);
      expect(nativeFileSourcesSupported, isTrue);

      final async = await openFileSource(sampleFile.path);
      expect(await async.length, sampleData.length);
      expect(
        (await async.read(7, 13)).asUint8List(),
        sampleData.sublist(7, 20),
      );
      await async.close();

      final sync = openFileSourceSync(sampleFile.path);
      expect(sync.length, sampleData.length);
      expect(sync.read(7, 13).asUint8List(), sampleData.sublist(7, 20));
      sync.close();
    });
  });

  group('BlitSource', () {
    test('reads through blit BytesFile', () async {
      final source = await BlitSource.open(sampleFile.path);
      addTearDown(source.close);
      expect(await source.length, sampleData.length);
      expect(
        (await source.read(1234, 4321)).asUint8List(),
        sampleData.sublist(1234, 1234 + 4321),
      );
      expect((await source.read(0, 0)).length, 0);
      expect(
        (await source.read(0, sampleData.length)).asUint8List(),
        sampleData,
      );
    });

    test('rejects out-of-range reads', () async {
      final source = await BlitSource.open(sampleFile.path);
      addTearDown(source.close);
      await expectLater(source.read(-1, 1), throwsRangeError);
      await expectLater(source.read(sampleData.length, 1), throwsRangeError);
    });
  });

  group('BlobSliceSource', () {
    test('reads callback ranges', () async {
      final source = BlobSliceSource(
        length: sampleData.length,
        readRange: (offset, length) async =>
            Uint8List.sublistView(sampleData, offset, offset + length),
      );
      addTearDown(source.close);
      expect(await source.length, sampleData.length);
      expect(
        (await source.read(99, 1000)).asUint8List(),
        sampleData.sublist(99, 1099),
      );
      expect((await source.read(sampleData.length, 0)).length, 0);
    });

    test('rejects short reads and invalid ranges', () async {
      final short = BlobSliceSource(
        length: 10,
        readRange: (offset, length) async => Uint8List(3),
      );
      await expectLater(short.read(0, 5), throwsA(isA<ParquetEofException>()));

      final source = BlobSliceSource(
        length: sampleData.length,
        readRange: (offset, length) async =>
            Uint8List.sublistView(sampleData, offset, offset + length),
      );
      await expectLater(source.read(-1, 1), throwsRangeError);
      await expectLater(source.read(sampleData.length, 1), throwsRangeError);
      await source.close();
      await expectLater(source.length, throwsStateError);
    });
  });

  group('HttpRangeSource', () {
    test('fetches the length once and reads ranges', () async {
      var lengthCalls = 0;
      final source = HttpRangeSource(
        lengthProvider: () async {
          lengthCalls++;
          return sampleData.length;
        },
        fetchRange: (offset, length) async =>
            Bytes.fromUint8List(sampleData.sublist(offset, offset + length)),
      );
      addTearDown(source.close);

      expect(await source.length, sampleData.length);
      expect(await source.length, sampleData.length);
      expect(lengthCalls, 1);
      expect(
        (await source.read(5000, 500)).asUint8List(),
        sampleData.sublist(5000, 5500),
      );
    });

    test('rejects short range responses', () async {
      final source = HttpRangeSource(
        lengthProvider: () async => 100,
        fetchRange: (offset, length) async => Bytes.fromUint8List(Uint8List(3)),
      );
      await expectLater(
        source.read(0, 10),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('rejects reads after close', () async {
      final source = HttpRangeSource(
        lengthProvider: () async => 100,
        fetchRange: (offset, length) async =>
            Bytes.fromUint8List(Uint8List(length)),
      );
      await source.close();
      await expectLater(source.length, throwsStateError);
      await expectLater(source.read(0, 1), throwsStateError);
    });
  });

  group('openSource helper', () {
    test('opens plain paths and file:// URIs', () async {
      final fromPath = await openSource(sampleFile.path);
      expect(await fromPath.length, sampleData.length);
      await fromPath.close();

      final fromUri = await openSource(sampleFile.uri.toString());
      expect(
        (await fromUri.read(0, sampleData.length)).asUint8List(),
        sampleData,
      );
      await fromUri.close();

      final sync = openSourceSync(sampleFile.uri.toString());
      expect(sync.read(0, 10).asUint8List(), sampleData.sublist(0, 10));
      sync.close();
    });

    test('explains that HTTP(S) needs a range fetcher', () async {
      await expectLater(
        openSource('https://example.com/data.parquet'),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => openSourceSync('http://example.com/data.parquet'),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('explains that OPFS is async-only', () {
      expect(
        () => openSourceSync('opfs://data/file.parquet'),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });
  });
}
