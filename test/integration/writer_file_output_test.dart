@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import '../support/blit_file.dart';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  late Directory tempDir;

  setUp(() {
    tempDir = Directory.systemTemp.createTempSync('parquet_writer_test_');
  });

  tearDown(() {
    if (tempDir.existsSync()) tempDir.deleteSync(recursive: true);
  });

  group('writeParquetTableToFile', () {
    test('writes flat and nested files that read back', () async {
      for (final name in <String>[
        'plain_snappy.parquet',
        'nested_snappy.parquet',
        'nested_structs_multi.parquet',
      ]) {
        final table = readFixtureTable(name);
        final path = '${tempDir.path}/out_$name';
        await writeParquetTableToFile(table, path);
        final bytes = File(path).readAsBytesSync();
        expect(bytes, writeParquetTable(table), reason: name);
        expectTablesEqual(
          table,
          ParquetReader.fromBytes(bytes).readTableSync(),
        );
      }
    });

    test('overwrites and truncates existing contents', () async {
      final table = readFixtureTable('web_tiny.parquet');
      final path = '${tempDir.path}/overwrite.parquet';
      File(path)
          .writeAsBytesSync(Uint8List(100000)..fillRange(0, 100000, 0xAA));
      await writeParquetTableToFile(table, path);
      final bytes = File(path).readAsBytesSync();
      expect(bytes.length, writeParquetTable(table).length);
      expectTablesEqual(table, ParquetReader.fromBytes(bytes).readTableSync());
    });

    test('create: false requires an existing file', () async {
      final table = readFixtureTable('web_tiny.parquet');
      await expectLater(
        writeParquetTableToFile(
          table,
          '${tempDir.path}/missing.parquet',
          create: false,
        ),
        throwsA(isA<FileSystemException>()),
      );
      final path = '${tempDir.path}/exists.parquet';
      File(path).writeAsBytesSync(<int>[1]);
      await writeParquetTableToFile(table, path, create: false);
      expectTablesEqual(
        table,
        ParquetReader.fromBytes(File(path).readAsBytesSync()).readTableSync(),
      );
    });

    test('passes writer options through', () async {
      final table = readFixtureTable('plain_snappy.parquet');
      final path = '${tempDir.path}/options.parquet';
      await writeParquetTableToFile(
        table,
        path,
        options: const ParquetWriterOptions(
          compression: CompressionType.zstd,
          dataPageVersion: 2,
          pageSize: 512,
        ),
      );
      final reader = ParquetReader.fromBytes(File(path).readAsBytesSync());
      expect(
        reader.metadata.rowGroup(0).columnByPath(['i64'])!.compression,
        CompressionType.zstd,
      );
      expect(reader.numRows, table.numRows);
    });
  });

  group('writeParquetTableToBytesFile', () {
    test('writes through a caller-owned BytesFile and truncates', () async {
      final table = readFixtureTable('nested_snappy.parquet');
      final path = '${tempDir.path}/blit.parquet';
      final file = await openBytesFile(path, create: true, readOnly: false);
      // Leave longer stale contents so truncation is observable.
      await file.writeAt(0, Uint8List(1000)..fillRange(0, 1000, 0x5A));
      await file.flush();

      await writeParquetTableToBytesFile(table, file);
      expect(file.isClosed, isFalse, reason: 'the caller owns the file');
      expect(await file.length, writeParquetTable(table).length);
      await file.close();

      final bytes = File(path).readAsBytesSync();
      expectTablesEqual(table, ParquetReader.fromBytes(bytes).readTableSync());
    });

    test('honors writer options', () async {
      final table = readFixtureTable('plain_snappy.parquet');
      final path = '${tempDir.path}/options_blit.parquet';
      final file = await openBytesFile(path, create: true, readOnly: false);
      await writeParquetTableToBytesFile(
        table,
        file,
        options: const ParquetWriterOptions(
          compression: CompressionType.zstd,
          dataPageVersion: 2,
        ),
      );
      await file.close();
      final reader = ParquetReader.fromBytes(File(path).readAsBytesSync());
      expect(
        reader.metadata.rowGroup(0).columnByPath(['i64'])!.compression,
        CompressionType.zstd,
      );
      expectTablesEqual(table, reader.readTableSync());
    });
  });
}
