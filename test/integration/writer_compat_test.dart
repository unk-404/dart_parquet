@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

/// Regression guard for the v0.1.1 byte layout: with all v0.1.2 options left at
/// their defaults the writer must produce the same file as v0.1.1. The
/// `created_by` field is pinned to the v0.1.1 value here because v0.1.2 bumps
/// it deliberately; apart from that string the output is unchanged.
void main() {
  group('v0.1.1 compatibility', () {
    test('flat default output is unchanged', () {
      final table = _flat();
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(
          createdBy: 'parquet-dart version 0.1.1',
        ),
      );
      expect(bytes.length, 62576);
      expect(_fnv(bytes), 4137258665);
    });

    test('nested default output is unchanged', () {
      final table = _nested();
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(
          createdBy: 'parquet-dart version 0.1.1',
        ),
      );
      expect(bytes.length, 6835);
      expect(_fnv(bytes), 2817446741);
    });

    test('v2 + zstd output is unchanged', () {
      final table = _flat();
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(
          compression: CompressionType.zstd,
          dataPageVersion: 2,
          pageSize: 256,
          createdBy: 'parquet-dart version 0.1.1',
        ),
      );
      expect(bytes.length, 29684);
      expect(_fnv(bytes), 929198025);
    });

    test('multi-row-group output is unchanged', () {
      final reader = ParquetReader.fromBytes(
        File('test/fixtures/nested_structs_multi.parquet').readAsBytesSync(),
      );
      final writer = ParquetWriter(
        schema: reader.schema,
        options: const ParquetWriterOptions(
          createdBy: 'parquet-dart version 0.1.1',
        ),
      );
      for (var i = 0; i < reader.numRowGroups; i++) {
        writer.add(reader.readRowGroupSync(i));
      }
      final bytes = writer.finish();
      expect(bytes.length, 4208);
      expect(_fnv(bytes), 1353453133);
    });

    test('explicitly disabled v0.1.2 options match the defaults exactly', () {
      final table = _flat();
      final oldBy = const ParquetWriterOptions(
        createdBy: 'parquet-dart version 0.1.1',
      );
      final explicit = writeParquetTable(
        table,
        options: const ParquetWriterOptions(
          createdBy: 'parquet-dart version 0.1.1',
          writeCrc: false,
          writePageIndex: false,
          writeBloomFilter: false,
        ),
      );
      expect(explicit, writeParquetTable(table, options: oldBy));
    });

    test('default created_by is the v0.2.0 value', () {
      final bytes = writeParquetTable(_flat());
      final reader = ParquetReader.fromBytes(bytes);
      expect(reader.createdBy, 'parquet-dart version 0.2.0');
      expect(reader.metadata.rowGroup(0).column(0).offsetIndexOffset, isNull);
    });
  });
}

ParquetTable _flat() => ParquetReader.fromBytes(
  File('test/fixtures/plain_snappy.parquet').readAsBytesSync(),
).readTableSync();

ParquetTable _nested() => ParquetReader.fromBytes(
  File('test/fixtures/nested_snappy.parquet').readAsBytesSync(),
).readTableSync();

int _fnv(Uint8List bytes) {
  var hash = 0x811C9DC5;
  for (final byte in bytes) {
    hash ^= byte;
    hash = (hash * 0x01000193) & 0xFFFFFFFF;
  }
  return hash;
}
