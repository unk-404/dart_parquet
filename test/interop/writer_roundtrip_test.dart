@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import '../../tool/interop/interop_canonical.dart';
import '../../tool/interop/write_matrix.dart' as matrix;

/// CI-native writer matrix: every case is written with the public writer and
/// read back with the public reader, comparing canonical value checksums. The
/// Python engines are verified separately by `tool/interop/verify_writer.py`.
void main() {
  final flat = matrix.loadSourceTable('flat');
  final nested = matrix.loadSourceTable('nested');
  final cases = matrix.buildMatrix();

  test('matrix covers flat+nested, all codecs, v1/v2, dictionary on/off', () {
    expect(cases.length, 56);
    expect(
      cases.map((c) => c.compression).toSet(),
      matrix.matrixCodecs.toSet(),
    );
    expect(cases.map((c) => c.pageVersion).toSet(), <int>{1, 2});
    expect(cases.map((c) => c.dictionary).toSet(), <bool>{true, false});
    expect(cases.map((c) => c.shape).toSet(), <String>{'flat', 'nested'});
  });

  for (final testCase in cases) {
    test('roundtrip ${testCase.id}', () {
      final source = testCase.shape == 'flat' ? flat : nested;
      final bytes = matrix.writeWithOptions(source, testCase.options);
      final reader = ParquetReader.fromBytes(bytes);
      expect(reader.numRows, source.numRows);
      expect(reader.numRowGroups, 1);
      expect(reader.createdBy, testCase.options.createdBy);

      final read = reader.readTableSync();
      expect(read.numRows, source.numRows);
      expect(
        read.columns.map((column) => column.name).toList(),
        source.columns.map((column) => column.name).toList(),
      );
      for (var i = 0; i < source.columns.length; i++) {
        expect(
          columnChecksum(read.columns[i]),
          columnChecksum(source.columns[i]),
          reason: '${testCase.id} column ${source.columns[i].name}',
        );
      }

      final rowGroup = reader.metadata.rowGroup(0);
      final dataPageType = testCase.pageVersion == 2
          ? PageType.dataPageV2
          : PageType.dataPage;
      var dictionaryChunks = 0;
      for (var i = 0; i < rowGroup.numColumns; i++) {
        final chunk = rowGroup.column(i);
        expect(
          chunk.compression,
          testCase.compression,
          reason: '${testCase.id} ${chunk.dotPath} compression',
        );
        expect(
          chunk.hasDictionaryPage,
          chunk.isDictionaryEncoded,
          reason: '${testCase.id} ${chunk.dotPath} dictionary metadata',
        );
        if (chunk.hasDictionaryPage) dictionaryChunks++;
        final stats = chunk.encodingStats;
        expect(
          stats,
          isNotNull,
          reason: '${testCase.id} ${chunk.dotPath} stats',
        );
        final dataPages = stats!.where(
          (stat) => stat.pageType != PageType.dictionaryPage,
        );
        expect(
          dataPages,
          isNotEmpty,
          reason: '${testCase.id} ${chunk.dotPath} data pages',
        );
        for (final stat in dataPages) {
          expect(
            stat.pageType,
            dataPageType,
            reason: '${testCase.id} ${chunk.dotPath} page version',
          );
        }
        if (!testCase.dictionary) {
          expect(
            chunk.isDictionaryEncoded,
            isFalse,
            reason: '${testCase.id} ${chunk.dotPath} must not use a dictionary',
          );
        }
      }
      if (!testCase.dictionary) {
        expect(dictionaryChunks, 0, reason: '${testCase.id} dictionary pages');
      }
    });
  }

  test('streaming writer produces four row groups with identical values', () {
    final sourceBytes = File(matrix.sourceFixture).readAsBytesSync();
    final sourceReader = ParquetReader.fromBytes(sourceBytes);
    final writer = ParquetWriter(
      schema: flat.schema,
      options: const ParquetWriterOptions(
        compression: CompressionType.snappy,
        pageSize: 256,
      ),
    );
    for (var i = 0; i < sourceReader.numRowGroups; i++) {
      writer.add(sourceReader.readRowGroupSync(i, columns: matrix.flatColumns));
    }
    final bytes = writer.finish();
    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.numRows, flat.numRows);
    expect(reader.numRowGroups, sourceReader.numRowGroups);
    final read = reader.readTableSync();
    for (var i = 0; i < flat.columns.length; i++) {
      expect(
        columnChecksum(read.columns[i]),
        columnChecksum(flat.columns[i]),
        reason: 'streamed column ${flat.columns[i].name}',
      );
    }
  });

  test('writer emits a readable zero-row file', () {
    final empty = ParquetReader.fromBytes(
      File(matrix.sourceFixture).readAsBytesSync(),
    ).readRowGroupSync(0, columns: matrix.flatColumns, maxRows: 0);
    final bytes = writeParquetTable(empty);
    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.numRows, 0);
    expect(reader.numRowGroups, 1);
    final table = reader.readTableSync();
    expect(table.numRows, 0);
    expect(table.columns.length, matrix.flatColumns.length);
    for (final column in table.columns) {
      expect(column.length, 0);
    }
  });
}
