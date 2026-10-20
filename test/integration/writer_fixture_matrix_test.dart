@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

final List<String> fixtures =
    Directory('test/fixtures')
        .listSync()
        .whereType<File>()
        .where((file) => file.path.endsWith('.parquet'))
        .map((file) => file.uri.pathSegments.last)
        .toList()
      ..sort();

/// Round-trips every committed fixture (flat and nested) through the writer and
/// compares all values and the schema tree.
void main() {
  setUpAll(() {
    expect(fixtures, isNotEmpty);
  });

  for (final name in fixtures) {
    group(name, () {
      for (final options in <ParquetWriterOptions>[
        const ParquetWriterOptions(),
        const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
          pageSize: 512,
        ),
        const ParquetWriterOptions(
          compression: CompressionType.zstd,
          pageSize: 4096,
        ),
      ]) {
        test('${options.compression.name} v${options.dataPageVersion}', () {
          final source = readFixtureTable(name);
          final reader = writeAndOpen(source, options: options);
          expectSchemasEqual(source.schema, reader.schema);
          expectTablesEqual(source, reader.readTableSync());
          expect(reader.numRows, source.numRows);
        });
      }
    });
  }

  test('every fixture keeps its row-group structure when copied', () {
    for (final name in fixtures) {
      final sourceReader = readFixtureReader(name);
      final writer = ParquetWriter(schema: sourceReader.schema);
      for (var i = 0; i < sourceReader.numRowGroups; i++) {
        writer.add(sourceReader.readRowGroupSync(i));
      }
      final reader = ParquetReader.fromBytes(writer.finish());
      expect(reader.numRowGroups, sourceReader.numRowGroups, reason: name);
      expect(reader.numRows, sourceReader.numRows, reason: name);
      expectTablesEqual(sourceReader.readTableSync(), reader.readTableSync());
    }
  });
}
