@TestOn('vm')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  group('nested roundtrip', () {
    for (final name in <String>[
      'nested_snappy.parquet',
      'nested_uncompressed.parquet',
      'nested_structs_multi.parquet',
      'web_tiny.parquet',
    ]) {
      for (final options in <ParquetWriterOptions>[
        const ParquetWriterOptions(),
        const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
          pageSize: 64,
        ),
        const ParquetWriterOptions(
          compression: CompressionType.zstd,
          pageSize: 128,
        ),
      ]) {
        test(
          '$name (${options.compression.name}, v${options.dataPageVersion})',
          () {
            final source = readFixtureTable(name);
            final reader = writeAndOpen(source, options: options);
            expectTablesEqual(source, reader.readTableSync());
            expectSchemasEqual(source.schema, reader.schema);
          },
        );
      }
    }

    test('multiple add() calls become multiple row groups', () {
      final sourceReader = readFixtureReader('nested_structs_multi.parquet');
      final writer = ParquetWriter(
        schema: sourceReader.schema,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
          pageSize: 64,
        ),
      );
      for (var i = 0; i < sourceReader.numRowGroups; i++) {
        writer.add(sourceReader.readRowGroupSync(i));
      }
      expect(writer.numRowGroups, 3);
      expect(writer.numRows, 120);

      final back = ParquetReader.fromBytes(writer.finish());
      expect(back.numRowGroups, 3);
      expect(back.numRows, 120);
      final groups = [
        for (var i = 0; i < back.numRowGroups; i++)
          back.readRowGroupSync(i).numRows,
      ];
      expect(groups, [50, 50, 20]);
      expectTablesEqual(sourceReader.readTableSync(), back.readTableSync());
    });

    test('list of struct with several pages preserves element order', () {
      final source = readFixtureTable('nested_structs_multi.parquet');
      final reader = writeAndOpen(
        source,
        options: const ParquetWriterOptions(pageSize: 32, dataPageVersion: 2),
      );
      final back = reader.readTableSync();
      expectTablesEqual(source, back);
      final original = source['list_struct'] as ListColumn;
      final decoded = back['list_struct'] as ListColumn;
      for (var i = 0; i < original.length; i++) {
        expect(decoded.getList(i), original.getList(i), reason: 'row $i');
      }
    });

    test('legacy bare repeated leaf is accepted', () {
      final schema = ParquetSchema.fromSchemaElements([
        const SchemaElement(name: 'schema', numChildren: 1),
        const SchemaElement(
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.repeated,
          name: 'vals',
        ),
      ]);
      expect(() => ParquetWriter(schema: schema), returnsNormally);
    });

    test('nested projection still reads written files', () {
      final source = readFixtureTable('nested_snappy.parquet');
      final reader = writeAndOpen(source);
      final projected = reader.readTableSync(columns: ['list_int', 'map']);
      expect(projected.columns.map((c) => c.name), ['list_int', 'map']);
      final list = projected['list_int'] as ListColumn;
      expect(list.getList(1), [0]);
      expect(list.getList(3), [0, 1, 2]);
      final map = projected['map'] as MapColumn;
      expect(map.getMap(2), {'k0': 0, 'k1': 1});
    });

    test('struct nulls survive the roundtrip', () {
      final source = readFixtureTable('nested_snappy.parquet');
      final back = writeAndOpen(source).readTableSync();
      final original = source['struct'] as StructColumn;
      final decoded = back['struct'] as StructColumn;
      for (var i = 0; i < original.length; i++) {
        expect(decoded.isNull(i), original.isNull(i), reason: 'null[$i]');
        if (!original.isNull(i)) {
          expect(decoded.getStruct(i), original.getStruct(i));
        }
      }
    });

    test('rejects a struct column where the schema says list', () {
      final source = readFixtureTable('nested_snappy.parquet');
      final columns = <ParquetColumn>[];
      for (final column in source.columns) {
        if (column.name == 'list_int') {
          columns.add(
            StructColumn(
              name: 'list_int',
              length: source.numRows,
              fields: const <String, ParquetColumn>{},
              validity: Validity(source.numRows),
            ),
          );
        } else {
          columns.add(column);
        }
      }
      final table = ParquetTable(
        schema: source.schema,
        columns: columns,
        numRows: source.numRows,
      );
      expect(
        () => ParquetWriter(schema: source.schema).add(table),
        throwsArgumentError,
      );
    });
  });
}
