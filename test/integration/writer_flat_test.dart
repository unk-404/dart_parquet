@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  group('flat roundtrip', () {
    for (final name in <String>[
      'plain_snappy.parquet',
      'flat_multi_rowgroup.parquet',
      'v2_pages.parquet',
      'dict_snappy.parquet',
      'delta_int.parquet',
      'bss.parquet',
    ]) {
      for (final options in <ParquetWriterOptions>[
        const ParquetWriterOptions(),
        const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
        ),
        const ParquetWriterOptions(
          compression: CompressionType.zstd,
          pageSize: 512,
        ),
      ]) {
        test(
          '$name (${options.compression.name}, v${options.dataPageVersion})',
          () {
            final source = readFixtureTable(name);
            final reader = writeAndOpen(source, options: options);
            final back = reader.readTableSync();
            expectTablesEqual(source, back);
            expect(reader.numRowGroups, 1);
            expect(reader.createdBy, 'parquet-dart version 0.1.1');
            expect(reader.metadata.rowGroup(0).numRows, source.numRows);
          },
        );
      }
    }

    test('single-entry dictionary uses a zero bit width', () {
      final schema = (ParquetSchemaBuilder()..addBinary('s', utf8: true))
          .build();
      const numRows = 100;
      final data = Uint8List(numRows * 4);
      final offsets = Uint32List(numRows + 1);
      for (var i = 0; i < numRows; i++) {
        for (var b = 0; b < 4; b++) {
          data[i * 4 + b] = 'same'.codeUnitAt(b);
        }
        offsets[i + 1] = offsets[i] + 4;
      }
      final table = ParquetTable(
        schema: schema,
        numRows: numRows,
        columns: <ParquetColumn>[
          BinaryColumn(
            name: 's',
            data: data,
            offsets: offsets,
            validity: Validity(numRows),
            isUtf8: true,
          ),
        ],
      );
      final reader = writeAndOpen(table);
      expectTablesEqual(table, reader.readTableSync());
      final chunk = reader.metadata.rowGroup(0).column(0);
      expect(chunk.hasDictionaryPage, isTrue);
      expect(chunk.statistics!.distinctCount, 1);
    });

    test('unsigned integer statistics use unsigned ordering', () {
      final schema = (ParquetSchemaBuilder()..addInt32('u', unsigned: true))
          .build();
      final table = ParquetTable(
        schema: schema,
        numRows: 3,
        columns: <ParquetColumn>[
          Int32Column(
            name: 'u',
            values: Int32List.fromList([-1, 0, 5]),
            validity: Validity(3),
          ),
        ],
      );
      final reader = writeAndOpen(table);
      final stats = reader.metadata.rowGroup(0).column(0).statistics!;
      expect(stats.minValue, [0, 0, 0, 0]);
      expect(stats.maxValue, [255, 255, 255, 255]);
      // Legacy signed min/max must not be written for unsigned columns.
      expect(stats.min, isNull);
      expect(stats.max, isNull);
    });

    test('required and optional primitive columns', () {
      final schema =
          (ParquetSchemaBuilder()
                ..addInt32('req_i32', nullable: false)
                ..addInt64('opt_i64')
                ..addBoolean('req_bool', nullable: false)
                ..addFloat32('f32')
                ..addFloat64('f64')
                ..addBinary('bytes')
                ..addFixedLenByteArray('fixed', 4))
              .build();
      const numRows = 8;
      final optional = Validity(numRows);
      for (var i = 0; i < numRows; i++) {
        if (i.isEven) optional.setNull(i);
      }
      final fixed = Uint8List(numRows * 4);
      final bytes = Uint8List(numRows);
      for (var i = 0; i < numRows; i++) {
        bytes[i] = i;
        for (var b = 0; b < 4; b++) {
          fixed[i * 4 + b] = i + b;
        }
      }
      final table = ParquetTable(
        schema: schema,
        numRows: numRows,
        columns: <ParquetColumn>[
          Int32Column(
            name: 'req_i32',
            values: Int32List.fromList(
              List<int>.generate(numRows, (i) => i - 4),
            ),
            validity: Validity(numRows),
          ),
          Int64Column(
            name: 'opt_i64',
            values: _int64Values(
              List<int>.generate(numRows, (i) => i * 10000000000),
            ),
            validity: optional,
          ),
          BooleanColumn(
            name: 'req_bool',
            bits: Uint8List.fromList([0xAA]),
            validity: Validity(numRows),
          ),
          Float32Column(
            name: 'f32',
            values: Float32List.fromList(
              List<double>.generate(numRows, (i) => i / 2),
            ),
            validity: Validity(numRows),
          ),
          Float64Column(
            name: 'f64',
            values: Float64List.fromList(
              List<double>.generate(numRows, (i) => i / 3),
            ),
            validity: Validity(numRows),
          ),
          BinaryColumn(
            name: 'bytes',
            data: bytes,
            offsets: Uint32List.fromList([
              for (var i = 0; i <= numRows; i++) i,
            ]),
            validity: Validity(numRows),
          ),
          FixedLenColumn(
            name: 'fixed',
            data: fixed,
            width: 4,
            validity: Validity(numRows),
          ),
        ],
      );

      for (final version in <int>[1, 2]) {
        final reader = writeAndOpen(
          table,
          options: ParquetWriterOptions(dataPageVersion: version),
        );
        expectTablesEqual(table, reader.readTableSync());
        expect(reader.schema.columnByPathString('req_i32')!.maxDefLevel, 0);
      }
    });

    test('INT96 values roundtrip', () {
      final schema = (ParquetSchemaBuilder()..addInt96('legacy')).build();
      const numRows = 5;
      final data = Uint8List(numRows * 12);
      final validity = Validity(numRows);
      for (var i = 0; i < numRows; i++) {
        if (i == 3) validity.setNull(i);
        for (var b = 0; b < 12; b++) {
          data[i * 12 + b] = (i * 12 + b) & 0xFF;
        }
      }
      final table = ParquetTable(
        schema: schema,
        numRows: numRows,
        columns: <ParquetColumn>[
          Int96Column(name: 'legacy', data: data, validity: validity),
        ],
      );
      final reader = writeAndOpen(
        table,
        options: const ParquetWriterOptions(dataPageVersion: 2),
      );
      expectTablesEqual(table, reader.readTableSync());
      final chunk = reader.metadata.rowGroup(0).column(0);
      // INT96 statistics are optional; the writer skips them.
      expect(chunk.statistics?.minValue, isNull);
      expect(chunk.statistics?.maxValue, isNull);
      expect(chunk.statistics?.nullCount, 1);
    });

    test('empty table keeps its schema and row group', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final empty = emptyTableFor(source.schema);
      final reader = writeAndOpen(empty);
      final back = reader.readTableSync();
      expect(back.numRows, 0);
      expect(back.columns.length, source.schema.numColumns);
      expect(reader.numRowGroups, 1);
      expect(reader.metadata.rowGroup(0).numRows, 0);
    });

    test('table columns may be reordered relative to the schema', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final reordered = ParquetTable(
        schema: source.schema,
        columns: source.columns.reversed.toList(),
        numRows: source.numRows,
      );
      final back = writeAndOpen(reordered).readTableSync();
      expect(
        back.columns.map((column) => column.name),
        source.columns.map((column) => column.name),
      );
      expectTablesEqual(source, back);
    });
  });

  group('flat statistics', () {
    test('nullable int32 null count and min/max', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(source);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['nullable_i32'])!;
      final stats = chunk.statistics!;
      expect(stats.nullCount, 143);
      expect(stats.isMinValueExact, isTrue);
      expect(stats.isMaxValueExact, isTrue);
      expect(stats.min, isNotNull); // deprecated fields present
      expect(stats.max, isNotNull);
      expect(_int32(stats.minValue!), 1);
      expect(_int32(stats.maxValue!), 999);
    });

    test('byte array statistics are lexicographic raw bytes', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(source);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['str'])!;
      final stats = chunk.statistics!;
      expect(String.fromCharCodes(stats.minValue!), 'value-00000');
      expect(String.fromCharCodes(stats.maxValue!), 'value-00999');
      expect(stats.distinctCount, 1000);
      // Legacy min/max are not written for byte arrays (signed ordering).
      expect(stats.min, isNull);
      expect(stats.max, isNull);
    });

    test('floating point statistics', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(source);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['f64'])!;
      final stats = chunk.statistics!;
      expect(_float64(stats.minValue!), 0.0);
      expect(_float64(stats.maxValue!), 333.0);
      final boolChunk = reader.metadata.rowGroup(0).columnByPath(['bool'])!;
      expect(boolChunk.statistics!.minValue, [0]);
      expect(boolChunk.statistics!.maxValue, [1]);
    });

    test('nan values never become min/max', () {
      final schema = (ParquetSchemaBuilder()..addFloat64('v')).build();
      final table = ParquetTable(
        schema: schema,
        numRows: 4,
        columns: <ParquetColumn>[
          Float64Column(
            name: 'v',
            values: Float64List.fromList([double.nan, double.nan, 2.0, 1.0]),
            validity: Validity(4),
          ),
        ],
      );
      final reader = writeAndOpen(table);
      final stats = reader.metadata.rowGroup(0).column(0).statistics!;
      expect(_float64(stats.minValue!), 1.0);
      expect(_float64(stats.maxValue!), 2.0);

      final allNan = ParquetTable(
        schema: schema,
        numRows: 2,
        columns: <ParquetColumn>[
          Float64Column(
            name: 'v',
            values: Float64List.fromList([double.nan, double.nan]),
            validity: Validity(2),
          ),
        ],
      );
      final nanReader = writeAndOpen(allNan);
      final nanStats = nanReader.metadata.rowGroup(0).column(0).statistics!;
      expect(nanStats.minValue, isNull);
      expect(nanStats.maxValue, isNull);
      expect(nanStats.nullCount, 0);
    });

    test('writeStatistics false omits chunk statistics', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(
        source,
        options: const ParquetWriterOptions(writeStatistics: false),
      );
      for (var i = 0; i < reader.schema.numColumns; i++) {
        expect(
          reader.metadata.rowGroup(0).column(i).statistics,
          isNull,
          reason: 'column $i',
        );
      }
    });
  });

  group('writer validation', () {
    test('rejects a mismatched column count', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final table = ParquetTable(
        schema: source.schema,
        columns: source.columns.sublist(1),
        numRows: source.numRows,
      );
      expect(
        () => ParquetWriter(schema: source.schema).add(table),
        throwsArgumentError,
      );
    });

    test('rejects a mismatched column name', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final columns = source.columns.toList();
      columns[0] = Int64Column(
        name: 'not_i64',
        values: Int64Buffer(source.numRows),
        validity: Validity(source.numRows),
      );
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

    test('rejects a column with the wrong physical type', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final columns = source.columns.toList();
      columns[0] = Int32Column(
        name: 'i64',
        values: Int32List(source.numRows),
        validity: Validity(source.numRows),
      );
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

    test('rejects a mismatched row count', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final table = ParquetTable(
        schema: source.schema,
        columns: source.columns,
        numRows: source.numRows + 1,
      );
      expect(
        () => ParquetWriter(schema: source.schema).add(table),
        throwsArgumentError,
      );
    });

    test('rejects nulls in required columns', () {
      final schema = (ParquetSchemaBuilder()..addInt32('v', nullable: false))
          .build();
      final validity = Validity(2)..setNull(1);
      final table = ParquetTable(
        schema: schema,
        numRows: 2,
        columns: <ParquetColumn>[
          Int32Column(
            name: 'v',
            values: Int32List.fromList([1, 2]),
            validity: validity,
          ),
        ],
      );
      expect(() => writeParquetTable(table), throwsArgumentError);
    });

    test('finish can only be called once and add after finish fails', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final writer = ParquetWriter(schema: source.schema);
      writer.add(source);
      expect(writer.finish(), isNotEmpty);
      expect(() => writer.finish(), throwsStateError);
      expect(() => writer.add(source), throwsStateError);
    });
  });
}

Int64Buffer _int64Values(List<int> values) {
  final buffer = Int64Buffer(values.length);
  for (var i = 0; i < values.length; i++) {
    buffer.setInt(i, values[i]);
  }
  return buffer;
}

int _int32(Uint8List bytes) {
  final data = ByteData.sublistView(bytes);
  return data.getInt32(0, Endian.little);
}

double _float64(Uint8List bytes) {
  final data = ByteData.sublistView(bytes);
  return data.getFloat64(0, Endian.little);
}
