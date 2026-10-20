@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/src/format/footer.dart';
import 'package:parquet/src/format/parquet_format.dart';
import 'package:parquet/src/metadata/metadata.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:parquet/src/util/errors.dart';
import 'package:test/test.dart';

Bytes fixtureBytes(String name) =>
    Bytes.fromUint8List(File('test/fixtures/$name').readAsBytesSync());

ParquetMetadata metadataFor(String name) =>
    ParquetMetadata(parseFooter(fixtureBytes(name)).fileMetadata);

const _fixtureCodecs = <String, CompressionType>{
  'plain_uncompressed.parquet': CompressionType.uncompressed,
  'plain_snappy.parquet': CompressionType.snappy,
  'plain_gzip.parquet': CompressionType.gzip,
  'plain_brotli.parquet': CompressionType.brotli,
  'plain_zstd.parquet': CompressionType.zstd,
  'plain_lz4.parquet': CompressionType.lz4Raw,
  'dict_snappy.parquet': CompressionType.snappy,
  'v2_pages.parquet': CompressionType.snappy,
  'delta_int.parquet': CompressionType.uncompressed,
  'delta_str.parquet': CompressionType.uncompressed,
  'delta_len_str.parquet': CompressionType.uncompressed,
  'bss.parquet': CompressionType.uncompressed,
  'nested_snappy.parquet': CompressionType.snappy,
  'nested_uncompressed.parquet': CompressionType.uncompressed,
};

int _expectedRows(String fixture) => fixture.startsWith('nested') ? 200 : 1000;

int _expectedColumns(String fixture) => fixture.startsWith('nested') ? 6 : 9;

void main() {
  group('all fixtures', () {
    for (final entry in _fixtureCodecs.entries) {
      test('parses ${entry.key}', () {
        final file = fixtureBytes(entry.key);
        final footer = parseFooter(file);
        final metadata = ParquetMetadata(footer.fileMetadata);

        expect(metadata.numRows, _expectedRows(entry.key));
        expect(metadata.numRowGroups, 1);
        expect(metadata.rowGroups, hasLength(1));
        expect(metadata.schema.numColumns, _expectedColumns(entry.key));
        expect(metadata.version, 2);
        expect(metadata.createdBy, contains('parquet-cpp-arrow'));

        final rowGroup = metadata.rowGroups.single;
        expect(rowGroup.numRows, metadata.numRows);
        expect(rowGroup.numColumns, metadata.schema.numColumns);
        expect(rowGroup.index, 0);
        expect(rowGroup.totalByteSize, greaterThan(0));

        var rowSum = 0;
        for (final column in rowGroup.columns) {
          rowSum += column.numValues;
          expect(column.compression, entry.value);
          expect(column.descriptor, isNotNull);
          expect(column.path, column.descriptor!.path);
          expect(column.dotPath, column.path.join('.'));
          expect(column.type, column.descriptor!.physicalType);
          expect(column.numValues, greaterThan(0));
          expect(column.totalCompressedSize, greaterThan(0));
          expect(column.totalUncompressedSize, greaterThan(0));
          expect(column.hasDataPage, isTrue);
          expect(column.startOffset, greaterThanOrEqualTo(4));
          expect(column.endOffset, lessThanOrEqualTo(file.length - 8));
        }
        expect(rowSum, greaterThanOrEqualTo(metadata.numRows));
      });
    }
  });

  group('flat file metadata', () {
    late ParquetMetadata metadata;
    late RowGroupMetaData rowGroup;

    setUpAll(() {
      metadata = metadataFor('plain_lz4.parquet');
      rowGroup = metadata.rowGroups.single;
    });

    test('exposes file and row group totals', () {
      expect(metadata.numRows, 1000);
      expect(metadata.numRowGroups, 1);
      expect(metadata.schema.numColumns, 9);
      expect(rowGroup.numRows, 1000);
      expect(rowGroup.totalByteSize, greaterThan(0));
      expect(
        ParquetMetadata.fromFileMetaData(metadata.fileMetadata).numRows,
        1000,
      );
      expect(metadata.rowGroup(0), same(rowGroup));
      expect(metadata.columnChunk(0, 1), same(rowGroup.column(1)));
    });

    test('describes the first column chunk', () {
      final column = rowGroup.column(0);

      expect(column.type, PhysicalType.int64);
      expect(column.physicalType, PhysicalType.int64);
      expect(column.compression, CompressionType.lz4Raw);
      expect(column.path, ['i64']);
      expect(column.numValues, 1000);
      expect(column.dataPageOffset, 4);
      expect(column.indexPageOffset, isNull);
      expect(column.dictionaryPageOffset, isNull);
      expect(column.hasDictionaryPage, isFalse);
      expect(column.hasDataPage, isTrue);
      expect(column.totalUncompressedSize, 8073);
      expect(column.totalCompressedSize, 4223);
      expect(column.startOffset, 4);
      expect(column.endOffset, 4227);
      expect(column.encodings, containsAll([Encoding.rle, Encoding.plain]));
      expect(column.isDictionaryEncoded, isFalse);
      expect(column.encodingStats, hasLength(1));
      expect(column.encodingStats!.single.pageType, PageType.dataPage);
      expect(column.encodingStats!.single.encoding, Encoding.plain);
      expect(column.encodingStats!.single.count, 1);
      expect(column.descriptor!.leafIndex, 0);
    });

    test('keeps raw statistics', () {
      final statistics = rowGroup.column(0).statistics;
      expect(statistics, isNotNull);
      expect(statistics!.nullCount, 0);
      expect(statistics.maxValue, hasLength(8));
      expect(statistics.minValue, hasLength(8));
      expect(statistics.max, hasLength(8));
      expect(statistics.min, hasLength(8));
      expect(statistics.isMaxValueExact, isTrue);
      expect(statistics.isMinValueExact, isTrue);
      expect(statistics.hasMinMax, isTrue);
      expect(statistics.effectiveMax, statistics.maxValue);
      expect(statistics.effectiveMin, statistics.minValue);
      expect(statistics.nanCount, isNull);
    });

    test('exposes nullable statistics', () {
      final statistics = rowGroup.column(2).statistics;
      expect(statistics, isNotNull);
      expect(statistics!.nullCount, 143);
      expect(rowGroup.column(2).path, ['nullable_i32']);
    });

    test('resolves row group columns by path and index', () {
      expect(rowGroup.columnByPath(['i64']), same(rowGroup.column(0)));
      expect(rowGroup.columnByPath(['nope']), isNull);
      expect(() => rowGroup.column(9), throwsA(isA<ParquetRangeException>()));
      expect(() => metadata.rowGroup(1), throwsA(isA<ParquetRangeException>()));
      expect(
        () => metadata.columnChunk(0, 9),
        throwsA(isA<ParquetRangeException>()),
      );
    });
  });

  group('dictionary encoding', () {
    test('v2_pages has a dictionary page and a v2 data page', () {
      final metadata = metadataFor('v2_pages.parquet');
      final column = metadata.rowGroups.single.column(0);

      expect(column.compression, CompressionType.snappy);
      expect(column.hasDictionaryPage, isTrue);
      expect(column.dictionaryPageOffset, 4);
      expect(column.dataPageOffset, 4062);
      expect(column.startOffset, 4);
      expect(
        column.encodings,
        containsAll([Encoding.plain, Encoding.rle, Encoding.rleDictionary]),
      );
      expect(column.isDictionaryEncoded, isTrue);
      expect(column.encodingStats, hasLength(2));
      expect(column.encodingStats![0].pageType, PageType.dictionaryPage);
      expect(column.encodingStats![0].encoding, Encoding.plain);
      expect(column.encodingStats![0].count, 1);
      expect(column.encodingStats![1].pageType, PageType.dataPage);
      expect(column.encodingStats![1].encoding, Encoding.rleDictionary);
      expect(column.encodingStats![1].count, 1);
    });

    test('dict_snappy has a dictionary page', () {
      final column = metadataFor('dict_snappy.parquet').rowGroups.single
          .column(0);
      expect(column.hasDictionaryPage, isTrue);
      expect(column.dictionaryPageOffset, 4);
      expect(column.dataPageOffset, 4062);
      expect(column.startOffset, 4);
      expect(column.isDictionaryEncoded, isTrue);
    });
  });

  group('encodings', () {
    test('delta_int uses DELTA_BINARY_PACKED', () {
      final column = metadataFor('delta_int.parquet').rowGroups.single
          .column(0);
      expect(column.encodings, contains(Encoding.deltaBinaryPacked));
      expect(column.encodingStats!.single.encoding, Encoding.deltaBinaryPacked);
      expect(column.encodingStats!.single.pageType, PageType.dataPage);
    });

    test('delta_str uses DELTA_BYTE_ARRAY', () {
      final column = metadataFor('delta_str.parquet').rowGroups.single
          .column(1);
      expect(column.encodings, contains(Encoding.deltaByteArray));
      expect(column.encodingStats!.single.encoding, Encoding.deltaByteArray);
    });

    test('delta_len_str uses DELTA_LENGTH_BYTE_ARRAY', () {
      final column = metadataFor('delta_len_str.parquet').rowGroups.single
          .column(1);
      expect(column.encodings, contains(Encoding.deltaLengthByteArray));
      expect(
        column.encodingStats!.single.encoding,
        Encoding.deltaLengthByteArray,
      );
    });

    test('bss uses BYTE_STREAM_SPLIT', () {
      final rowGroup = metadataFor('bss.parquet').rowGroups.single;
      expect(rowGroup.column(0).encodings, contains(Encoding.byteStreamSplit));
      expect(rowGroup.column(4).encodings, contains(Encoding.byteStreamSplit));
      expect(
        rowGroup.column(0).encodingStats!.single.encoding,
        Encoding.byteStreamSplit,
      );
    });
  });

  group('nested metadata', () {
    late ParquetMetadata metadata;

    setUpAll(() {
      metadata = metadataFor('nested_snappy.parquet');
    });

    test('binds chunks to nested schema leaves', () {
      expect(metadata.numRows, 200);
      expect(metadata.schema.numColumns, 6);
      final rowGroup = metadata.rowGroups.single;
      expect(rowGroup.numColumns, 6);
      expect(rowGroup.columns.map((column) => column.path).toList(), [
        ['id'],
        ['list_int', 'list', 'element'],
        ['struct', 'a'],
        ['struct', 'b'],
        ['map', 'key_value', 'key'],
        ['map', 'key_value', 'value'],
      ]);
      expect(rowGroup.column(1).descriptor!.maxDefLevel, 3);
      expect(rowGroup.column(1).descriptor!.maxRepLevel, 1);
      expect(rowGroup.column(1).statistics!.nullCount, 50);
      expect(
        rowGroup.column(3).descriptor!.physicalType,
        PhysicalType.byteArray,
      );
      expect(
        rowGroup.column(4).descriptor!.repetition,
        FieldRepetitionType.required,
      );
      expect(rowGroup.column(5).descriptor!.maxDefLevel, 3);
      expect(rowGroup.column(5).descriptor!.maxRepLevel, 1);
      expect(rowGroup.column(4).compression, CompressionType.snappy);
    });
  });

  group('validation', () {
    const schemaElements = [
      SchemaElement(name: 'schema', numChildren: 1),
      SchemaElement(
        name: 'x',
        type: PhysicalType.int32,
        repetitionType: FieldRepetitionType.optional,
      ),
    ];

    const columnMetaData = ColumnMetaData(
      type: PhysicalType.int32,
      encodings: [Encoding.plain],
      pathInSchema: ['x'],
      codec: CompressionType.uncompressed,
      numValues: 0,
      totalUncompressedSize: 0,
      totalCompressedSize: 0,
    );

    const fileMetadata = FileMetaData(
      version: 1,
      schema: schemaElements,
      numRows: 0,
      rowGroups: [
        RowGroup(
          columns: [ColumnChunk(metaData: columnMetaData)],
          totalByteSize: 0,
          numRows: 0,
        ),
      ],
    );

    test('reports missing page offsets lazily', () {
      final metadata = ParquetMetadata(fileMetadata);
      final column = metadata.columnChunk(0, 0);
      expect(column.hasDataPage, isFalse);
      expect(column.hasDictionaryPage, isFalse);
      expect(column.descriptor, isNotNull);
      expect(() => column.startOffset, throwsA(isA<ParquetFormatException>()));
    });

    test('rejects row groups with a different column count', () {
      const badMetadata = FileMetaData(
        version: 1,
        schema: schemaElements,
        numRows: 0,
        rowGroups: [RowGroup(columns: [], totalByteSize: 0, numRows: 0)],
      );
      expect(
        () => ParquetMetadata(badMetadata),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects column chunks whose path is not in the schema', () {
      const wrongPathMetaData = ColumnMetaData(
        type: PhysicalType.int32,
        encodings: [Encoding.plain],
        pathInSchema: ['nope'],
        codec: CompressionType.uncompressed,
        numValues: 0,
        totalUncompressedSize: 0,
        totalCompressedSize: 0,
      );
      const badMetadata = FileMetaData(
        version: 1,
        schema: schemaElements,
        numRows: 0,
        rowGroups: [
          RowGroup(
            columns: [ColumnChunk(metaData: wrongPathMetaData)],
            totalByteSize: 0,
            numRows: 0,
          ),
        ],
      );
      expect(
        () => ParquetMetadata(badMetadata),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });
}
