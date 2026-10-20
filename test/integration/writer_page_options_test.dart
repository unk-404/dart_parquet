@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/compression/registry.dart';
import 'package:parquet/src/encodings/levels.dart';
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  group('data page versions', () {
    for (final version in <int>[1, 2]) {
      test('v$version headers, sizes and level layout', () {
        final source = readFixtureTable('plain_snappy.parquet');
        final bytes = writeParquetTable(
          source,
          options: ParquetWriterOptions(
            compression: CompressionType.snappy,
            dataPageVersion: version,
            pageSize: 512,
          ),
        );
        final reader = ParquetReader.fromBytes(bytes);
        final group = reader.metadata.rowGroup(0);
        var totalPages = 0;
        for (var i = 0; i < reader.schema.numColumns; i++) {
          final chunk = group.column(i);
          final pages = _readPages(bytes, chunk);
          totalPages += pages.length;
          expect(pages, isNotEmpty);
          for (final page in pages) {
            if (version == 1) {
              expect(page.header.type, PageType.dataPage);
              final header = page.header.dataPageHeader!;
              expect(header.definitionLevelEncoding, Encoding.rle);
              expect(header.repetitionLevelEncoding, Encoding.rle);
              expect(header.statistics, isNotNull);
              // The body starts with a 4-byte definition-level length (and a
              // repetition-level length first for nested columns).
              final raw = codecFor(chunk.compression).decode(
                page.body,
                uncompressedSize: page.header.uncompressedPageSize,
              );
              final body = ByteReader(raw);
              if (chunk.descriptor!.maxRepLevel > 0) {
                final repLength = body.readUint32LE();
                expect(repLength, lessThanOrEqualTo(body.remaining));
                body.skip(repLength);
              }
              if (chunk.descriptor!.maxDefLevel > 0) {
                final defLength = body.readUint32LE();
                expect(defLength, lessThanOrEqualTo(body.remaining));
              }
            } else {
              expect(page.header.type, PageType.dataPageV2);
              final header = page.header.dataPageHeaderV2!;
              expect(header.numValues, greaterThanOrEqualTo(0));
              expect(header.numNulls, lessThanOrEqualTo(header.numValues));
              expect(header.numRows, greaterThan(0));
              expect(header.isCompressed, isTrue);
              expect(header.statistics, isNotNull);
              expect(
                header.repetitionLevelsByteLength +
                    header.definitionLevelsByteLength,
                lessThanOrEqualTo(page.header.compressedPageSize),
              );
            }
          }
        }
        expect(totalPages, greaterThan(reader.schema.numColumns));
      });
    }

    test('small page sizes produce multiple pages', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(
        source,
        options: const ParquetWriterOptions(pageSize: 256),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['i64'])!;
      expect(_readPages(bytes, chunk).length, greaterThan(1));
    });

    test('nested pages never split a row', () {
      final source = readFixtureTable('nested_snappy.parquet');
      for (final version in <int>[1, 2]) {
        final bytes = writeParquetTable(
          source,
          options: ParquetWriterOptions(
            compression: CompressionType.snappy,
            dataPageVersion: version,
            pageSize: 16,
          ),
        );
        final reader = ParquetReader.fromBytes(bytes);
        final group = reader.metadata.rowGroup(0);
        var sawNestedPages = 0;
        for (var i = 0; i < reader.schema.numColumns; i++) {
          final chunk = group.column(i);
          final maxRep = chunk.descriptor!.maxRepLevel;
          if (maxRep == 0) continue;
          final pages = _readPages(bytes, chunk);
          var totalValues = 0;
          for (final page in pages) {
            final numValues = version == 1
                ? page.header.dataPageHeader!.numValues
                : page.header.dataPageHeaderV2!.numValues;
            totalValues += numValues;
            final repLevels = _pageRepLevels(page, chunk, maxRep, version);
            expect(repLevels, isNotNull);
            expect(repLevels!.length, numValues);
            expect(
              repLevels[0],
              0,
              reason: 'page must start at a row boundary',
            );
            if (version == 2) {
              expect(
                page.header.dataPageHeaderV2!.numRows,
                repLevels.where((level) => level == 0).length,
              );
            }
            sawNestedPages++;
          }
          expect(totalValues, chunk.numValues);
        }
        expect(sawNestedPages, greaterThan(0));
      }
    });
  });

  group('dictionary options', () {
    test('dictionary encoding is on by default', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(source);
      final stringChunk = reader.metadata.rowGroup(0).columnByPath(['str'])!;
      expect(stringChunk.hasDictionaryPage, isTrue);
      expect(stringChunk.isDictionaryEncoded, isTrue);
      expect(stringChunk.encodings, contains(Encoding.rleDictionary));
      expect(
        stringChunk.dataPageOffset,
        greaterThan(stringChunk.dictionaryPageOffset!),
      );
    });

    test('useDictionary: false falls back to PLAIN', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(
        source,
        options: const ParquetWriterOptions(useDictionary: false),
      );
      for (var i = 0; i < reader.schema.numColumns; i++) {
        final chunk = reader.metadata.rowGroup(0).column(i);
        expect(chunk.hasDictionaryPage, isFalse, reason: 'column $i');
        expect(chunk.encodings, isNot(contains(Encoding.rleDictionary)));
        expect(chunk.encodings, contains(Encoding.plain));
      }
    });

    test('oversized dictionary falls back to PLAIN', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final reader = writeAndOpen(
        source,
        options: const ParquetWriterOptions(dictionaryPageSizeLimit: 8),
      );
      final stringChunk = reader.metadata.rowGroup(0).columnByPath(['str'])!;
      expect(stringChunk.hasDictionaryPage, isFalse);
      expect(stringChunk.encodings, isNot(contains(Encoding.rleDictionary)));
      expectTablesEqual(source, reader.readTableSync());
    });

    test('dictionary pages carry chunk and page statistics', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(source);
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['str'])!;
      expect(chunk.statistics!.nullCount, 0);
      expect(chunk.statistics!.distinctCount, 1000);
      for (final page in _readPages(bytes, chunk)) {
        final stats = page.header.dataPageHeader!.statistics!;
        expect(stats.nullCount, 0);
        expect(stats.minValue, isNotNull);
        expect(stats.maxValue, isNotNull);
      }
    });
  });

  group('file metadata', () {
    test('createdBy and key/value metadata', () {
      final source = readFixtureTable('flat_multi_rowgroup.parquet');
      final reader = writeAndOpen(
        source,
        options: const ParquetWriterOptions(
          createdBy: 'test-writer version 9.9',
          keyValueMetadata: <String, String>{'team': 'parquet', 'a': 'b'},
        ),
      );
      expect(reader.createdBy, 'test-writer version 9.9');
      final metadata = reader.metadata.fileMetadata;
      expect(metadata.keyValueMetadata!.length, 2);
      expect(metadata.lookupMetadata('team'), 'parquet');
      expect(metadata.lookupMetadata('a'), 'b');
      expect(metadata.lookupMetadata('missing'), isNull);
      expect(reader.version, 1);
    });

    test('multiple add() calls create ordered row groups', () {
      final sourceReader = readFixtureReader('flat_multi_rowgroup.parquet');
      final writer = ParquetWriter(
        schema: sourceReader.schema,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
        ),
      );
      final counts = <int>[];
      for (var i = 0; i < sourceReader.numRowGroups; i++) {
        final group = sourceReader.readRowGroupSync(i);
        counts.add(group.numRows);
        writer.add(group);
      }
      final reader = ParquetReader.fromBytes(writer.finish());
      expect(reader.numRowGroups, sourceReader.numRowGroups);
      expect(reader.numRows, counts.fold<int>(0, (a, b) => a + b));
      final group = reader.metadata.rowGroup(0);
      expect(group.fileOffset, 4);
      expect(group.ordinal, 0);
      expect(reader.metadata.rowGroup(1).ordinal, 1);
      expect(group.totalByteSize, greaterThan(0));
      expect(group.totalCompressedSize, greaterThan(0));
      expect(
        group.totalByteSize,
        group.columns.fold<int>(0, (a, c) => a + c.totalUncompressedSize),
      );
    });

    test('row group offsets are absolute and chunks are contiguous', () {
      final source = readFixtureTable('nested_structs_multi.parquet');
      final bytes = writeParquetTable(
        source,
        options: const ParquetWriterOptions(pageSize: 64),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final fileLength = bytes.length;
      for (var rg = 0; rg < reader.numRowGroups; rg++) {
        final group = reader.metadata.rowGroup(rg);
        var expected = group.fileOffset!;
        for (final chunk in group.columns) {
          final start =
              chunk.dictionaryPageOffset ?? chunk.dataPageOffset ?? -1;
          expect(start, greaterThanOrEqualTo(4));
          expect(start, expected, reason: 'chunks must be contiguous');
          final end = start + chunk.totalCompressedSize;
          expect(end, lessThan(fileLength));
          expected = end;
        }
      }
    });
  });
}

List<_ParsedPage> _readPages(Uint8List bytes, ColumnChunkMetaData chunk) {
  final start = chunk.dictionaryPageOffset ?? chunk.dataPageOffset!;
  final data = MemorySource.fromUint8List(bytes)
      .read(start, chunk.totalCompressedSize);
  final input = ByteReader(data);
  final pages = <_ParsedPage>[];
  while (input.remaining > 0) {
    final header = readPageHeader(input);
    final body = input.readSlice(header.compressedPageSize);
    if (header.type == PageType.dataPage ||
        header.type == PageType.dataPageV2) {
      pages.add(_ParsedPage(header, body));
    }
  }
  return pages;
}

Uint8List? _pageRepLevels(
  _ParsedPage page,
  ColumnChunkMetaData chunk,
  int maxRep,
  int version,
) {
  if (version == 2) {
    final header = page.header.dataPageHeaderV2!;
    final repLength = header.repetitionLevelsByteLength;
    return decodeLevels(
      page.body.slice(0, repLength),
      maxLevel: maxRep,
      numValues: header.numValues,
      encoding: Encoding.rle,
    );
  }
  final header = page.header.dataPageHeader!;
  final raw = codecFor(chunk.compression)
      .decode(page.body, uncompressedSize: page.header.uncompressedPageSize);
  final body = ByteReader(raw);
  final repLength = body.readUint32LE();
  final repBytes = body.readSlice(repLength);
  return decodeLevels(
    repBytes,
    maxLevel: maxRep,
    numValues: header.numValues,
    encoding: Encoding.rle,
  );
}

final class _ParsedPage {
  const _ParsedPage(this.header, this.body);

  final PageHeader header;
  final Bytes body;
}
