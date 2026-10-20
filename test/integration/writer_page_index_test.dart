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
  group('page index roundtrip', () {
    for (final name in <String>[
      'plain_snappy.parquet',
      'nested_snappy.parquet',
    ]) {
      for (final version in <int>[1, 2]) {
        for (final codec in CompressionType.values) {
          test('$name v$version ${codec.name}', () {
            final source = readFixtureTable(name);
            final bytes = writeParquetTable(
              source,
              options: ParquetWriterOptions(
                compression: codec,
                dataPageVersion: version,
                pageSize: name.startsWith('nested') ? 16 : 128,
                writePageIndex: true,
              ),
            );
            final reader = ParquetReader.fromBytes(bytes);
            final memory = MemorySource.fromUint8List(bytes);
            expectTablesEqual(source, reader.readTableSync());

            for (var rg = 0; rg < reader.numRowGroups; rg++) {
              final group = reader.metadata.rowGroup(rg);
              for (var c = 0; c < reader.schema.numColumns; c++) {
                final chunk = group.column(c);
                final offsetIndex = chunk.readOffsetIndex(memory);
                final columnIndex = chunk.readColumnIndex(memory);
                expect(
                  offsetIndex,
                  isNotNull,
                  reason: '$name rg$rg col$c offset index',
                );
                expect(
                  columnIndex,
                  isNotNull,
                  reason: '$name rg$rg col$c column index',
                );
                _validateChunk(
                  chunk,
                  group.numRows,
                  offsetIndex!,
                  columnIndex!,
                );
              }
            }

            // At least one chunk (and the repeated leaf of the nested file)
            // must actually have multiple pages.
            final firstChunk = reader.metadata.rowGroup(0).column(0);
            final firstIndex = firstChunk.readOffsetIndex(memory)!;
            expect(firstIndex.pageLocations.length, greaterThan(1));
            if (name.startsWith('nested')) {
              final listChunk = reader.metadata.rowGroup(0).columnByPath([
                'list_int',
                'list',
                'element',
              ])!;
              final listIndex = listChunk.readOffsetIndex(memory)!;
              expect(listIndex.pageLocations.length, greaterThan(1));
            }
          });
        }
      }
    }

    test('multiple row groups each carry their own page index', () {
      final sourceReader = readFixtureReader('flat_multi_rowgroup.parquet');
      final writer = ParquetWriter(
        schema: sourceReader.schema,
        options: const ParquetWriterOptions(
          pageSize: 128,
          writePageIndex: true,
        ),
      );
      for (var i = 0; i < sourceReader.numRowGroups; i++) {
        writer.add(sourceReader.readRowGroupSync(i));
      }
      final bytes = writer.finish();
      final reader = ParquetReader.fromBytes(bytes);
      final memory = MemorySource.fromUint8List(bytes);
      expect(reader.numRowGroups, sourceReader.numRowGroups);
      for (var rg = 0; rg < reader.numRowGroups; rg++) {
        final group = reader.metadata.rowGroup(rg);
        for (var c = 0; c < reader.schema.numColumns; c++) {
          final chunk = group.column(c);
          final offsetIndex = chunk.readOffsetIndex(memory)!;
          final columnIndex = chunk.readColumnIndex(memory)!;
          _validateChunk(chunk, group.numRows, offsetIndex, columnIndex);
        }
      }
    });

    test('column index min/max match the flat page contents', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(
        source,
        options: const ParquetWriterOptions(
          pageSize: 128,
          writePageIndex: true,
        ),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final memory = MemorySource.fromUint8List(bytes);
      final group = reader.metadata.rowGroup(0);

      final intChunk = group.columnByPath(['nullable_i32'])!;
      final intIndex = intChunk.readColumnIndex(memory)!;
      final intLocations = intChunk.readOffsetIndex(memory)!.pageLocations;
      final intValues = source['nullable_i32'] as Int32Column;
      for (var p = 0; p < intLocations.length; p++) {
        final start = intLocations[p].firstRowIndex;
        final end = p + 1 < intLocations.length
            ? intLocations[p + 1].firstRowIndex
            : source.numRows;
        var nulls = 0;
        int? min;
        int? max;
        for (var i = start; i < end; i++) {
          if (intValues.isNull(i)) {
            nulls++;
            continue;
          }
          final value = intValues.getInt(i);
          min = min == null || value < min ? value : min;
          max = max == null || value > max ? value : max;
        }
        expect(intIndex.nullPages[p], min == null);
        expect(intIndex.nullCounts![p], nulls);
        if (min != null) {
          expect(_int32(intIndex.minValues[p]), min);
          expect(_int32(intIndex.maxValues[p]), max);
        } else {
          expect(intIndex.minValues[p], isEmpty);
          expect(intIndex.maxValues[p], isEmpty);
        }
      }

      final stringChunk = group.columnByPath(['str'])!;
      final stringIndex = stringChunk.readColumnIndex(memory)!;
      final stringLocations = stringChunk
          .readOffsetIndex(memory)!
          .pageLocations;
      final strings = source['str'] as BinaryColumn;
      for (var p = 0; p < stringLocations.length; p++) {
        final start = stringLocations[p].firstRowIndex;
        final end = p + 1 < stringLocations.length
            ? stringLocations[p + 1].firstRowIndex
            : source.numRows;
        var min = strings.getString(start);
        var max = min;
        for (var i = start + 1; i < end; i++) {
          final value = strings.getString(i);
          if (value.compareTo(min) < 0) min = value;
          if (value.compareTo(max) > 0) max = value;
        }
        expect(String.fromCharCodes(stringIndex.minValues[p]), min);
        expect(String.fromCharCodes(stringIndex.maxValues[p]), max);
      }
    });

    test('all-null pages are marked and get empty bounds', () {
      final schema = (ParquetSchemaBuilder()..addInt32('v')).build();
      final table = ParquetTable(
        schema: schema,
        numRows: 100,
        columns: <ParquetColumn>[
          Int32Column(
            name: 'v',
            values: Int32List(100),
            validity: Validity(100, allValid: false),
          ),
        ],
      );
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(pageSize: 64, writePageIndex: true),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final memory = MemorySource.fromUint8List(bytes);
      final chunk = reader.metadata.rowGroup(0).column(0);
      final offsetIndex = chunk.readOffsetIndex(memory)!;
      final columnIndex = chunk.readColumnIndex(memory)!;
      expect(offsetIndex.pageLocations.length, 2);
      expect(columnIndex.nullPages, [true, true]);
      expect(columnIndex.minValues[0], isEmpty);
      expect(columnIndex.minValues[1], isEmpty);
      expect(columnIndex.maxValues[0], isEmpty);
      expect(columnIndex.maxValues[1], isEmpty);
      expect(columnIndex.nullCounts, [64, 36]);
      expectTablesEqual(table, reader.readTableSync());
    });

    test('boundary order reflects page min/max ordering', () {
      final schema = (ParquetSchemaBuilder()..addInt32('v')).build();
      const options = ParquetWriterOptions(pageSize: 64, writePageIndex: true);

      BoundaryOrder boundaryFor(List<int> values) {
        final table = ParquetTable(
          schema: schema,
          numRows: values.length,
          columns: <ParquetColumn>[
            Int32Column(
              name: 'v',
              values: Int32List.fromList(values),
              validity: Validity(values.length),
            ),
          ],
        );
        final bytes = writeParquetTable(table, options: options);
        final reader = ParquetReader.fromBytes(bytes);
        return reader.metadata
            .rowGroup(0)
            .column(0)
            .readColumnIndex(MemorySource.fromUint8List(bytes))!
            .boundaryOrder;
      }

      expect(
        boundaryFor(List<int>.generate(100, (i) => i)),
        BoundaryOrder.ascending,
      );
      expect(
        boundaryFor(List<int>.generate(100, (i) => 100 - i)),
        BoundaryOrder.descending,
      );
      final mixed = <int>[
        for (var i = 0; i < 64; i++) i,
        for (var i = 0; i < 36; i++) i.isEven ? -100 : 200,
      ];
      expect(boundaryFor(mixed), BoundaryOrder.unordered);
    });

    test('first_row_index matches decoded repetition levels', () {
      final source = readFixtureTable('nested_snappy.parquet');
      for (final version in <int>[1, 2]) {
        final bytes = writeParquetTable(
          source,
          options: ParquetWriterOptions(
            compression: CompressionType.snappy,
            dataPageVersion: version,
            pageSize: 16,
            writePageIndex: true,
          ),
        );
        final reader = ParquetReader.fromBytes(bytes);
        final memory = MemorySource.fromUint8List(bytes);
        final group = reader.metadata.rowGroup(0);
        for (var c = 0; c < reader.schema.numColumns; c++) {
          final chunk = group.column(c);
          final maxRep = chunk.descriptor!.maxRepLevel;
          final location = chunk.readOffsetIndex(memory)!.pageLocations;
          var rows = 0;
          for (final entry in location) {
            expect(entry.firstRowIndex, rows);
            final data = memory.read(entry.offset, entry.compressedPageSize);
            final input = ByteReader(data);
            final header = readPageHeader(input);
            final body = input.readSlice(header.compressedPageSize);
            final numValues =
                header.dataPageHeader?.numValues ??
                header.dataPageHeaderV2!.numValues;
            if (maxRep == 0) {
              rows += numValues;
              continue;
            }
            final Uint8List repLevels;
            if (version == 2) {
              final repLength =
                  header.dataPageHeaderV2!.repetitionLevelsByteLength;
              repLevels = decodeLevels(
                body.slice(0, repLength),
                maxLevel: maxRep,
                numValues: numValues,
                encoding: Encoding.rle,
              );
            } else {
              final raw = codecFor(chunk.compression)
                  .decode(body, uncompressedSize: header.uncompressedPageSize);
              final rawReader = ByteReader(raw);
              final repLength = rawReader.readUint32LE();
              repLevels = decodeLevels(
                rawReader.readSlice(repLength),
                maxLevel: maxRep,
                numValues: numValues,
                encoding: Encoding.rle,
              );
            }
            expect(repLevels[0], 0, reason: 'pages start at a row boundary');
            rows += repLevels.where((level) => level == 0).length;
          }
          expect(rows, group.numRows);
        }
      }
    });
  });

  test('page index is absent by default', () {
    final source = readFixtureTable('plain_snappy.parquet');
    final bytes = writeParquetTable(source);
    final reader = ParquetReader.fromBytes(bytes);
    for (var c = 0; c < reader.schema.numColumns; c++) {
      final chunk = reader.metadata.rowGroup(0).column(c);
      expect(chunk.offsetIndexOffset, isNull);
      expect(chunk.offsetIndexLength, isNull);
      expect(chunk.columnIndexOffset, isNull);
      expect(chunk.columnIndexLength, isNull);
    }
  });
}

void _validateChunk(
  ColumnChunkMetaData chunk,
  int rowGroupRows,
  OffsetIndex offsetIndex,
  ColumnIndex columnIndex,
) {
  final locations = offsetIndex.pageLocations;
  expect(locations, isNotEmpty);
  expect(locations.first.firstRowIndex, 0);
  expect(locations.first.offset, chunk.dataPageOffset);

  var expectedOffset = chunk.dataPageOffset!;
  var compressed = 0;
  for (var i = 0; i < locations.length; i++) {
    final location = locations[i];
    expect(location.offset, expectedOffset);
    expect(location.compressedPageSize, greaterThan(0));
    if (i > 0) {
      expect(
        location.firstRowIndex,
        greaterThan(locations[i - 1].firstRowIndex),
        reason: 'first_row_index must be strictly increasing',
      );
    }
    expect(location.firstRowIndex, lessThan(rowGroupRows));
    expectedOffset += location.compressedPageSize;
    compressed += location.compressedPageSize;
  }
  final dictionaryBytes = chunk.dictionaryPageOffset == null
      ? 0
      : chunk.dataPageOffset! - chunk.dictionaryPageOffset!;
  expect(
    compressed,
    chunk.totalCompressedSize - dictionaryBytes,
    reason: 'page locations must cover exactly the data pages',
  );

  expect(columnIndex.nullPages.length, locations.length);
  expect(columnIndex.minValues.length, locations.length);
  expect(columnIndex.maxValues.length, locations.length);
  final nullCounts = columnIndex.nullCounts;
  if (nullCounts != null) {
    expect(nullCounts.length, locations.length);
    final chunkNulls = chunk.statistics?.nullCount;
    if (chunkNulls != null) {
      expect(nullCounts.fold<int>(0, (a, b) => a + b), chunkNulls);
    }
  }
  for (var i = 0; i < locations.length; i++) {
    if (columnIndex.nullPages[i]) {
      expect(columnIndex.minValues[i], isEmpty);
      expect(columnIndex.maxValues[i], isEmpty);
    } else {
      expect(columnIndex.minValues[i], isNotEmpty);
      expect(columnIndex.maxValues[i], isNotEmpty);
      expect(
        _compareValues(
          chunk,
          columnIndex.minValues[i],
          columnIndex.maxValues[i],
        ),
        lessThanOrEqualTo(0),
      );
    }
  }

  final nonNullPages = <int>[
    for (var i = 0; i < locations.length; i++)
      if (!columnIndex.nullPages[i]) i,
  ];
  if (nonNullPages.length >= 2) {
    var ascending = true;
    var descending = true;
    for (var k = 1; k < nonNullPages.length; k++) {
      final previous = nonNullPages[k - 1];
      final current = nonNullPages[k];
      final minOrder = _compareValues(
        chunk,
        columnIndex.minValues[previous],
        columnIndex.minValues[current],
      );
      final maxOrder = _compareValues(
        chunk,
        columnIndex.maxValues[previous],
        columnIndex.maxValues[current],
      );
      if (minOrder > 0 || maxOrder > 0) ascending = false;
      if (minOrder < 0 || maxOrder < 0) descending = false;
    }
    switch (columnIndex.boundaryOrder) {
      case BoundaryOrder.ascending:
        expect(ascending, isTrue);
      case BoundaryOrder.descending:
        expect(descending, isTrue);
      case BoundaryOrder.unordered:
        break;
    }
  }
}

/// Compares raw index bounds using the column's physical ordering.
int _compareValues(ColumnChunkMetaData chunk, Uint8List a, Uint8List b) {
  final data = ByteData.sublistView(a);
  final other = ByteData.sublistView(b);
  switch (chunk.type) {
    case PhysicalType.int32:
      final logical = chunk.descriptor?.logicalType;
      final unsigned = logical is IntegerLogicalType && !logical.isSigned;
      final left = data.getInt32(0, Endian.little);
      final right = other.getInt32(0, Endian.little);
      if (unsigned) {
        return _unsigned32(left).compareTo(_unsigned32(right));
      }
      return left.compareTo(right);
    case PhysicalType.int64:
      return data
          .getInt64(0, Endian.little)
          .compareTo(other.getInt64(0, Endian.little));
    case PhysicalType.float:
      return data
          .getFloat32(0, Endian.little)
          .compareTo(other.getFloat32(0, Endian.little));
    case PhysicalType.double:
      return data
          .getFloat64(0, Endian.little)
          .compareTo(other.getFloat64(0, Endian.little));
    case PhysicalType.boolean:
      return a[0].compareTo(b[0]);
    case PhysicalType.byteArray:
    case PhysicalType.fixedLenByteArray:
    case PhysicalType.int96:
      return _compareBytes(a, b);
  }
}

int _unsigned32(int value) => value < 0 ? value + 0x100000000 : value;

int _compareBytes(Uint8List a, Uint8List b) {
  final shared = a.length < b.length ? a.length : b.length;
  for (var i = 0; i < shared; i++) {
    final diff = a[i] - b[i];
    if (diff != 0) return diff;
  }
  return a.length.compareTo(b.length);
}

int _int32(Uint8List bytes) =>
    ByteData.sublistView(bytes).getInt32(0, Endian.little);
