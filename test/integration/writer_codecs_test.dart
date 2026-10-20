@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  group('all compression codecs', () {
    for (final codec in CompressionType.values) {
      for (final version in <int>[1, 2]) {
        test('${codec.name} v$version roundtrip and metadata', () {
          final table = _sampleTable();
          final bytes = writeParquetTable(
            table,
            options: ParquetWriterOptions(
              compression: codec,
              dataPageVersion: version,
              pageSize: 256,
            ),
          );
          final reader = ParquetReader.fromBytes(bytes);
          expectTablesEqual(table, reader.readTableSync());
          expect(reader.schema.numColumns, table.columns.length);

          final group = reader.metadata.rowGroup(0);
          for (var i = 0; i < reader.schema.numColumns; i++) {
            final chunk = group.column(i);
            expect(chunk.compression, codec, reason: 'column $i');
            expect(chunk.totalCompressedSize, greaterThan(0));
            expect(chunk.totalUncompressedSize, greaterThan(0));
            if (codec == CompressionType.uncompressed) {
              expect(
                chunk.totalCompressedSize,
                chunk.totalUncompressedSize,
                reason: 'column $i',
              );
            }
            // Walk the pages: header + declared compressed body must consume
            // exactly total_compressed_size.
            final start = chunk.dictionaryPageOffset ?? chunk.dataPageOffset!;
            final chunkBytes = MemorySource.fromUint8List(bytes)
                .read(start, chunk.totalCompressedSize);
            final input = ByteReader(chunkBytes);
            var pages = 0;
            var compressedBytes = 0;
            var uncompressedBytes = 0;
            while (input.remaining > 0) {
              final header = readPageHeader(input);
              expect(
                input.remaining,
                greaterThanOrEqualTo(header.compressedPageSize),
              );
              input.skip(header.compressedPageSize);
              compressedBytes +=
                  header.bytesConsumed + header.compressedPageSize;
              uncompressedBytes +=
                  header.bytesConsumed + header.uncompressedPageSize;
              pages++;
            }
            expect(compressedBytes, chunk.totalCompressedSize);
            expect(uncompressedBytes, chunk.totalUncompressedSize);
            expect(pages, greaterThan(0));
          }
        });
      }
    }
  });

  test('compression reduces the size of compressible data', () {
    final table = _sampleTable();
    for (final codec in CompressionType.values) {
      if (codec == CompressionType.uncompressed) continue;
      final reader = writeAndOpen(
        table,
        options: ParquetWriterOptions(compression: codec),
      );
      final chunk = reader.metadata.rowGroup(0).columnByPath(['i64'])!;
      expect(
        chunk.totalCompressedSize,
        lessThan(chunk.totalUncompressedSize),
        reason: '${codec.name} should shrink the i64 chunk',
      );
    }
  });

  test('dictionary pages precede data pages', () {
    final table = _sampleTable();
    final reader = writeAndOpen(table);
    final chunk = reader.metadata.rowGroup(0).columnByPath(['name'])!;
    expect(chunk.hasDictionaryPage, isTrue);
    expect(chunk.dictionaryPageOffset!, lessThan(chunk.dataPageOffset!));
  });

  test('all eight codecs are represented', () {
    expect(CompressionType.values.length, 8);
  });
}

ParquetTable _sampleTable() {
  const numRows = 200;
  final schema =
      (ParquetSchemaBuilder()
            ..addInt32('id', nullable: false)
            ..addInt64('i64')
            ..addBinary('name', utf8: true)
            ..addFloat64('value')
            ..addBoolean('flag'))
          .build();

  final validity = Validity(numRows);
  for (var i = 0; i < numRows; i++) {
    if (i % 11 == 0) validity.setNull(i);
  }
  final i64 = Int64Buffer(numRows);
  for (var i = 0; i < numRows; i++) {
    i64.setInt(i, i * 10000000000 - 5);
  }
  final names = Uint8List(numRows * 8);
  final offsets = Uint32List(numRows + 1);
  for (var i = 0; i < numRows; i++) {
    final text = 'row-${i % 7}';
    for (var b = 0; b < text.length; b++) {
      names[offsets[i] + b] = text.codeUnitAt(b);
    }
    offsets[i + 1] = offsets[i] + text.length;
  }
  final bits = Uint8List((numRows + 7) >> 3);
  for (var i = 0; i < numRows; i++) {
    if (i % 3 == 0) bits[i >> 3] |= 1 << (i & 7);
  }

  return ParquetTable(
    schema: schema,
    numRows: numRows,
    columns: <ParquetColumn>[
      Int32Column(
        name: 'id',
        values: Int32List.fromList(List<int>.generate(numRows, (i) => i)),
        validity: Validity(numRows),
      ),
      Int64Column(name: 'i64', values: i64, validity: validity),
      BinaryColumn(
        name: 'name',
        data: names,
        offsets: offsets,
        validity: Validity(numRows),
        isUtf8: true,
      ),
      Float64Column(
        name: 'value',
        values: Float64List.fromList(
          List<double>.generate(numRows, (i) => i / 7),
        ),
        validity: Validity(numRows),
      ),
      BooleanColumn(name: 'flag', bits: bits, validity: Validity(numRows)),
    ],
  );
}
