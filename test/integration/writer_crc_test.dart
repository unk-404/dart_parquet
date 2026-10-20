@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/crc32.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  group('page checksums', () {
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
                pageSize: 64,
                writeCrc: true,
              ),
            );

            // The reader verifies CRCs when asked to.
            final reader = ParquetReader.fromBytes(
              bytes,
              options: const ParquetReaderOptions(verifyCrc: true),
            );
            expectTablesEqual(source, reader.readTableSync());

            for (var rg = 0; rg < reader.numRowGroups; rg++) {
              final group = reader.metadata.rowGroup(rg);
              for (var c = 0; c < reader.schema.numColumns; c++) {
                final chunk = group.column(c);
                final pages = readChunkPages(bytes, chunk);
                expect(pages, isNotEmpty);
                for (final page in pages) {
                  final crc = page.header.crc;
                  expect(
                    crc,
                    isNotNull,
                    reason: '${chunk.path} page at ${page.offset}',
                  );
                  expect(
                    crc! & 0xFFFFFFFF,
                    crc32(page.body.asUint8List()),
                    reason: '${chunk.path} page at ${page.offset}',
                  );
                }
              }
            }
          });
        }
      }
    }

    test('checksums are absent by default', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(source);
      final reader = ParquetReader.fromBytes(bytes);
      for (var c = 0; c < reader.schema.numColumns; c++) {
        for (final page in readChunkPages(
          bytes,
          reader.metadata.rowGroup(0).column(c),
        )) {
          expect(page.header.crc, isNull);
        }
      }
    });

    test('verifying a corrupted page body fails', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(
        source,
        options: const ParquetWriterOptions(pageSize: 256, writeCrc: true),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).column(0);
      final pages = readChunkPages(bytes, chunk);
      final target = pages.firstWhere(
        (page) => page.header.type == PageType.dataPage,
      );
      final corrupted = Uint8List.fromList(bytes);
      // Flip the last byte of the page body (a value byte for the uncompressed
      // writer used here), not a level length or the header.
      final bodyStart = target.offset + target.headerLength;
      final bodyEnd = bodyStart + target.header.compressedPageSize;
      corrupted[bodyEnd - 1] ^= 0xFF;

      expect(
        () => ParquetReader.fromBytes(
          corrupted,
          options: const ParquetReaderOptions(verifyCrc: true),
        ).readTableSync(),
        throwsA(
          isA<ParquetFormatException>().having(
            (error) => error.toString(),
            'message',
            contains('CRC mismatch'),
          ),
        ),
      );

      // Without verification the corrupt page is decoded (wrong values) but
      // does not throw a checksum error.
      expect(
        () => ParquetReader.fromBytes(corrupted).readTableSync(),
        returnsNormally,
      );
    });

    test('known CRC-32 vector', () {
      final bytes = Uint8List.fromList('123456789'.codeUnits);
      expect(crc32(bytes), 0xCBF43926);
      expect(crc32(Uint8List(0)), 0);
    });
  });
}
