@TestOn('vm')
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:parquet/src/writer/bloom_filter.dart';
import 'package:parquet/src/writer/shredder.dart';
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  group('bloom filter roundtrip', () {
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
                writeBloomFilter: true,
              ),
            );
            final reader = ParquetReader.fromBytes(bytes);
            expectTablesEqual(source, reader.readTableSync());
            final memory = MemorySource.fromUint8List(bytes);
            final leaves = shredTable(source, reader.schema);
            final group = reader.metadata.rowGroup(0);
            for (var c = 0; c < reader.schema.numColumns; c++) {
              final chunk = group.column(c);
              final bloom = chunk.readBloomFilter(memory);
              expect(
                bloom,
                isNotNull,
                reason: '${chunk.path} should have a bloom filter',
              );
              expect(bloom!.numBytes, greaterThanOrEqualTo(32));
              expect(
                bloom.numBytes & (bloom.numBytes - 1),
                0,
                reason: 'bloom size must be a power of two',
              );
              final leaf = leaves[c];
              for (var i = 0; i < leaf.values.length; i++) {
                final raw = canonicalBloomBytes(
                  leaf.values[i],
                  leaf.descriptor.physicalType,
                );
                expect(
                  bloom.mightContainBytes(raw),
                  isTrue,
                  reason: '${chunk.path} value $i must be present',
                );
              }
              expect(
                chunk.bloomFilterLength,
                encodeBloomFilterHeader(bloom.numBytes).length + bloom.numBytes,
              );
            }
          });
        }
      }
    }

    test('bloom filters are absent by default', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(source);
      final reader = ParquetReader.fromBytes(bytes);
      final memory = MemorySource.fromUint8List(bytes);
      for (var c = 0; c < reader.schema.numColumns; c++) {
        final chunk = reader.metadata.rowGroup(0).column(c);
        expect(chunk.bloomFilterOffset, isNull);
        expect(chunk.bloomFilterLength, isNull);
        expect(chunk.readBloomFilter(memory), isNull);
      }
    });

    test('multi row groups each get their own filter', () {
      final sourceReader = readFixtureReader('flat_multi_rowgroup.parquet');
      final writer = ParquetWriter(
        schema: sourceReader.schema,
        options: const ParquetWriterOptions(
          pageSize: 128,
          writeBloomFilter: true,
        ),
      );
      for (var i = 0; i < sourceReader.numRowGroups; i++) {
        writer.add(sourceReader.readRowGroupSync(i));
      }
      final bytes = writer.finish();
      final reader = ParquetReader.fromBytes(bytes);
      final memory = MemorySource.fromUint8List(bytes);
      for (var rg = 0; rg < reader.numRowGroups; rg++) {
        final group = reader.metadata.rowGroup(rg);
        for (var c = 0; c < reader.schema.numColumns; c++) {
          final bloom = group.column(c).readBloomFilter(memory);
          expect(bloom, isNotNull);
        }
      }
    });

    test('all-null chunks get no bloom filter', () {
      final schema = (ParquetSchemaBuilder()..addInt32('v')).build();
      final table = ParquetTable(
        schema: schema,
        numRows: 10,
        columns: <ParquetColumn>[
          Int32Column(
            name: 'v',
            values: Int32List(10),
            validity: Validity(10, allValid: false),
          ),
        ],
      );
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(writeBloomFilter: true),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).column(0);
      expect(chunk.bloomFilterOffset, isNull);
      expect(chunk.readBloomFilter(MemorySource.fromUint8List(bytes)), isNull);
    });

    test('bloomFilterMaxBytes caps the filter size', () {
      final source = readFixtureTable('plain_snappy.parquet');
      final bytes = writeParquetTable(
        source,
        options: const ParquetWriterOptions(
          writeBloomFilter: true,
          bloomFilterMaxBytes: 64,
        ),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final memory = MemorySource.fromUint8List(bytes);
      for (var c = 0; c < reader.schema.numColumns; c++) {
        final bloom = reader.metadata
            .rowGroup(0)
            .column(c)
            .readBloomFilter(memory);
        expect(bloom, isNotNull);
        expect(bloom!.numBytes, lessThanOrEqualTo(64));
        expect(bloom.numBytes, greaterThanOrEqualTo(32));
      }
    });

    test('false positives stay near the configured fpp', () {
      final schema = (ParquetSchemaBuilder()..addBinary('s', utf8: true))
          .build();
      const rows = 2000;
      final data = Uint8List(rows * 12);
      final offsets = Uint32List(rows + 1);
      for (var i = 0; i < rows; i++) {
        final text = 'present-$i';
        for (var b = 0; b < text.length; b++) {
          data[offsets[i] + b] = text.codeUnitAt(b);
        }
        offsets[i + 1] = offsets[i] + text.length;
      }
      final table = ParquetTable(
        schema: schema,
        numRows: rows,
        columns: <ParquetColumn>[
          BinaryColumn(
            name: 's',
            data: data,
            offsets: offsets,
            validity: Validity(rows),
            isUtf8: true,
          ),
        ],
      );
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(
          writeBloomFilter: true,
          bloomFilterFpp: 0.01,
        ),
      );
      final bloom = ParquetReader.fromBytes(bytes).metadata
          .rowGroup(0)
          .column(0)
          .readBloomFilter(MemorySource.fromUint8List(bytes))!;
      for (var i = 0; i < rows; i++) {
        expect(
          bloom.mightContainBytes(
            Uint8List.fromList(utf8.encode('present-$i')),
          ),
          isTrue,
        );
      }
      var falsePositives = 0;
      const probes = 5000;
      for (var i = 0; i < probes; i++) {
        if (bloom.mightContainBytes(
          Uint8List.fromList(utf8.encode('absent-value-$i')),
        )) {
          falsePositives++;
        }
      }
      expect(falsePositives / probes, lessThan(0.04));
    });

    test('null values are skipped but present values still hit', () {
      final schema = (ParquetSchemaBuilder()..addInt64('v')).build();
      const rows = 100;
      final values = Int64Buffer(rows);
      final validity = Validity(rows);
      final expected = <int>[];
      for (var i = 0; i < rows; i++) {
        values.setInt(i, i * 10000000000);
        if (i % 2 == 0) {
          validity.setNull(i);
        } else {
          expected.add(i * 10000000000);
        }
      }
      final table = ParquetTable(
        schema: schema,
        numRows: rows,
        columns: <ParquetColumn>[
          Int64Column(name: 'v', values: values, validity: validity),
        ],
      );
      final bytes = writeParquetTable(
        table,
        options: const ParquetWriterOptions(writeBloomFilter: true),
      );
      final bloom = ParquetReader.fromBytes(bytes).metadata
          .rowGroup(0)
          .column(0)
          .readBloomFilter(MemorySource.fromUint8List(bytes))!;
      for (final value in expected) {
        final data = ByteData(8)..setInt64(0, value, Endian.little);
        expect(bloom.mightContainBytes(data.buffer.asUint8List()), isTrue);
      }
    });
  });
}
