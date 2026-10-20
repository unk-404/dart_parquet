/// Browser/dart2js reader and writer matrix over tiny embedded fixtures.
///
/// Run with:
///
/// ```sh
/// dart test -p chrome test/web
/// dart test -p chrome -c dart2wasm test/wasm
/// ```
///
/// The fixtures are base64 constants (see `web_fixtures.dart`) so no browser
/// storage or network access is involved. Storage-specific coverage lives in
/// `web_storage_test.dart`.
@TestOn('browser')
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
// ignore: implementation_imports
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:test/test.dart';

import 'fixture_expectations.dart';
import 'web_fixtures.dart';

const Map<String, CompressionType> _flatCodecFixtures =
    <String, CompressionType>{
      'flat_uncompressed.parquet': CompressionType.uncompressed,
      'flat_snappy.parquet': CompressionType.snappy,
      'flat_gzip.parquet': CompressionType.gzip,
      'flat_brotli.parquet': CompressionType.brotli,
      'flat_lz4.parquet': CompressionType.lz4Raw,
      'flat_zstd.parquet': CompressionType.zstd,
    };

void main() {
  group('reader: flat codec matrix', () {
    for (final entry in _flatCodecFixtures.entries) {
      test(entry.key, () {
        final bytes = webFixture(entry.key);
        final reader = ParquetReader.fromBytes(bytes);
        expect(reader.numRows, 8);
        expect(reader.numRowGroups, 1);
        expect(reader.version, 2); // PyArrow writes FileMetaData.version = 2.
        expect(reader.metadata.rowGroup(0).column(0).compression, entry.value);
        expectFlatTable(reader.readTableSync());
      });
    }

    test('every flat fixture has an identical value matrix', () {
      for (final name in _flatCodecFixtures.keys) {
        final reader = ParquetReader.fromBytes(webFixture(name));
        expectFlatTable(reader.readTableSync());
      }
    });
  });

  group('reader: encodings and page versions', () {
    test('dictionary encoding', () {
      final bytes = webFixture('flat_dict_snappy.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['s'])!;
      expect(chunk.hasDictionaryPage, isTrue);
      expect(chunk.isDictionaryEncoded, isTrue);
      expect(chunk.encodings, contains(Encoding.rleDictionary));
      expect(chunk.encodings, contains(Encoding.plain));
      expectFlatTable(reader.readTableSync());
    });

    test('DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY', () {
      final bytes = webFixture('flat_delta.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      final group = reader.metadata.rowGroup(0);
      expect(
        group.columnByPath(['i64'])!.encodings,
        contains(Encoding.deltaBinaryPacked),
      );
      expect(
        group.columnByPath(['i32'])!.encodings,
        contains(Encoding.deltaBinaryPacked),
      );
      expect(
        group.columnByPath(['s'])!.encodings,
        contains(Encoding.deltaByteArray),
      );
      expectFlatTable(reader.readTableSync());
    });

    test('DELTA_LENGTH_BYTE_ARRAY', () {
      final bytes = webFixture('flat_delta_length.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['s'])!;
      expect(chunk.encodings, contains(Encoding.deltaLengthByteArray));
      expectFlatTable(reader.readTableSync());
    });

    test('BYTE_STREAM_SPLIT on data page v2', () {
      final bytes = webFixture('flat_bss_v2.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      final group = reader.metadata.rowGroup(0);
      expect(
        group.columnByPath(['f32'])!.encodings,
        contains(Encoding.byteStreamSplit),
      );
      expect(
        group.columnByPath(['f64'])!.encodings,
        contains(Encoding.byteStreamSplit),
      );
      expectFirstDataPageVersion(bytes, reader, ['f64'], 2);
      expectFlatTable(reader.readTableSync());
    });

    test('v1 data pages for PLAIN fixtures', () {
      final bytes = webFixture('flat_snappy.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      expectFirstDataPageVersion(bytes, reader, ['i64'], 1);
    });
  });

  group('reader: nested', () {
    for (final name in <String>[
      'nested.parquet',
      'nested_snappy.parquet',
      'nested_v2.parquet',
    ]) {
      test(name, () {
        final bytes = webFixture(name);
        final reader = ParquetReader.fromBytes(bytes);
        expect(reader.numRows, 6);
        expectNestedTable(reader.readTableSync());
      });
    }

    test('data page v2 for nested columns', () {
      final bytes = webFixture('nested_v2.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      expectFirstDataPageVersion(bytes, reader, ['lst', 'list', 'element'], 2);
      expectFirstDataPageVersion(bytes, reader, ['st', 'a'], 2);
    });

    test('dotted-path nested projection', () {
      final reader = ParquetReader.fromBytes(webFixture('nested.parquet'));

      final structOnly = reader.readTableSync(columns: ['id', 'st.a']);
      expect(structOnly.columns.map((c) => c.name).toList(), <String>[
        'id',
        'st',
      ]);
      final st = structOnly['st'] as StructColumn;
      expect(st.fields.keys, <String>['a']);
      expect(st.isNull(0), isTrue);
      expect(st.getStruct(1), <String, Object?>{'a': 1});
      expect(st.getStruct(2), <String, Object?>{'a': 2});

      final listOnly = reader.readTableSync(columns: ['lst.list.element']);
      final lst = listOnly['lst'] as ListColumn;
      expect(lst.getList(0), isEmpty);
      expect(lst.getList(2), <Object?>[20, 21]);
      expect(lst.getList(4), <Object?>[30, 31, 32]);

      final mapOnly = reader.readTableSync(columns: ['mp.key_value.value']);
      final mp = mapOnly['mp'] as MapColumn;
      expect(mp.getMap(1), <Object?, Object?>{'k0': 0});
      expect(mp.getMap(2), <Object?, Object?>{'k0': 0, 'k1': 1});
    });
  });

  group('reader: projection, row groups, streaming', () {
    test('top-level projection reads only requested columns', () {
      final reader = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'));
      final table = reader.readTableSync(columns: ['i64', 's']);
      expect(table.columns.map((c) => c.name).toList(), <String>['i64', 's']);
      expect(table.column('f64'), isNull);
      expect((table['i64'] as Int64Column).getBigInt(6), expectedInt64[6]);
      expect((table['s'] as BinaryColumn).getString(1), 'beta');
    });

    test('maxRows truncates flat results', () {
      final reader = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'));
      final table = reader.readTableSync(maxRows: 3);
      expect(table.numRows, 3);
      expect(table['i64'].length, 3);
      expect(reader.readRowGroupSync(0, maxRows: 2).numRows, 2);
    });

    test('rowGroupFilter skips groups before decoding', () {
      final reader = ParquetReader.fromBytes(
        webFixture('multi_rowgroup.parquet'),
      );
      var calls = 0;
      final table = reader.readTableSync(
        rowGroupFilter: (group) {
          calls++;
          return group.index.isEven;
        },
      );
      expect(calls, 3);
      expect(table.numRows, 8);
      final id = table['id'] as Int64Column;
      final s = table['s'] as BinaryColumn;
      expect(id.getInt(0), 0);
      expect(id.getInt(3), 3);
      expect(id.getInt(4), 8);
      expect(id.getInt(7), 11);
      expect(s.getString(0), 'row-00');
      expect(s.getString(7), 'row-11');
    });

    test('readRowGroupSync reads one group', () {
      final reader = ParquetReader.fromBytes(
        webFixture('multi_rowgroup.parquet'),
      );
      final group = reader.readRowGroupSync(1);
      expect(group.numRows, 4);
      expect((group['id'] as Int64Column).getInt(0), 4);
      expect((group['s'] as BinaryColumn).getString(0), 'row-04');
    });

    test('readRowGroups streams one table per group', () async {
      final reader = ParquetReader.fromBytes(
        webFixture('multi_rowgroup.parquet'),
      );
      final sizes = <int>[];
      final projected = <List<String>>[];
      await for (final group in reader.readRowGroups()) {
        sizes.add(group.numRows);
        projected.add(group.columns.map((c) => c.name).toList());
      }
      expect(sizes, <int>[4, 4, 4]);
      expect(projected.first, <String>['id', 's', 'flag']);
    });

    test('readRowGroups applies filter, projection and maxRows', () async {
      final reader = ParquetReader.fromBytes(
        webFixture('multi_rowgroup.parquet'),
      );
      final sizes = <int>[];
      await for (final group in reader.readRowGroups(
        columns: ['id'],
        rowGroupFilter: (group) => group.index.isEven,
        maxRows: 6,
      )) {
        expect(group.columns.map((c) => c.name).toList(), <String>['id']);
        sizes.add(group.numRows);
      }
      expect(sizes, <int>[4, 2]);
    });

    test('async readTable equals sync readTable', () async {
      final reader = ParquetReader.fromBytes(webFixture('flat_delta.parquet'));
      final sync = reader.readTableSync();
      final async = await reader.readTable();
      expect(async.numRows, sync.numRows);
      for (var i = 0; i < sync.numRows; i++) {
        expect(
          (async['i64'] as Int64Column).getBigInt(i),
          (sync['i64'] as Int64Column).getBigInt(i),
        );
        expect(
          (async['s'] as BinaryColumn).isNull(i)
              ? null
              : (async['s'] as BinaryColumn).getString(i),
          (sync['s'] as BinaryColumn).isNull(i)
              ? null
              : (sync['s'] as BinaryColumn).getString(i),
        );
      }
    });

    test('predicate API prunes and filters flat rows', () async {
      final bytes = webFixture('multi_rowgroup.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      final filtered = await reader.readTable(
        predicate: ColumnPredicate('id', FilterOperator.gt, 8),
      );
      expect(filtered.numRows, 3);
      expect((filtered['id'] as Int64Column).getInt(0), 9);

      final table = reader.readTableSync();
      final rowFiltered = table.filterRows(
        AndPredicate(<ParquetPredicate>[
          ColumnPredicate('id', FilterOperator.gtEq, 2),
          ColumnPredicate('flag', FilterOperator.eq, true),
        ]),
      );
      expect(rowFiltered.numRows, 5); // ids 2, 4, 6, 8, 10
      expect((rowFiltered['id'] as Int64Column).getInt(0), 2);
    });
  });

  group('reader: exact int64 on the web', () {
    test('getBigInt is exact beyond 2^53', () {
      final reader = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'));
      final table = reader.readTableSync();
      final i64 = table['i64'] as Int64Column;
      for (var i = 0; i < expectedInt64.length; i++) {
        expect(i64.getBigInt(i), expectedInt64[i], reason: 'BigInt[$i]');
      }
    });

    test('getInt throws where a JS double cannot be exact', () {
      final reader = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'));
      final i64 = reader.readTableSync()['i64'] as Int64Column;
      expect(i64.getInt(0), 0);
      expect(i64.getInt(2), -1);
      expect(i64.getInt(3), 42);
      expect(i64.getInt(4), -2);
      // 2^53, 2^53 + 1 and int64.min are not exactly representable in a
      // JavaScript number, so the int accessor must refuse them.
      expect(() => i64.getInt(5), throwsStateError);
      expect(() => i64.getInt(6), throwsStateError);
      expect(() => i64.getInt(7), throwsStateError);
    });

    test('getBigInt is exact for DELTA_BINARY_PACKED int64', () {
      final reader = ParquetReader.fromBytes(webFixture('flat_delta.parquet'));
      final i64 = reader.readTableSync()['i64'] as Int64Column;
      expect(i64.getBigInt(6), BigInt.parse('9007199254740993'));
      expect(i64.getBigInt(7), BigInt.parse('-9223372036854775808'));
    });
  });

  group('reader: page index', () {
    test('reads OffsetIndex and ColumnIndex from a fixture', () {
      final bytes = webFixture('page_index.parquet');
      final reader = ParquetReader.fromBytes(bytes);
      final source = MemorySource.fromUint8List(bytes);
      final group = reader.metadata.rowGroup(0);
      final chunk = group.columnByPath(['id'])!;
      expect(chunk.offsetIndexOffset, isNotNull);
      expect(chunk.columnIndexOffset, isNotNull);

      final offsetIndex = chunk.readOffsetIndex(source);
      expect(offsetIndex, isNotNull);
      expect(offsetIndex!.pageLocations, isNotEmpty);
      expect(offsetIndex.pageLocations.first.firstRowIndex, 0);
      expect(offsetIndex.pageLocations.first.offset, greaterThan(0));

      final columnIndex = chunk.readColumnIndex(source);
      expect(columnIndex, isNotNull);
      expect(columnIndex!.nullPages.length, offsetIndex.pageLocations.length);
      expect(columnIndex.minValues.length, columnIndex.nullPages.length);
      expect(columnIndex.maxValues.length, columnIndex.nullPages.length);
      expect(reader.readTableSync().numRows, 200);
    });
  });

  group('writer: round trips', () {
    late ParquetTable flat;
    late ParquetTable nested;
    setUp(() {
      flat = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'))
          .readTableSync();
      nested = ParquetReader.fromBytes(webFixture('nested.parquet'))
          .readTableSync();
    });

    for (final compression in allCompressionTypes) {
      for (final version in <int>[1, 2]) {
        test('flat ${compression.name} v$version', () {
          final bytes = writeParquetTable(
            flat,
            options: ParquetWriterOptions(
              compression: compression,
              dataPageVersion: version,
            ),
          );
          final reader = ParquetReader.fromBytes(bytes);
          expect(reader.numRows, 8);
          expect(
            reader.metadata.rowGroup(0).column(0).compression,
            compression,
          );
          expectFirstDataPageVersion(bytes, reader, ['i64'], version);
          expectFlatTable(reader.readTableSync());
        });
      }
    }

    test('dictionary on/off', () {
      final withDictionary = ParquetReader.fromBytes(writeParquetTable(flat));
      final stringChunk = withDictionary.metadata.rowGroup(0).columnByPath([
        's',
      ])!;
      expect(stringChunk.hasDictionaryPage, isTrue);
      expectFlatTable(withDictionary.readTableSync());

      final plain = ParquetReader.fromBytes(
        writeParquetTable(
          flat,
          options: const ParquetWriterOptions(useDictionary: false),
        ),
      );
      final plainChunk = plain.metadata.rowGroup(0).columnByPath(['s'])!;
      expect(plainChunk.hasDictionaryPage, isFalse);
      expect(plainChunk.encodings, isNot(contains(Encoding.rleDictionary)));
      expectFlatTable(plain.readTableSync());
    });

    for (final version in <int>[1, 2]) {
      test('nested snappy v$version', () {
        final bytes = writeParquetTable(
          nested,
          options: ParquetWriterOptions(
            compression: CompressionType.snappy,
            dataPageVersion: version,
          ),
        );
        final reader = ParquetReader.fromBytes(bytes);
        expectFirstDataPageVersion(bytes, reader, [
          'lst',
          'list',
          'element',
        ], version);
        expectNestedTable(reader.readTableSync());
      });
    }

    test('multiple row groups preserve order', () {
      final reader = ParquetReader.fromBytes(
        webFixture('multi_rowgroup.parquet'),
      );
      final writer = ParquetWriter(
        schema: reader.schema,
        options: const ParquetWriterOptions(compression: CompressionType.zstd),
      );
      for (var i = 0; i < reader.numRowGroups; i++) {
        writer.add(reader.readRowGroupSync(i));
      }
      final out = ParquetReader.fromBytes(writer.finish());
      expect(out.numRowGroups, 3);
      expectMultiRowGroupTable(out.readTableSync());
    });

    test('empty writer round trip', () {
      final writer = ParquetWriter(schema: flat.schema);
      final bytes = writer.finish();
      final reader = ParquetReader.fromBytes(bytes);
      expect(reader.numRows, 0);
      expect(reader.numRowGroups, 0);
      expect(reader.readTableSync().numRows, 0);
    });
  });

  group('writer: v0.1.2 options', () {
    late ParquetTable flat;
    setUp(() {
      flat = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'))
          .readTableSync();
    });

    test('CRC32 is written and verified', () {
      final bytes = writeParquetTable(
        flat,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          writeCrc: true,
        ),
      );
      final reader = ParquetReader.fromBytes(
        bytes,
        options: const ParquetReaderOptions(verifyCrc: true),
      );
      expectFlatTable(reader.readTableSync());

      final chunk = reader.metadata.rowGroup(0).columnByPath(['i64'])!;
      final input = ByteReader(
        MemorySource.fromUint8List(bytes)
            .read(chunk.dataPageOffset!, chunk.totalCompressedSize),
      );
      final header = readPageHeaderWithSize(input).header;
      expect(header.crc, isNotNull);

      final corrupted = Uint8List.fromList(bytes);
      final bodyOffset = firstDataPageBodyOffset(bytes, reader, ['i64']);
      corrupted[bodyOffset] = corrupted[bodyOffset] ^ 0xFF;
      final broken = ParquetReader.fromBytes(
        corrupted,
        options: const ParquetReaderOptions(verifyCrc: true),
      );
      expect(
        () => broken.readTableSync(),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('page index is written and readable', () {
      final bytes = writeParquetTable(
        flat,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          writePageIndex: true,
          pageSize: 16,
        ),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final source = MemorySource.fromUint8List(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['i64'])!;
      expect(chunk.offsetIndexOffset, isNotNull);
      expect(chunk.columnIndexOffset, isNotNull);

      final offsetIndex = chunk.readOffsetIndex(source)!;
      expect(offsetIndex.pageLocations.length, greaterThan(1));
      expect(offsetIndex.pageLocations.first.firstRowIndex, 0);
      expect(offsetIndex.pageLocations.first.offset, greaterThan(0));

      final columnIndex = chunk.readColumnIndex(source)!;
      expect(columnIndex.nullPages.length, offsetIndex.pageLocations.length);
      expect(columnIndex.minValues.length, columnIndex.nullPages.length);
      expect(columnIndex.maxValues.length, columnIndex.nullPages.length);
      expectFlatTable(reader.readTableSync());
    });

    test('bloom filter is written and queryable', () {
      final bytes = writeParquetTable(
        flat,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          writeBloomFilter: true,
          bloomFilterFpp: 1e-9,
          bloomFilterMaxBytes: 1024,
        ),
      );
      final reader = ParquetReader.fromBytes(bytes);
      final source = MemorySource.fromUint8List(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['s'])!;
      expect(chunk.bloomFilterOffset, isNotNull);
      expect(chunk.bloomFilterLength, isNotNull);

      final filter = chunk.readBloomFilter(source)!;
      for (final present in <String>['alpha', 'beta', 'delta', 'zeta']) {
        expect(
          filter.mightContainBytes(Uint8List.fromList(utf8.encode(present))),
          isTrue,
          reason: 'bloom must contain $present',
        );
      }
      expect(
        filter.mightContainBytes(
          Uint8List.fromList(utf8.encode('definitely-not-present')),
        ),
        isFalse,
      );
      expectFlatTable(reader.readTableSync());
    });

    test('options default to the v0.1.1 byte layout', () {
      final bytes = writeParquetTable(flat);
      final reader = ParquetReader.fromBytes(bytes);
      final chunk = reader.metadata.rowGroup(0).columnByPath(['i64'])!;
      expect(chunk.bloomFilterOffset, isNull);
      expect(chunk.offsetIndexOffset, isNull);
      final input = ByteReader(
        MemorySource.fromUint8List(bytes)
            .read(chunk.dataPageOffset!, chunk.totalCompressedSize),
      );
      expect(readPageHeaderWithSize(input).header.crc, isNull);
    });
  });
}
