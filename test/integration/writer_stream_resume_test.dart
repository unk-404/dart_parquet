@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import '../support/blit_file.dart';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/format/parquet_format.dart' show ColumnOrderKind;
import 'package:test/test.dart';

import 'writer_test_utils.dart';

void main() {
  late Directory temp;

  setUp(() {
    temp = Directory.systemTemp.createTempSync('parquet_stream_resume_');
  });

  tearDown(() {
    if (temp.existsSync()) temp.deleteSync(recursive: true);
  });

  group('fresh stream writer', () {
    test('output bytes are unchanged by the resume refactor', () async {
      final nested = readFixtureTable('nested_snappy.parquet');
      final flat = readFixtureTable('plain_snappy.parquet');

      Future<Uint8List> writeTwice(
        ParquetTable table,
        ParquetWriterOptions options,
      ) async {
        final path = '${temp.path}/stable_${_fnv(Uint8List(0))}.parquet';
        final file = await openBytesFile(path, create: true, readOnly: false);
        final writer = ParquetStreamWriter(
          table.schema,
          file,
          options: options,
        );
        await writer.add(table);
        await writer.add(table);
        await writer.finish(close: true);
        return File(path).readAsBytesSync();
      }

      final defaultBytes = await writeTwice(
        nested,
        const ParquetWriterOptions(createdBy: 'parquet-dart version 0.1.3'),
      );
      expect(defaultBytes.length, 13462);
      expect(_fnv(defaultBytes), 1623461065);

      final featureBytes = await writeTwice(
        nested,
        const ParquetWriterOptions(
          pageSize: 64,
          writeCrc: true,
          writePageIndex: true,
          writeBloomFilter: true,
          createdBy: 'parquet-dart version 0.1.3',
        ),
      );
      expect(featureBytes.length, 35948);
      expect(_fnv(featureBytes), 1650552960);

      final zstdBytes = await writeTwice(
        flat,
        const ParquetWriterOptions(
          compression: CompressionType.zstd,
          dataPageVersion: 2,
          pageSize: 256,
          writeCrc: true,
          writePageIndex: true,
          writeBloomFilter: true,
          createdBy: 'parquet-dart version 0.1.3',
        ),
      );
      expect(zstdBytes.length, 111700);
      expect(_fnv(zstdBytes), 1329586747);
    });

    test('fileMetaData is valid on an empty writer', () async {
      final table = readFixtureTable('web_tiny.parquet');
      final path = '${temp.path}/empty_meta.parquet';
      final file = await openBytesFile(path, create: true, readOnly: false);
      final writer = ParquetStreamWriter(table.schema, file);
      try {
        final metadata = writer.fileMetaData;
        expect(metadata.numRows, 0);
        expect(metadata.rowGroups, isEmpty);
        expect(
          diffSchemas(table.schema, ParquetSchema.fromFileMetaData(metadata)),
          isEmpty,
        );
        expect(metadata.columnOrders!.length, table.schema.numColumns);
      } finally {
        await writer.finish(close: true);
      }
    });

    test(
      'fileMetaData reflects state before finish and throws after',
      () async {
        final table = readFixtureTable('nested_snappy.parquet');
        const options = ParquetWriterOptions(
          pageSize: 64,
          writePageIndex: true,
          writeBloomFilter: true,
        );
        final path = '${temp.path}/getter.parquet';
        final file = await openBytesFile(path, create: true, readOnly: false);
        final writer = ParquetStreamWriter(
          table.schema,
          file,
          options: options,
        );
        await writer.add(table);
        final before = writer.fileMetaData;
        expect(before.version, 1);
        expect(before.numRows, table.numRows);
        expect(before.rowGroups.length, 1);
        expect(before.createdBy, options.createdBy);
        expect(before.columnOrders, isNotNull);
        expect(before.columnOrders!.length, table.schema.numColumns);
        for (final chunk in before.rowGroups.single.columns) {
          expect(chunk.offsetIndexOffset, isNotNull);
          expect(chunk.metaData.bloomFilterOffset, isNotNull);
        }

        await writer.finish(close: true);
        expect(() => writer.fileMetaData, throwsStateError);

        final after = _metadataOf(File(path).readAsBytesSync());
        expect(after.numRows, before.numRows);
        expect(after.rowGroups.length, before.rowGroups.length);
        expect(after.createdBy, before.createdBy);
        expect(after.version, before.version);
        for (var i = 0; i < before.rowGroups.length; i++) {
          final expected = before.rowGroups[i];
          final actual = after.rowGroups[i];
          expect(actual.numRows, expected.numRows);
          for (var c = 0; c < expected.columns.length; c++) {
            expect(
              actual.columns[c].offsetIndexOffset,
              expected.columns[c].offsetIndexOffset,
            );
            expect(
              actual.columns[c].metaData.bloomFilterOffset,
              expected.columns[c].metaData.bloomFilterOffset,
            );
          }
        }
      },
    );

    test('default stream writer still round-trips', () async {
      final table = readFixtureTable('web_tiny.parquet');
      final path = '${temp.path}/default.parquet';
      final file = await openBytesFile(path, create: true, readOnly: false);
      final writer = ParquetStreamWriter(table.schema, file);
      await writer.add(table);
      await writer.finish(close: true);
      final streamed = File(path).readAsBytesSync();
      final reader = ParquetReader.fromBytes(streamed);
      expect(reader.numRows, table.numRows);
      expectTablesEqual(table, reader.readTableSync());
    });
  });

  group('resume', () {
    test('empty prefix (PAR1 only) is valid', () async {
      final table = readFixtureTable('nested_snappy.parquet');
      final emptyMetadata = _metadataOf(
        ParquetWriter(schema: table.schema).finish(),
      );
      expect(emptyMetadata.numRows, 0);
      expect(emptyMetadata.rowGroups, isEmpty);

      final path = '${temp.path}/empty_prefix.parquet';
      await _writeFileBytes(path, _magic);
      final file = await openBytesFile(path, readOnly: false);

      final resumed = await ParquetStreamWriter.resume(
        table.schema,
        file,
        emptyMetadata,
      );
      expect(resumed.position, 4);
      expect(resumed.numRows, 0);
      expect(resumed.numRowGroups, 0);
      await resumed.add(table);
      expect(resumed.position, greaterThan(4));
      await resumed.finish(close: true);

      final bytes = File(path).readAsBytesSync();
      final reader = ParquetReader.fromBytes(bytes);
      expect(reader.numRowGroups, 1);
      expect(reader.numRows, table.numRows);
      expectTablesEqual(table, reader.readTableSync());
    });

    test('empty prefix can be finished without adding rows', () async {
      final table = readFixtureTable('web_tiny.parquet');
      final emptyMetadata = _metadataOf(
        ParquetWriter(schema: table.schema).finish(),
      );
      final path = '${temp.path}/empty_finish.parquet';
      await _writeFileBytes(path, _magic);
      final file = await openBytesFile(path, readOnly: false);
      final resumed = await ParquetStreamWriter.resume(
        table.schema,
        file,
        emptyMetadata,
      );
      await resumed.finish(close: true);
      final reader = ParquetReader.fromBytes(File(path).readAsBytesSync());
      expect(reader.numRows, 0);
      expect(reader.numRowGroups, 0);
    });

    test('abandon, resume, finish keeps indexes and blooms', () async {
      final source = readFixtureReader('nested_structs_multi.parquet');
      final schema = source.schema;
      final first = source.readRowGroupSync(0);
      final second = source.readRowGroupSync(1);
      const options = ParquetWriterOptions(
        compression: CompressionType.snappy,
        dataPageVersion: 2,
        pageSize: 32,
        writeCrc: true,
        writePageIndex: true,
        writeBloomFilter: true,
      );
      final path = '${temp.path}/crash.parquet';

      final abandoned = await openBytesFile(
        path,
        create: true,
        readOnly: false,
      );
      final crashed = ParquetStreamWriter(schema, abandoned, options: options);
      await crashed.add(first);
      await abandoned.flush();
      final metadata = crashed.fileMetaData;
      final prefix = File(path).readAsBytesSync();
      // Simulate a crash: close without writing the footer.
      await abandoned.close();
      expect(prefix.length, greaterThan(4));
      expect(metadata.rowGroups.length, 1);
      expect(metadata.numRows, first.numRows);

      final reopened = await openBytesFile(path, readOnly: false);
      final resumed = await ParquetStreamWriter.resume(
        schema,
        reopened,
        metadata,
        options: options,
      );
      expect(resumed.position, prefix.length);
      expect(resumed.numRows, first.numRows);
      expect(resumed.numRowGroups, 1);
      expect(resumed.fileMetaData.numRows, first.numRows);

      await resumed.add(second);
      await resumed.finish(close: true);

      final bytes = File(path).readAsBytesSync();
      expect(bytes.sublist(0, prefix.length), prefix, reason: 'prefix kept');
      final reader = ParquetReader.fromBytes(
        bytes,
        options: const ParquetReaderOptions(verifyCrc: true),
      );
      expect(reader.numRowGroups, 2);
      expect(reader.numRows, first.numRows + second.numRows);
      expectTablesEqual(first, reader.readRowGroupSync(0));
      expectTablesEqual(second, reader.readRowGroupSync(1));

      final memory = MemorySource.fromUint8List(bytes);
      for (final group in reader.metadata.rowGroups) {
        for (final chunk in group.columns) {
          expect(chunk.readOffsetIndex(memory), isNotNull);
          expect(chunk.readBloomFilter(memory), isNotNull);
        }
      }
    });

    test('restores row group ordinals', () async {
      final source = readFixtureReader('flat_multi_rowgroup.parquet');
      final schema = source.schema;
      final groups = [for (var i = 0; i < 2; i++) source.readRowGroupSync(i)];
      final path = '${temp.path}/ordinals.parquet';

      final abandoned = await openBytesFile(
        path,
        create: true,
        readOnly: false,
      );
      final crashed = ParquetStreamWriter(schema, abandoned);
      for (final group in groups) {
        await crashed.add(group);
      }
      await abandoned.flush();
      final metadata = crashed.fileMetaData;
      await abandoned.close();

      final reopened = await openBytesFile(path, readOnly: false);
      final resumed = await ParquetStreamWriter.resume(
        schema,
        reopened,
        metadata,
      );
      await resumed.add(source.readRowGroupSync(2));
      await resumed.finish(close: true);

      final reader = ParquetReader.fromBytes(File(path).readAsBytesSync());
      expect(reader.numRowGroups, 3);
      expect(
        [for (final group in reader.metadata.rowGroups) group.ordinal],
        [0, 1, 2],
      );
    });

    test('preserves version, key/value metadata and column orders', () async {
      final table = readFixtureTable('flat_multi_rowgroup.parquet');
      final path = '${temp.path}/preserve.parquet';
      final (prefix, original) = await _abandonedPrefix(
        path,
        table.schema,
        table,
        const ParquetWriterOptions(),
      );
      final leafCount = ParquetSchema.fromFileMetaData(original).numColumns;
      final custom = FileMetaData(
        version: 2,
        schema: original.schema,
        numRows: original.numRows,
        rowGroups: original.rowGroups,
        keyValueMetadata: const <KeyValue>[
          KeyValue(key: 'team', value: 'parquet'),
          KeyValue(key: 'flag'),
        ],
        createdBy: 'external writer',
        columnOrders: <ColumnOrder>[
          for (var i = 0; i < leafCount; i++)
            const ColumnOrder(ColumnOrderKind.ieee754TotalOrder),
        ],
      );

      final file = await openBytesFile(path, readOnly: false);
      final resumed = await ParquetStreamWriter.resume(
        table.schema,
        file,
        custom,
        options: const ParquetWriterOptions(
          createdBy: 'parquet-dart resumed 2.0',
        ),
      );
      final before = resumed.fileMetaData;
      expect(before.version, 2);
      expect(before.keyValueMetadata!.length, 2);
      expect(before.keyValueMetadata![0].key, 'team');
      expect(before.keyValueMetadata![1].value, isNull);
      expect(before.columnOrders!.length, leafCount);
      expect(
        before.columnOrders!.every(
          (order) => order.kind == ColumnOrderKind.ieee754TotalOrder,
        ),
        isTrue,
      );
      // Resumed data is still readable and the new part is appended.
      expect(resumed.position, prefix.length);
      await resumed.finish(close: true);

      final after = _metadataOf(File(path).readAsBytesSync());
      expect(after.version, 2);
      expect(after.createdBy, 'parquet-dart resumed 2.0');
      expect(after.keyValueMetadata!.length, 2);
      expect(after.columnOrders!.length, leafCount);
      expect(after.columnOrders!.first.kind, ColumnOrderKind.ieee754TotalOrder);
      expect(after.numRows, original.numRows);
    });

    test('current options override resumed key/value metadata', () async {
      final table = readFixtureTable('flat_multi_rowgroup.parquet');
      final path = '${temp.path}/override_kv.parquet';
      final (_, original) = await _abandonedPrefix(
        path,
        table.schema,
        table,
        const ParquetWriterOptions(),
      );
      final custom = FileMetaData(
        version: original.version,
        schema: original.schema,
        numRows: original.numRows,
        rowGroups: original.rowGroups,
        keyValueMetadata: const <KeyValue>[KeyValue(key: 'old', value: 'v')],
        createdBy: original.createdBy,
        columnOrders: original.columnOrders,
      );
      final file = await openBytesFile(path, readOnly: false);
      final resumed = await ParquetStreamWriter.resume(
        table.schema,
        file,
        custom,
        options: const ParquetWriterOptions(
          keyValueMetadata: <String, String>{'new': 'kv'},
        ),
      );
      final metadata = resumed.fileMetaData;
      expect(metadata.keyValueMetadata!.length, 1);
      expect(metadata.keyValueMetadata!.single.key, 'new');
      await resumed.finish(close: true);
    });

    test('rejects a complete file with a footer', () async {
      final table = readFixtureTable('web_tiny.parquet');
      final path = '${temp.path}/complete.parquet';
      final bytes = await _writeStreamedFile(path, table.schema, <ParquetTable>[
        table,
      ]);
      final metadata = _metadataOf(bytes);
      final file = await openBytesFile(path, readOnly: false);
      try {
        await expectLater(
          ParquetStreamWriter.resume(table.schema, file, metadata),
          throwsA(
            isA<ParquetFormatException>().having(
              (error) => error.toString(),
              'message',
              contains('footer'),
            ),
          ),
        );
      } finally {
        await file.close();
      }
    });

    test('rejects empty and non-PAR1 prefixes', () async {
      final table = readFixtureTable('web_tiny.parquet');
      final emptyMetadata = _metadataOf(
        ParquetWriter(schema: table.schema).finish(),
      );

      final emptyPath = '${temp.path}/no_magic.parquet';
      await _writeFileBytes(emptyPath, const <int>[]);
      var file = await openBytesFile(emptyPath, readOnly: false);
      try {
        await expectLater(
          ParquetStreamWriter.resume(table.schema, file, emptyMetadata),
          throwsA(isA<ParquetFormatException>()),
        );
      } finally {
        await file.close();
      }

      final badPath = '${temp.path}/bad_magic.parquet';
      await _writeFileBytes(badPath, 'NOPE'.codeUnits);
      file = await openBytesFile(badPath, readOnly: false);
      try {
        await expectLater(
          ParquetStreamWriter.resume(table.schema, file, emptyMetadata),
          throwsA(
            isA<ParquetFormatException>().having(
              (error) => error.toString(),
              'message',
              contains('PAR1'),
            ),
          ),
        );
      } finally {
        await file.close();
      }
    });

    test('rejects a nonzero num_rows mismatch', () async {
      final table = readFixtureTable('flat_multi_rowgroup.parquet');
      final path = '${temp.path}/bad_rows.parquet';
      final (_, original) = await _abandonedPrefix(
        path,
        table.schema,
        table,
        const ParquetWriterOptions(),
      );
      final bad = FileMetaData(
        version: original.version,
        schema: original.schema,
        numRows: original.numRows + 1,
        rowGroups: original.rowGroups,
        keyValueMetadata: original.keyValueMetadata,
        createdBy: original.createdBy,
        columnOrders: original.columnOrders,
      );
      final file = await openBytesFile(path, readOnly: false);
      try {
        await expectLater(
          ParquetStreamWriter.resume(table.schema, file, bad),
          throwsA(
            isA<ParquetFormatException>().having(
              (error) => error.toString(),
              'message',
              contains('num_rows'),
            ),
          ),
        );
      } finally {
        await file.close();
      }
    });

    test('rejects a schema mismatch', () async {
      final table = readFixtureTable('flat_multi_rowgroup.parquet');
      final path = '${temp.path}/bad_schema.parquet';
      final (_, original) = await _abandonedPrefix(
        path,
        table.schema,
        table,
        const ParquetWriterOptions(),
      );
      // Same column count but the first column has a different type.
      final wrongSchema =
          (ParquetSchemaBuilder()
                ..addInt32('i64')
                ..addBinary('str', utf8: true)
                ..addInt32('nullable_i32')
                ..addInt64('ts_us')
                ..addFloat64('f64')
                ..addBoolean('bool'))
              .build();
      final file = await openBytesFile(path, readOnly: false);
      try {
        await expectLater(
          ParquetStreamWriter.resume(wrongSchema, file, original),
          throwsA(
            isA<ParquetFormatException>().having(
              (error) => error.toString(),
              'message',
              contains('schema mismatch'),
            ),
          ),
        );
      } finally {
        await file.close();
      }
    });

    test('rejects a renamed column', () async {
      final table = readFixtureTable('flat_multi_rowgroup.parquet');
      final path = '${temp.path}/renamed.parquet';
      final (_, original) = await _abandonedPrefix(
        path,
        table.schema,
        table,
        const ParquetWriterOptions(),
      );
      final elements = original.schema.toList();
      final leaf = elements[1];
      elements[1] = SchemaElement(
        type: leaf.type,
        typeLength: leaf.typeLength,
        repetitionType: leaf.repetitionType,
        name: 'renamed',
        numChildren: leaf.numChildren,
        convertedType: leaf.convertedType,
        scale: leaf.scale,
        precision: leaf.precision,
        fieldId: leaf.fieldId,
        logicalType: leaf.logicalType,
      );
      final wrongSchema = ParquetSchema.fromSchemaElements(elements);
      final file = await openBytesFile(path, readOnly: false);
      try {
        await expectLater(
          ParquetStreamWriter.resume(wrongSchema, file, original),
          throwsA(isA<ParquetFormatException>()),
        );
      } finally {
        await file.close();
      }
    });
  });
}

const List<int> _magic = <int>[0x50, 0x41, 0x52, 0x31];

FileMetaData _metadataOf(Uint8List bytes) =>
    ParquetReader.fromBytes(bytes).metadata.fileMetadata;

int _fnv(Uint8List bytes) {
  var hash = 0x811C9DC5;
  for (final byte in bytes) {
    hash ^= byte;
    hash = (hash * 0x01000193) & 0xFFFFFFFF;
  }
  return hash;
}

Future<void> _writeFileBytes(String path, List<int> bytes) async {
  final file = await openBytesFile(path, create: true, readOnly: false);
  if (bytes.isNotEmpty) {
    await file.writeAt(0, Uint8List.fromList(bytes));
  }
  await file.truncate(bytes.length);
  await file.flush();
  await file.close();
}

/// Writes [groups] with a stream writer, returns the completed file bytes.
Future<Uint8List> _writeStreamedFile(
  String path,
  ParquetSchema schema,
  List<ParquetTable> groups, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
}) async {
  final file = await openBytesFile(path, create: true, readOnly: false);
  final writer = ParquetStreamWriter(schema, file, options: options);
  for (final group in groups) {
    await writer.add(group);
  }
  await writer.finish(close: true);
  return File(path).readAsBytesSync();
}

/// Abandons a writer after adding one row group (no footer) and returns the
/// prefix bytes and the metadata the writer had persisted.
Future<(Uint8List, FileMetaData)> _abandonedPrefix(
  String path,
  ParquetSchema schema,
  ParquetTable group,
  ParquetWriterOptions options,
) async {
  final file = await openBytesFile(path, create: true, readOnly: false);
  final writer = ParquetStreamWriter(schema, file, options: options);
  await writer.add(group);
  await file.flush();
  final metadata = writer.fileMetaData;
  await file.close();
  return (File(path).readAsBytesSync(), metadata);
}
