@TestOn('vm')
library;

import 'dart:io';

import 'package:blit/blit.dart' show openBytesFile;
import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  late Directory temp;
  late ParquetTable table;

  setUpAll(() {
    temp = Directory.systemTemp.createTempSync('parquet_stream_');
    table = ParquetReader.fromBytes(
      File('benchmark/fixtures/flat_snappy.parquet').readAsBytesSync(),
    ).readTableSync();
  });

  tearDownAll(() {
    temp.deleteSync(recursive: true);
  });

  test('stream writer appends row groups and reads back', () async {
    final path = '${temp.path}/stream.parquet';
    final file = await openBytesFile(path, create: true, readOnly: false);
    final writer = ParquetStreamWriter(
      table.schema,
      file,
      options: const ParquetWriterOptions(
        writePageIndex: true,
        writeBloomFilter: true,
        writeCrc: true,
      ),
    );
    await writer.add(table);
    await writer.add(table);
    await writer.finish(close: true);

    final bytes = File(path).readAsBytesSync();
    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.numRows, table.numRows * 2);
    expect(reader.metadata.rowGroups.length, 2);
    expect((await reader.readTable()).numRows, table.numRows * 2);

    final source = MemorySource.fromUint8List(bytes);
    for (final group in reader.metadata.rowGroups) {
      for (final chunk in group.columns) {
        expect(chunk.readOffsetIndex(source), isNotNull);
        expect(chunk.readBloomFilter(source), isNotNull);
      }
    }
  });

  test('stream writer rejects add after finish', () async {
    final path = '${temp.path}/finished.parquet';
    final file = await openBytesFile(path, create: true, readOnly: false);
    final writer = ParquetStreamWriter(table.schema, file);
    await writer.finish(close: true);
    await expectLater(writer.add(table), throwsStateError);
  });

  test('metadata sidecar preserves index and bloom offsets', () {
    final bytes = writeParquetTable(
      table,
      options: const ParquetWriterOptions(
        writePageIndex: true,
        writeBloomFilter: true,
      ),
    );
    final metadata = ParquetReader.fromBytes(bytes).metadata.fileMetadata;
    final restored = readFileMetaData(writeFileMetaData(metadata));
    for (final group in restored.rowGroups) {
      for (final chunk in group.columns) {
        expect(chunk.offsetIndexOffset, isNotNull);
        expect(chunk.metaData.bloomFilterOffset, isNotNull);
      }
    }
  });

  test('metadata sidecar round-trips', () {
    final bytes = writeParquetTable(table);
    final reader = ParquetReader.fromBytes(bytes);
    final metadata = reader.metadata.fileMetadata;
    final sidecar = writeFileMetaData(metadata);
    final restored = readFileMetaData(sidecar);
    expect(restored.numRows, metadata.numRows);
    expect(restored.schema.length, metadata.schema.length);
    expect(restored.rowGroups.length, metadata.rowGroups.length);
    expect(restored.rowGroups.first.numRows, metadata.rowGroups.first.numRows);
    expect(restored.createdBy, metadata.createdBy);
  });

  test('resume appends a row group to existing bytes and metadata', () {
    final first = ParquetWriter(
      schema: table.schema,
      options: const ParquetWriterOptions(
        writePageIndex: true,
        writeBloomFilter: true,
      ),
    );
    first.add(table);
    final prefix = first.finish();
    final metadata = ParquetReader.fromBytes(prefix).metadata.fileMetadata;

    final resumed = ParquetWriter.resume(
      prefix,
      metadata,
      options: const ParquetWriterOptions(
        writePageIndex: true,
        writeBloomFilter: true,
      ),
    );
    resumed.add(table);
    final bytes = resumed.finish();

    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.metadata.rowGroups.length, 2);
    expect(reader.readTableSync().numRows, table.numRows * 2);
    final source = MemorySource.fromUint8List(bytes);
    for (final group in reader.metadata.rowGroups) {
      for (final chunk in group.columns) {
        expect(chunk.readOffsetIndex(source), isNotNull);
        expect(chunk.readBloomFilter(source), isNotNull);
      }
    }
  });

  test('resume accepts a standalone metadata sidecar', () {
    final first = ParquetWriter(schema: table.schema);
    first.add(table);
    final prefix = first.finish();
    final sidecar = writeFileMetaData(
      ParquetReader.fromBytes(prefix).metadata.fileMetadata,
    );

    final resumed = ParquetWriter.resume(prefix, readFileMetaData(sidecar));
    resumed.add(table);
    final bytes = resumed.finish();

    expect(ParquetReader.fromBytes(bytes).readTableSync().numRows, 200000);
  });
}
