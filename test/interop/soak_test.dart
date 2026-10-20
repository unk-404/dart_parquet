@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

/// Bounded streaming soak / memory-bound proxy for the v0.1.2 acceptance
/// target ("1 GB streamed by row group <= 256 MB peak"). A 1 GB fixture is not
/// committed; this builds a synthetic multi-row-group file and asserts that
/// `readRowGroups` keeps the resident-set growth far below 256 MB and that
/// repeated passes stay bounded and complete.
void main() {
  const int groups = 16;
  const int rowsPerGroup = 25_000;
  const int totalRows = groups * rowsPerGroup;
  const int memoryBoundBytes = 256 * 1024 * 1024;

  late Uint8List fileBytes;

  setUpAll(() {
    final schema =
        (ParquetSchemaBuilder()
              ..addInt64('id', nullable: false)
              ..addBinary('name', utf8: true)
              ..addFloat64('score'))
            .build();
    final writer = ParquetWriter(
      schema: schema,
      options: const ParquetWriterOptions(
        compression: CompressionType.snappy,
        pageSize: 64 * 1024,
      ),
    );
    for (var group = 0; group < groups; group++) {
      writer.add(_buildGroup(schema, group * rowsPerGroup, rowsPerGroup));
    }
    fileBytes = writer.finish();
    expect(writer.numRows, totalRows);
    expect(writer.numRowGroups, groups);
  });

  test('synthetic soak file is valid and multi-row-group', () {
    final reader = ParquetReader.fromBytes(fileBytes);
    expect(reader.numRows, totalRows);
    expect(reader.numRowGroups, groups);
    expect(fileBytes.length, greaterThan(1 << 20));
  });

  test('row-group streaming stays under the memory bound', () async {
    final reader = ParquetReader.fromBytes(fileBytes);
    final rssBefore = ProcessInfo.currentRss;
    var rows = 0;
    var observedGroups = 0;
    await for (final group in reader.readRowGroups()) {
      expect(group.numRows, rowsPerGroup);
      expect(group.columns.length, 3);
      final ids = group['id'] as Int64Column;
      expect(ids.getInt(0), observedGroups * rowsPerGroup);
      expect(
        ids.getInt(rowsPerGroup - 1),
        observedGroups * rowsPerGroup + rowsPerGroup - 1,
      );
      rows += group.numRows;
      observedGroups++;
      // Drop the reference: only one group should be live at a time.
    }
    final rssAfter = ProcessInfo.currentRss;
    expect(observedGroups, groups);
    expect(rows, totalRows);
    final growth = rssAfter - rssBefore;
    expect(
      growth,
      lessThan(memoryBoundBytes),
      reason:
          'RSS grew by ${growth ~/ (1 << 20)} MiB, bound is '
          '${memoryBoundBytes ~/ (1 << 20)} MiB',
    );
  });

  test('repeated streaming passes complete and remain bounded', () async {
    final stopwatch = Stopwatch()..start();
    for (var pass = 0; pass < 3; pass++) {
      final reader = ParquetReader.fromBytes(fileBytes);
      var rows = 0;
      await for (final group in reader.readRowGroups()) {
        rows += group.numRows;
      }
      expect(rows, totalRows, reason: 'pass $pass');
    }
    stopwatch.stop();
    expect(stopwatch.elapsed, lessThan(const Duration(seconds: 60)));
  });

  test('maxRows truncation across row groups', () {
    final reader = ParquetReader.fromBytes(fileBytes);
    final table = reader.readTableSync(maxRows: 1000);
    expect(table.numRows, 1000);
    expect((table['id'] as Int64Column).getInt(999), 999);
  });

  test('row-group filter skips without decoding', () {
    final reader = ParquetReader.fromBytes(fileBytes);
    var visited = 0;
    final table = reader.readTableSync(
      rowGroupFilter: (group) {
        visited++;
        return group.index == 0;
      },
    );
    expect(visited, groups);
    expect(table.numRows, rowsPerGroup);
  });
}

ParquetTable _buildGroup(ParquetSchema schema, int start, int count) {
  final ids = Int64Buffer(count);
  final scores = Float64List(count);
  final names = <int>[];
  final offsets = Uint32List(count + 1);
  for (var i = 0; i < count; i++) {
    final row = start + i;
    ids.setInt(i, row);
    scores[i] = row / 3.0;
    final encoded = utf8.encode('row_$row');
    names.addAll(encoded);
    offsets[i + 1] = names.length;
  }
  return ParquetTable(
    schema: schema,
    columns: <ParquetColumn>[
      Int64Column(name: 'id', values: ids, validity: Validity(count)),
      BinaryColumn(
        name: 'name',
        data: Uint8List.fromList(names),
        offsets: offsets,
        validity: Validity(count),
        isUtf8: true,
      ),
      Float64Column(name: 'score', values: scores, validity: Validity(count)),
    ],
    numRows: count,
  );
}
