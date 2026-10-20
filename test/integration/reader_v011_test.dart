@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  final bytes = File('test/fixtures/flat_multi_rowgroup.parquet')
      .readAsBytesSync();

  test('row-group filter skips groups before decoding', () {
    final reader = ParquetReader.fromBytes(bytes);
    var calls = 0;
    final table = reader.readTableSync(
      rowGroupFilter: (group) {
        calls++;
        // Keep row groups 0 and 2 (250 rows each).
        return group.index == 0 || group.index == 2;
      },
    );
    expect(calls, 4);
    expect(table.numRows, 500);
    final i64 = table['i64'] as Int64Column;
    expect(i64.getInt(0), -3000);
    expect(i64.getInt(249), 249 * 7 - 3000);
    expect(i64.getInt(250), 500 * 7 - 3000);
    expect(i64.getInt(499), 749 * 7 - 3000);
  });

  test('readRowGroups streams one table per group', () async {
    final reader = ParquetReader.fromBytes(bytes);
    final sizes = <int>[];
    await for (final group in reader.readRowGroups()) {
      sizes.add(group.numRows);
    }
    expect(sizes, [250, 250, 250, 250]);
  });

  test('async stream applies the filter and maxRows', () async {
    final source = await openSource(
      'test/fixtures/flat_multi_rowgroup.parquet',
    );
    final reader = await ParquetReader.open(source);
    final sizes = <int>[];
    await for (final group in reader.readRowGroups(
      rowGroupFilter: (g) => g.index.isEven,
      maxRows: 300,
    )) {
      sizes.add(group.numRows);
    }
    expect(sizes, [250, 50]);
    await source.close();
  });

  test('stream tables preserve projection schema', () async {
    final reader = ParquetReader.fromBytes(bytes);
    final group = await reader.readRowGroups(columns: ['i64']).first;
    expect(group.columns.map((c) => c.name), ['i64']);
    expect(group.numRows, 250);
  });

  test('nested projection selects a struct field only', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/nested_uncompressed.parquet').readAsBytesSync(),
    );
    final table = reader.readTableSync(columns: ['struct.a']);
    expect(table.columns.map((c) => c.name), ['struct']);
    final struct = table['struct'] as StructColumn;
    expect(struct.fields.keys, ['a']);
    expect(struct.isNull(0), isTrue);
    expect(struct.getStruct(1)['a'], 1);
    expect(struct.getStruct(2)['a'], 2);
  });

  test('nested projection selects a list element path', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/nested_uncompressed.parquet').readAsBytesSync(),
    );
    final table = reader.readTableSync(columns: ['list_int.list.element']);
    final list = table['list_int'] as ListColumn;
    expect(list.getList(0), isEmpty);
    expect(list.getList(1), [0]);
    expect(list.getList(2), [0, 1]);
  });

  test('map projection keeps key and value leaves', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/nested_uncompressed.parquet').readAsBytesSync(),
    );
    final table = reader.readTableSync(columns: ['map.key_value.value']);
    final map = table['map'] as MapColumn;
    expect(map.getMap(1), {'k0': 0});
    expect(map.getMap(2), {'k0': 0, 'k1': 1});
  });

  test('unknown projected column throws', () {
    final reader = ParquetReader.fromBytes(bytes);
    expect(
      () => reader.readTableSync(columns: ['nope']),
      throwsA(isA<ParquetFormatException>()),
    );
  });

  test('async readTable auto-parallel matches sync on the VM', () async {
    final benchBytes = File('benchmark/fixtures/flat_snappy.parquet')
        .readAsBytesSync();
    final sync = ParquetReader.fromBytes(benchBytes).readTableSync();
    final parallel = await ParquetReader.fromBytes(benchBytes).readTable();
    expect(parallel.numRows, sync.numRows);
    final a = sync['i64'] as Int64Column;
    final b = parallel['i64'] as Int64Column;
    for (var i = 0; i < sync.numRows; i++) {
      expect(b.getInt(i), a.getInt(i), reason: 'row $i');
    }
  });

  test('explicit parallelism decodes a nested row group set', () async {
    final nestedBytes = File('benchmark/fixtures/nested_snappy.parquet')
        .readAsBytesSync();
    final sync = ParquetReader.fromBytes(nestedBytes).readTableSync();
    final parallel = await ParquetReader.fromBytes(
      nestedBytes,
      options: const ParquetReaderOptions(parallelism: 2),
    ).readTable();
    expect(parallel.numRows, sync.numRows);
    expect(
      (parallel['lst'] as ListColumn).getList(2),
      (sync['lst'] as ListColumn).getList(2),
    );
  });

  test('reads OffsetIndex and ColumnIndex', () {
    final indexBytes = File('test/fixtures/page_index.parquet')
        .readAsBytesSync();
    final reader = ParquetReader.fromBytes(indexBytes);
    final source = MemorySource.fromUint8List(indexBytes);
    final chunk = reader.metadata.rowGroup(0).column(0);
    expect(chunk.hasDataPage, isTrue);
    final offsetIndex = chunk.readOffsetIndex(source);
    expect(offsetIndex, isNotNull);
    expect(offsetIndex!.pageLocations, isNotEmpty);
    expect(offsetIndex.pageLocations.first.firstRowIndex, 0);
    expect(offsetIndex.pageLocations.first.offset, greaterThan(0));

    final columnIndex = chunk.readColumnIndex(source);
    expect(columnIndex, isNotNull);
    expect(columnIndex!.nullPages, isNotEmpty);
    expect(columnIndex.minValues.length, columnIndex.maxValues.length);
  });
}
