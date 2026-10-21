@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  late ParquetReader reader;

  setUpAll(() {
    reader = ParquetReader.fromBytes(
      File('test/fixtures/plain_snappy.parquet').readAsBytesSync(),
    );
  });

  test('sync batch matches the row group', () {
    final batch = reader.readBatchSync(0);
    final table = reader.readRowGroupSync(0);
    expect(batch.numRows, table.numRows);
    expect(batch.columns.map((c) => c.name), table.columns.map((c) => c.name));
    expect(batch['i64'].length, table['i64'].length);
  });

  test('async batch matches the row group', () async {
    final batch = await reader.readBatch(0);
    expect(batch.toTable().numRows, reader.readRowGroupSync(0).numRows);
  });

  test('batch stream covers every row group in order', () async {
    final batches = await reader.readBatches().toList();
    expect(batches.length, reader.numRowGroups);
    expect(
      batches.fold<int>(0, (sum, batch) => sum + batch.numRows),
      reader.numRows,
    );
  });
}
