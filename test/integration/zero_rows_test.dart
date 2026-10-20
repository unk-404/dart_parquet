@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  test('zero-row pyarrow file', () {
    final r = ParquetReader.fromBytes(
      File('test/fixtures/zero_rows.parquet').readAsBytesSync(),
    );
    expect(r.numRows, 0);
    final t = r.readTableSync();
    expect(t.numRows, 0);
    expect(t.columns.length, 2);
  });
}
