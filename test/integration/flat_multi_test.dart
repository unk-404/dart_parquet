@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  test('flat multi-row-group concat validates every value', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/flat_multi_rowgroup.parquet').readAsBytesSync(),
    );
    expect(reader.numRowGroups, 4);
    final table = reader.readTableSync();
    expect(table.numRows, 1000);

    final i64 = table['i64'] as Int64Column;
    final str = table['str'] as BinaryColumn;
    final nullable = table['nullable_i32'] as Int32Column;
    final ts = table['ts_us'] as Int64Column;
    final f64 = table['f64'] as Float64Column;
    final bools = table['bool'] as BooleanColumn;

    for (var i = 0; i < 1000; i++) {
      expect(i64.getInt(i), i * 7 - 3000, reason: 'i64[$i]');
      expect(
        str.getString(i),
        'value-${i.toString().padLeft(5, '0')}',
        reason: 'str[$i]',
      );
      if (i % 7 == 0) {
        expect(nullable.isNull(i), isTrue, reason: 'null[$i]');
      } else {
        expect(nullable.getInt(i), i, reason: 'i32[$i]');
      }
      expect(ts.getInt(i), 1700000000000000 + i, reason: 'ts[$i]');
      expect(f64.getDouble(i), closeTo(i / 3.0, 1e-9), reason: 'f64[$i]');
      expect(bools.getBool(i), i % 3 == 0, reason: 'bool[$i]');
    }
  });

  test('async native file source + projection + row limit', () async {
    final source = await openSource(
      'test/fixtures/flat_multi_rowgroup.parquet',
    );
    final reader = await ParquetReader.open(source);
    final table = await reader.readTable(columns: ['i64', 'f64'], maxRows: 10);
    expect(table.numRows, 10);
    expect(table.columns.map((c) => c.name), ['i64', 'f64']);
    final i64 = table['i64'] as Int64Column;
    expect(i64.getInt(0), -3000);
    expect(i64.getInt(9), -2937);
    await source.close();
  });

  test('row limit slices flat columns', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/plain_snappy.parquet').readAsBytesSync(),
    );
    final table = reader.readTableSync(maxRows: 7);
    expect(table.numRows, 7);
    expect(table['i64'].length, 7);
    expect((table['str'] as BinaryColumn).getString(6), 'value-00006');
  });
}
