@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  final reader = ParquetReader.fromBytes(
    File('test/fixtures/plain_snappy.parquet').readAsBytesSync(),
  );

  test('column predicate prunes via statistics and filters rows', () {
    final predicate = ColumnPredicate('i64', FilterOperator.lt, -2990);
    final table = reader.readTableSync(predicate: predicate);
    expect(table.numRows, 2);
    final i64 = table['i64'] as Int64Column;
    expect(i64.getInt(0), -3000);
    expect(i64.getInt(1), -2993);
  });

  test('and/or predicates compose', () {
    final predicate = AndPredicate([
      ColumnPredicate('i64', FilterOperator.gtEq, 0),
      ColumnPredicate('nullable_i32', FilterOperator.isNull),
    ]);
    final table = reader.readTableSync(predicate: predicate);
    expect(table.numRows, greaterThan(0));
    for (var i = 0; i < table.numRows; i++) {
      expect((table['i64'] as Int64Column).getInt(i), greaterThanOrEqualTo(0));
      expect((table['nullable_i32'] as Int32Column).isNull(i), isTrue);
    }
  });

  test('statistics predicate returns false for impossible range', () {
    final predicate = ColumnPredicate('i64', FilterOperator.gt, 1000000);
    final table = reader.readTableSync(predicate: predicate);
    expect(table.numRows, 0);
  });

  test('string predicate uses row filtering', () {
    final table = reader.readTableSync(
      predicate: ColumnPredicate('str', FilterOperator.eq, 'value-00007'),
    );
    expect(table.numRows, 1);
    expect((table['str'] as BinaryColumn).getString(0), 'value-00007');
  });

  test('nested row filtering is explicit unsupported', () {
    final nested = ParquetReader.fromBytes(
      File('test/fixtures/nested_uncompressed.parquet').readAsBytesSync(),
    );
    expect(
      () => nested.readTableSync(
        predicate: ColumnPredicate('id', FilterOperator.gt, 5),
      ),
      throwsA(isA<ParquetUnsupportedException>()),
    );
  });
}
