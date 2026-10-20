@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  final fixtures = <String>[
    'plain_uncompressed.parquet',
    'plain_snappy.parquet',
    'dict_snappy.parquet',
    'plain_lz4.parquet',
    'plain_zstd.parquet',
    'plain_gzip.parquet',
    'plain_brotli.parquet',
    'delta_int.parquet',
    'delta_str.parquet',
    'delta_len_str.parquet',
    'bss.parquet',
    'v2_pages.parquet',
  ];

  for (final name in fixtures) {
    test('flat read $name', () {
      final path = 'test/fixtures/$name';
      final reader = ParquetReader.fromBytes(File(path).readAsBytesSync());
      expect(reader.numRows, 1000);
      final table = reader.readTableSync();
      expect(table.numRows, 1000);
      expect(table.columns.length, 9);

      final i64 = table['i64'] as Int64Column;
      expect(i64.getInt(0), -3000);
      expect(i64.getInt(999), 3993);

      final str = table['str'] as BinaryColumn;
      expect(str.getString(0), 'value-00000');
      expect(str.getString(999), 'value-00999');

      final nullable = table['nullable_i32'] as Int32Column;
      expect(nullable.isNull(0), isTrue);
      expect(nullable.getInt(1), 1);
      expect(nullable.isNull(7), isTrue);
      expect(nullable.getInt(8), 8);

      final f64 = table['f64'] as Float64Column;
      expect(f64.getDouble(0), 0.0);
      expect(f64.getDouble(3), 1.0);

      final boolCol = table['bool'] as BooleanColumn;
      expect(boolCol.getBool(0), isTrue);
      expect(boolCol.getBool(1), isFalse);
      expect(boolCol.getBool(3), isTrue);

      final fixed = table['fixed16'] as FixedLenColumn;
      expect(fixed.bytesAt(5).toUint8List(), List<int>.filled(16, 5));

      final bin = table['bin'] as BinaryColumn;
      expect(bin.getBytes(2), [2, 0, 255]);
    });
  }

  test('projection reads only requested columns', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/plain_snappy.parquet').readAsBytesSync(),
    );
    final table = reader.readTableSync(columns: ['i64', 'str']);
    expect(table.columns.length, 2);
    expect(table.column('i64'), isNotNull);
    expect(table.column('str'), isNotNull);
    expect(table.column('f64'), isNull);
  });
}
