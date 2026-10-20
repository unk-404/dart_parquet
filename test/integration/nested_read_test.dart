@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  for (final name in ['nested_uncompressed.parquet', 'nested_snappy.parquet']) {
    test('nested read $name', () {
      final reader = ParquetReader.fromBytes(
        File('test/fixtures/$name').readAsBytesSync(),
      );
      final table = reader.readTableSync();
      expect(table.numRows, 200);
      expect(table.columns.map((c) => c.name), [
        'id',
        'list_int',
        'struct',
        'map',
      ]);

      final id = table['id'] as Int32Column;
      expect(id.getInt(0), 0);
      expect(id.getInt(199), 199);

      final list = table['list_int'] as ListColumn;
      final child = list.child as Int32Column;
      expect(list.isNull(0), isFalse);
      expect(list.getList(0), isEmpty);
      expect(list.getList(1), [0]);
      expect(list.getList(2), [0, 1]);
      expect(list.getList(3), [0, 1, 2]);
      expect(list.getList(4), isEmpty);
      expect(
        child.length,
        [for (var i = 0; i < 200; i++) i % 4].fold(0, (a, b) => a + b),
      );

      final struct = table['struct'] as StructColumn;
      expect(struct.isNull(0), isTrue);
      expect(struct.isNull(1), isFalse);
      final s1 = struct.getStruct(1);
      expect(s1['a'], 1);
      expect(s1['b'], 's1');
      expect(struct.isNull(2), isFalse);
      expect(struct.getStruct(2)['a'], 2);
      expect(struct.isNull(3), isTrue);

      final map = table['map'] as MapColumn;
      expect(map.getMap(0), isEmpty);
      expect(map.getMap(1), {'k0': 0});
      expect(map.getMap(2), {'k0': 0, 'k1': 1});
      expect(map.getMap(3), {});

      for (var i = 0; i < 200; i++) {
        expect(id.getInt(i), i, reason: 'id[$i]');
        final values = list.getList(i);
        expect(values.length, i % 4, reason: 'list len[$i]');
        for (var j = 0; j < values.length; j++) {
          expect(values[j], j, reason: 'list[$i][$j]');
        }
        if (i % 3 == 0) {
          expect(struct.isNull(i), isTrue, reason: 'struct null[$i]');
        } else {
          expect(struct.isNull(i), isFalse, reason: 'struct[$i]');
          final s = struct.getStruct(i);
          expect(s['a'], i, reason: 'struct.a[$i]');
          expect(s['b'], 's$i', reason: 'struct.b[$i]');
        }
        final m = map.getMap(i);
        expect(m.length, i % 3, reason: 'map len[$i]');
        for (var j = 0; j < i % 3; j++) {
          expect(m['k$j'], j, reason: 'map[$i][k$j]');
        }
      }
    });
  }
}
