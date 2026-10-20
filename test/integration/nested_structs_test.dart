@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  test('nested struct list + null map values + multiple row groups', () {
    final reader = ParquetReader.fromBytes(
      File('test/fixtures/nested_structs_multi.parquet').readAsBytesSync(),
    );
    expect(reader.numRowGroups, 3);
    final table = reader.readTableSync();
    expect(table.numRows, 120);

    final list = table['list_struct'] as ListColumn;
    final element = list.child as StructColumn;
    expect(list.getList(0), isEmpty);
    expect(list.getList(1), [
      {'x': 0, 'y': 's0'},
    ]);
    expect(list.getList(2), [
      {'x': 0, 'y': 's0'},
      {'x': 1, 'y': 's1'},
    ]);
    expect(
      element.length,
      [for (var i = 0; i < 120; i++) i % 3].fold(0, (a, b) => a + b),
    );

    final map = table['map_null'] as MapColumn;
    expect(map.getMap(0), isEmpty);
    expect(map.getMap(1), {'k0': null});
    expect(map.getMap(2), {'k0': null, 'k1': 1});
    expect(map.getMap(5), {'k0': null, 'k1': 1});

    for (var i = 0; i < 120; i++) {
      final values = list.getList(i);
      expect(values.length, i % 3, reason: 'list len[$i]');
      for (var j = 0; j < values.length; j++) {
        expect(values[j], {'x': j, 'y': 's$j'}, reason: 'list[$i][$j]');
      }
      final m = map.getMap(i);
      expect(m.length, i % 3, reason: 'map len[$i]');
      for (var j = 0; j < i % 3; j++) {
        expect(m['k$j'], j == 0 ? null : j, reason: 'map[$i][k$j]');
      }
    }
  });
}
