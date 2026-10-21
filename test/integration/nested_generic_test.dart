@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/column/page_reader.dart';
import 'package:parquet/src/table/nested_assembler.dart';
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const String _official = 'test/fixtures/interop/official';

ParquetReader _open(String name) =>
    ParquetReader.fromBytes(File('$_official/$name').readAsBytesSync());

ParquetTable _read(String name) => _open(name).readTableSync();

/// Canonicalises nested values so map iteration order is irrelevant.
Object? _canon(Object? value) {
  if (value is Map) {
    final pairs = <List<Object?>>[
      for (final entry in value.entries)
        <Object?>[_canon(entry.key), _canon(entry.value)],
    ];
    pairs.sort((a, b) => a[0].toString().compareTo(b[0].toString()));
    return pairs;
  }
  if (value is List) return <Object?>[for (final item in value) _canon(item)];
  return value;
}

void _expectValues(
  ParquetColumn column,
  List<Object?> expected, {
  String reason = '',
}) {
  expect(column.length, expected.length, reason: '$reason length');
  for (var row = 0; row < expected.length; row++) {
    expect(
      _canon(column.value(row)),
      _canon(expected[row]),
      reason: '$reason row $row',
    );
  }
}

void _expectSameTable(ParquetTable original, ParquetTable back) {
  expect(back.numRows, original.numRows, reason: 'row count');
  expect(
    back.columns.map((column) => column.name),
    original.columns.map((column) => column.name),
    reason: 'column names',
  );
  expect(
    back.columns.map((column) => column.runtimeType),
    original.columns.map((column) => column.runtimeType),
    reason: 'column classes',
  );
  for (var i = 0; i < original.columns.length; i++) {
    final a = original.columns[i];
    final b = back.columns[i];
    expect(b.length, a.length, reason: '${a.name} length');
    for (var row = 0; row < a.length; row++) {
      expect(
        _canon(b.value(row)),
        _canon(a.value(row)),
        reason: '${a.name} row $row',
      );
    }
  }
}

/// Writes [table] with the schema it was read with, then reads it back.
ParquetTable _roundtrip(
  ParquetTable table,
  ParquetSchema schema, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
}) {
  final writer = ParquetWriter(schema: schema, options: options);
  writer.add(table);
  return ParquetReader.fromBytes(writer.finish()).readTableSync();
}

void _expectRoundtrip(String name) {
  final reader = _open(name);
  final table = reader.readTableSync();
  for (final options in <ParquetWriterOptions>[
    const ParquetWriterOptions(),
    const ParquetWriterOptions(
      compression: CompressionType.snappy,
      dataPageVersion: 2,
      pageSize: 64,
    ),
  ]) {
    final back = _roundtrip(table, reader.schema, options: options);
    _expectSameTable(table, back);
    expect(
      back.schema.columns.map((column) => column.dotPath),
      reader.schema.columns.map((column) => column.dotPath),
      reason: '$name leaf paths',
    );
  }
}

Validity _validity(List<bool> nulls) {
  final validity = Validity(nulls.length);
  for (var i = 0; i < nulls.length; i++) {
    if (nulls[i]) validity.setNull(i);
  }
  return validity;
}

Int32Column _int32(String name, List<int?> values) {
  final validity = Validity(values.length);
  final data = Int32List(values.length);
  for (var i = 0; i < values.length; i++) {
    final value = values[i];
    if (value == null) {
      validity.setNull(i);
    } else {
      data[i] = value;
    }
  }
  return Int32Column(name: name, values: data, validity: validity);
}

BinaryColumn _strings(String name, List<String?> values) {
  final validity = Validity(values.length);
  final bytes = <int>[];
  final offsets = Uint32List(values.length + 1);
  for (var i = 0; i < values.length; i++) {
    final value = values[i];
    if (value == null) {
      validity.setNull(i);
    } else {
      bytes.addAll(utf8.encode(value));
    }
    offsets[i + 1] = bytes.length;
  }
  return BinaryColumn(
    name: name,
    data: Uint8List.fromList(bytes),
    offsets: offsets,
    validity: validity,
    isUtf8: true,
  );
}

StructColumn _struct(
  String name,
  Map<String, ParquetColumn> fields, {
  List<bool>? nulls,
}) => StructColumn(
  name: name,
  length: fields.values.first.length,
  fields: fields,
  validity: _validity(
    nulls ?? List<bool>.filled(fields.values.first.length, false),
  ),
);

ListColumn _list(
  String name,
  List<int> offsets,
  ParquetColumn child, {
  List<bool>? nulls,
}) {
  final length = offsets.length - 1;
  return ListColumn(
    name: name,
    offsets: Uint32List.fromList(offsets),
    child: child,
    validity: _validity(nulls ?? List<bool>.filled(length, false)),
  );
}

MapColumn _map(
  String name,
  List<int> offsets,
  ParquetColumn keys,
  ParquetColumn values, {
  List<bool>? nulls,
}) {
  final length = offsets.length - 1;
  return MapColumn(
    name: name,
    offsets: Uint32List.fromList(offsets),
    keys: keys,
    values: values,
    validity: _validity(nulls ?? List<bool>.filled(length, false)),
  );
}

ParquetSchema _schema(List<SchemaElement> elements) =>
    ParquetSchema.fromSchemaElements(elements);

ParquetTable _table(
  ParquetSchema schema,
  List<ParquetColumn> columns,
  int numRows,
) => ParquetTable(schema: schema, columns: columns, numRows: numRows);

const SchemaElement _root = SchemaElement(name: 'schema', numChildren: 1);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('generic reader: official fixtures', () {
    test('nested_lists is a three-level list of nullable strings', () {
      final table = _read('nested_lists.snappy.parquet');
      expect(table.numRows, 3);
      final a = table['a'] as ListColumn;
      // No silent flattening: three nested list columns.
      expect(a.child, isA<ListColumn>());
      expect((a.child as ListColumn).child, isA<ListColumn>());
      expect(
        ((a.child as ListColumn).child as ListColumn).child,
        isA<BinaryColumn>(),
      );
      _expectValues(a, <Object?>[
        [
          [
            ['a', 'b'],
            ['c'],
          ],
          [
            null,
            ['d'],
          ],
        ],
        [
          [
            ['a', 'b'],
            ['c', 'd'],
          ],
          [
            null,
            ['e'],
          ],
        ],
        [
          [
            ['a', 'b'],
            ['c', 'd'],
            ['e'],
          ],
          [
            null,
            ['f'],
          ],
        ],
      ], reason: 'a');
      _expectValues(table['b'] as Int32Column, <Object?>[1, 1, 1], reason: 'b');
    });

    test('nested_maps is a map of maps with null and empty values', () {
      final table = _read('nested_maps.snappy.parquet');
      expect(table.numRows, 6);
      final a = table['a'] as MapColumn;
      expect(a.values, isA<MapColumn>(), reason: 'map value must stay nested');
      _expectValues(a, <Object?>[
        {
          'a': {1: true, 2: false},
        },
        {
          'b': {1: true},
        },
        {'c': null},
        {'d': <int, bool>{}},
        {
          'e': {1: true},
        },
        {
          'f': {3: true, 4: false, 5: true},
        },
      ], reason: 'a');
      _expectValues(table['b'] as Int32Column, <Object?>[
        1,
        1,
        1,
        1,
        1,
        1,
      ], reason: 'b');
      _expectValues(table['c'] as Float64Column, <Object?>[
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
      ], reason: 'c');
    });

    test('nonnullable.impala reads deep required combinations', () {
      final table = _read('nonnullable.impala.parquet');
      expect(table.numRows, 1);
      _expectValues(table['ID'] as Int64Column, <Object?>[8], reason: 'ID');
      _expectValues(table['Int_Array'] as ListColumn, <Object?>[
        [-1],
      ], reason: 'Int_Array');
      _expectValues(table['int_array_array'] as ListColumn, <Object?>[
        [
          [-1, -2],
          <int>[],
        ],
      ], reason: 'int_array_array');
      _expectValues(table['Int_Map'] as MapColumn, <Object?>[
        {'k1': -1},
      ], reason: 'Int_Map');
      _expectValues(table['int_map_array'] as ListColumn, <Object?>[
        [
          <String, int>{},
          {'k1': 1},
          <String, int>{},
          <String, int>{},
        ],
      ], reason: 'int_map_array');
      _expectValues(table['nested_Struct'] as StructColumn, <Object?>[
        {
          'a': -1,
          'B': [-1],
          'c': {
            'D': [
              [
                {'e': -1, 'f': 'nonnullable'},
              ],
            ],
          },
          'G': <String, Object?>{},
        },
      ], reason: 'nested_Struct');
    });

    test('nullable.impala reads optional nested combinations', () {
      final table = _read('nullable.impala.parquet');
      expect(table.numRows, 7);
      _expectValues(table['id'] as Int64Column, <Object?>[
        1,
        2,
        3,
        4,
        5,
        6,
        7,
      ], reason: 'id');
      _expectValues(table['int_array'] as ListColumn, <Object?>[
        [1, 2, 3],
        [null, 1, 2, null, 3, null],
        <int>[],
        null,
        null,
        null,
        null,
      ], reason: 'int_array');
      _expectValues(table['int_array_Array'] as ListColumn, <Object?>[
        [
          [1, 2],
          [3, 4],
        ],
        [
          [null, 1, 2, null],
          [3, null, 4],
          <int>[],
          null,
        ],
        [null],
        <Object?>[],
        null,
        null,
        [
          null,
          [5, 6],
        ],
      ], reason: 'int_array_Array');
      _expectValues(table['int_map'] as MapColumn, <Object?>[
        {'k1': 1, 'k2': 100},
        {'k1': 2, 'k2': null},
        <String, int>{},
        <String, int>{},
        <String, int>{},
        null,
        {'k1': null, 'k3': null},
      ], reason: 'int_map');
      _expectValues(table['int_Map_Array'] as ListColumn, <Object?>[
        [
          {'k1': 1},
        ],
        [
          {'k3': null, 'k1': 1},
          null,
          <String, int>{},
        ],
        [null, null],
        <Object?>[],
        null,
        null,
        null,
      ], reason: 'int_Map_Array');
      _expectValues(table['nested_struct'] as StructColumn, <Object?>[
        {
          'A': 1,
          'b': [1],
          'C': {
            'd': [
              [
                {'E': 10, 'F': 'aaa'},
                {'E': -10, 'F': 'bbb'},
              ],
              [
                {'E': 11, 'F': 'c'},
              ],
            ],
          },
          'g': {
            'foo': {
              'H': {
                'i': [1.1],
              },
            },
          },
        },
        {
          'A': null,
          'b': [null],
          'C': {
            'd': [
              [
                {'E': null, 'F': null},
                {'E': 10, 'F': 'aaa'},
                {'E': null, 'F': null},
                {'E': -10, 'F': 'bbb'},
                {'E': null, 'F': null},
              ],
              [
                {'E': 11, 'F': 'c'},
                null,
              ],
              <Object?>[],
              null,
            ],
          },
          'g': {
            'g1': {
              'H': {
                'i': [2.2, null],
              },
            },
            'g2': {
              'H': {'i': <double>[]},
            },
            'g3': null,
            'g4': {
              'H': {'i': null},
            },
            'g5': {'H': null},
          },
        },
        {
          'A': null,
          'b': null,
          'C': {'d': <Object?>[]},
          'g': <String, Object?>{},
        },
        {
          'A': null,
          'b': null,
          'C': {'d': null},
          'g': null,
        },
        {
          'A': null,
          'b': null,
          'C': null,
          'g': {
            'foo': {
              'H': {
                'i': [2.2, 3.3],
              },
            },
          },
        },
        null,
        {
          'A': 7,
          'b': [2, 3, null],
          'C': {
            'd': [
              <Object?>[],
              [null],
              null,
            ],
          },
          'g': null,
        },
      ], reason: 'nested_struct');
    });

    test('old_list_structure uses the legacy two-level LIST rules', () {
      final table = _read('old_list_structure.parquet');
      expect(table.numRows, 1);
      final a = table['a'] as ListColumn;
      expect(a.child, isA<ListColumn>(), reason: 'outer element is a list');
      _expectValues(a, <Object?>[
        [
          [1, 2],
          [3, 4],
        ],
      ], reason: 'a');
    });

    test('map_no_value reads key-only MAPs with set semantics', () {
      final table = _read('map_no_value.parquet');
      expect(table.numRows, 3);
      _expectValues(table['my_map'] as MapColumn, <Object?>[
        {1: null, 2: null, 3: null},
        {4: null, 5: null, 6: null},
        {7: null, 8: null, 9: null},
      ], reason: 'my_map');
      final noValue = table['my_map_no_v'] as MapColumn;
      expect(noValue.values, isA<NullColumn>());
      _expectValues(noValue, <Object?>[
        {1: null, 2: null, 3: null},
        {4: null, 5: null, 6: null},
        {7: null, 8: null, 9: null},
      ], reason: 'my_map_no_v');
      _expectValues(table['my_list'] as ListColumn, <Object?>[
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
      ], reason: 'my_list');
    });

    test('repeated_no_annotation reads a bare repeated group', () {
      final table = _read('repeated_no_annotation.parquet');
      expect(table.numRows, 6);
      _expectValues(table['id'] as Int32Column, <Object?>[
        1,
        2,
        3,
        4,
        5,
        6,
      ], reason: 'id');
      final phones = table['phoneNumbers'] as StructColumn;
      expect(phones.fields['phone'], isA<ListColumn>());
      expect((phones.fields['phone'] as ListColumn).child, isA<StructColumn>());
      _expectValues(phones, <Object?>[
        null,
        null,
        {'phone': <Object?>[]},
        {
          'phone': [
            {'number': 5555555555, 'kind': null},
          ],
        },
        {
          'phone': [
            {'number': 1111111111, 'kind': 'home'},
          ],
        },
        {
          'phone': [
            {'number': 1111111111, 'kind': 'home'},
            {'number': 2222222222, 'kind': null},
            {'number': 3333333333, 'kind': 'mobile'},
          ],
        },
      ], reason: 'phoneNumbers');
    });

    test('repeated_primitive_no_list reads bare repeated leaves', () {
      final table = _read('repeated_primitive_no_list.parquet');
      expect(table.numRows, 4);
      _expectValues(table['Int32_list'] as ListColumn, <Object?>[
        [0, 1, 2, 3],
        <int>[],
        [4],
        [5, 6, 7, 8],
      ], reason: 'Int32_list');
      _expectValues(table['String_list'] as ListColumn, <Object?>[
        ['foo', 'zero', 'one', 'two'],
        ['three'],
        ['four'],
        ['five', 'six', 'seven', 'eight'],
      ], reason: 'String_list');
      _expectValues(table['group_of_lists'] as StructColumn, <Object?>[
        {
          'Int32_list_in_group': [0, 1, 2, 3],
          'String_list_in_group': ['foo', 'zero', 'one', 'two'],
        },
        {
          'Int32_list_in_group': <int>[],
          'String_list_in_group': ['three'],
        },
        {
          'Int32_list_in_group': [4],
          'String_list_in_group': ['four'],
        },
        {
          'Int32_list_in_group': [5, 6, 7, 8],
          'String_list_in_group': ['five', 'six', 'seven', 'eight'],
        },
      ], reason: 'group_of_lists');
    });

    test('projection still assembles generic nesting', () {
      final lists = _open('nested_lists.snappy.parquet');
      final projectedLists = lists.readTableSync(columns: ['a']);
      expect(projectedLists.columns.map((column) => column.name), ['a']);
      _expectValues(projectedLists['a'] as ListColumn, <Object?>[
        [
          [
            ['a', 'b'],
            ['c'],
          ],
          [
            null,
            ['d'],
          ],
        ],
        [
          [
            ['a', 'b'],
            ['c', 'd'],
          ],
          [
            null,
            ['e'],
          ],
        ],
        [
          [
            ['a', 'b'],
            ['c', 'd'],
            ['e'],
          ],
          [
            null,
            ['f'],
          ],
        ],
      ], reason: 'projected a');

      final maps = _open('nested_maps.snappy.parquet');
      final fullMaps = maps.readTableSync();
      final projectedMaps = maps.readTableSync(columns: ['a']);
      expect(projectedMaps.columns.map((column) => column.name), ['a']);
      _expectValues(projectedMaps['a'] as MapColumn, <Object?>[
        for (var row = 0; row < 6; row++)
          (fullMaps['a'] as MapColumn).value(row),
      ], reason: 'projected a');
    });

    for (final name in <String>[
      'nested_lists.snappy.parquet',
      'nested_maps.snappy.parquet',
      'nonnullable.impala.parquet',
      'nullable.impala.parquet',
      'old_list_structure.parquet',
      'map_no_value.parquet',
      'repeated_no_annotation.parquet',
      'repeated_primitive_no_list.parquet',
    ]) {
      test('writer roundtrip preserves $name', () => _expectRoundtrip(name));
    }
  });

  group('generic writer: synthetic nesting', () {
    test('legacy LIST rule 1: repeated leaf is the element', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'l',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'vals',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.repeated,
        ),
      ]);
      final column = _list('l', [0, 2, 3], _int32('vals', [10, 20, 30]));
      final table = _table(schema, <ParquetColumn>[column], 2);
      final back = _roundtrip(table, schema);
      _expectValues(back['l'] as ListColumn, <Object?>[
        [10, 20],
        [30],
      ], reason: 'l');
    });

    test('legacy LIST rule 2: repeated multi-field group is the element', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'l',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'bag',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 2,
        ),
        SchemaElement(
          name: 'a',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
        SchemaElement(
          name: 'b',
          type: PhysicalType.byteArray,
          repetitionType: FieldRepetitionType.optional,
          logicalType: StringLogicalType(),
        ),
      ]);
      final element = _struct('bag', <String, ParquetColumn>{
        'a': _int32('a', [1, 2]),
        'b': _strings('b', ['x', null]),
      });
      final column = _list('l', [0, 2], element);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      _expectValues(back['l'] as ListColumn, <Object?>[
        [
          {'a': 1, 'b': 'x'},
          {'a': 2, 'b': null},
        ],
      ], reason: 'l');
    });

    test('legacy LIST rule 3: repeated "array" group is the element', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'l',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'array',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'x',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
      final element = _struct('array', <String, ParquetColumn>{
        'x': _int32('x', [1, null]),
      });
      final column = _list('l', [0, 2], element);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      _expectValues(back['l'] as ListColumn, <Object?>[
        [
          {'x': 1},
          {'x': null},
        ],
      ], reason: 'l');
    });

    test('legacy LIST rule 3: "<name>_tuple" group is the element', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'l',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'l_tuple',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'x',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
      ]);
      final element = _struct('l_tuple', <String, ParquetColumn>{
        'x': _int32('x', [7]),
      });
      final column = _list('l', [0, 1], element);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      _expectValues(back['l'] as ListColumn, <Object?>[
        [
          {'x': 7},
        ],
      ], reason: 'l');
    });

    test('legacy LIST rule 3: a consumed MAP element roundtrips', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'l',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'array',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 1,
          logicalType: MapLogicalType(),
        ),
        SchemaElement(
          name: 'map',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 2,
        ),
        SchemaElement(
          name: 'key',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
        SchemaElement(
          name: 'value',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
      final element = _map(
        'array',
        [0, 1, 2],
        _int32('key', [1, 2]),
        _int32('value', [10, 20]),
      );
      final column = _list('l', [0, 2], element);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      final list = back['l'] as ListColumn;
      expect(list.child, isA<MapColumn>());
      _expectValues(list, <Object?>[
        [
          {1: 10},
          {2: 20},
        ],
      ], reason: 'l');
    });

    test('MAP_KEY_VALUE is recognised without an outer MAP annotation', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'm',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'key_value',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 2,
          convertedType: ConvertedType.mapKeyValue,
        ),
        SchemaElement(
          name: 'key',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
        SchemaElement(
          name: 'value',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
      final column = _map(
        'm',
        [0, 2],
        _int32('key', [1, 2]),
        _int32('value', [7, null]),
      );
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      final map = back['m'] as MapColumn;
      expect(map.getMap(0), {1: 7, 2: null});
    });

    test('MAP without a value keeps set semantics', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'm',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: MapLogicalType(),
        ),
        SchemaElement(
          name: 'key_value',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'key',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
      ]);
      final keys = _int32('key', [1, 2, 3]);
      final column = _map(
        'm',
        [0, 3],
        keys,
        NullColumn(name: 'value', length: 3),
      );
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      final map = back['m'] as MapColumn;
      expect(map.values, isA<NullColumn>());
      expect(map.getMap(0), {1: null, 2: null, 3: null});
    });

    test('bare repeated group roundtrips as a list of structs', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'g',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 2,
        ),
        SchemaElement(
          name: 'a',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
        SchemaElement(
          name: 'b',
          type: PhysicalType.byteArray,
          repetitionType: FieldRepetitionType.optional,
          logicalType: StringLogicalType(),
        ),
      ]);
      final element = _struct('g', <String, ParquetColumn>{
        'a': _int32('a', [10, 20]),
        'b': _strings('b', ['x', null]),
      });
      final column = _list('g', [0, 2], element);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      _expectValues(back['g'] as ListColumn, <Object?>[
        [
          {'a': 10, 'b': 'x'},
          {'a': 20, 'b': null},
        ],
      ], reason: 'g');
    });

    test('bare repeated leaf inside a list-of-struct element', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'l',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'list',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'element',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'vals',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.repeated,
        ),
      ]);
      final element = _struct('element', <String, ParquetColumn>{
        'vals': _list('vals', [0, 2], _int32('vals', [1, 2])),
      });
      final column = _list('l', [0, 1], element);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      final list = back['l'] as ListColumn;
      expect(list.child, isA<StructColumn>());
      _expectValues(list, <Object?>[
        [
          {
            'vals': [1, 2],
          },
        ],
      ], reason: 'l');
    });

    test('map value may be a list (including a null list)', () {
      final schema = _schema(const <SchemaElement>[
        _root,
        SchemaElement(
          name: 'm',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: MapLogicalType(),
        ),
        SchemaElement(
          name: 'key_value',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 2,
        ),
        SchemaElement(
          name: 'key',
          type: PhysicalType.byteArray,
          repetitionType: FieldRepetitionType.required,
          logicalType: StringLogicalType(),
        ),
        SchemaElement(
          name: 'value',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
          logicalType: ListLogicalType(),
        ),
        SchemaElement(
          name: 'list',
          repetitionType: FieldRepetitionType.repeated,
          numChildren: 1,
        ),
        SchemaElement(
          name: 'element',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
      final values = _list(
        'value',
        [0, 2, 2],
        _int32('element', [1, 2]),
        nulls: [false, true],
      );
      final column = _map('m', [0, 2], _strings('key', ['k1', 'k2']), values);
      final table = _table(schema, <ParquetColumn>[column], 1);
      final back = _roundtrip(table, schema);
      final map = back['m'] as MapColumn;
      expect(map.values, isA<ListColumn>());
      expect(map.getMap(0), {
        'k1': [1, 2],
        'k2': null,
      });
    });
  });

  group('level validation', () {
    final schema = _schema(const <SchemaElement>[
      _root,
      SchemaElement(
        name: 'l',
        repetitionType: FieldRepetitionType.optional,
        numChildren: 1,
        logicalType: ListLogicalType(),
      ),
      SchemaElement(
        name: 'list',
        repetitionType: FieldRepetitionType.repeated,
        numChildren: 1,
      ),
      SchemaElement(
        name: 'element',
        type: PhysicalType.int32,
        repetitionType: FieldRepetitionType.optional,
      ),
    ]);
    final node = schema.root.children.single;

    DecodedColumnChunk chunk(List<int> reps, List<int> defs) =>
        DecodedColumnChunk(
          numValues: defs.length,
          presentCount: 0,
          repLevels: Uint8List.fromList(reps),
          defLevels: Uint8List.fromList(defs),
          values: Int32List(0),
        );

    test('definition level above maxDef is rejected', () {
      expect(
        () => buildNodeColumn(node, schema, {
          0: chunk([0], [4]),
        }, 1),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('repetition level above maxRep is rejected', () {
      expect(
        () => buildNodeColumn(node, schema, {
          0: chunk([2], [0]),
        }, 1),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('first repetition level must be zero', () {
      expect(
        () => buildNodeColumn(node, schema, {
          0: chunk([1], [0]),
        }, 1),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('level stream row count must match the row group', () {
      expect(
        () => buildNodeColumn(node, schema, {
          0: chunk([0], [0]),
        }, 2),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });
}
