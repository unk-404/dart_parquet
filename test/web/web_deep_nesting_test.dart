/// v0.2 Web/Wasm deep-nesting matrix.
///
/// Covers arbitrary LIST/STRUCT/MAP combinations, legacy two-level LIST rules,
/// bare repeated fields, MAP without a value and required/optional mixes. The
/// fixtures are base64 constants (see `web_fixtures.dart`); synthetic tables are
/// built in memory and written with the Dart writer. This suite is shared with
/// `test/wasm/wasm_deep_nesting_test.dart`.
@TestOn('browser')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'deep_fixture_expectations.dart';
import 'web_fixtures.dart';

void main() {
  group('generated deep fixtures: read', () {
    test('list<list<int32>>', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_list_list.parquet'),
      );
      expect(reader.numRows, 4);
      expectLeafPaths(reader, <String>['id', 'll.list.element.list.element']);
      expectDeepListList(reader.readTableSync());
    });

    test('list<struct> and list<struct{list}>', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_list_struct.parquet'),
      );
      expect(reader.numRows, 4);
      expectLeafPaths(reader, <String>[
        'id',
        'ls.list.element.a',
        'ls.list.element.b',
        'lsn.list.element.a',
        'lsn.list.element.tags.list.element',
      ]);
      expectDeepListStruct(reader.readTableSync());
    });

    test('struct{list,map,struct{list}}', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_struct_children.parquet'),
      );
      expect(reader.numRows, 4);
      expectLeafPaths(reader, <String>[
        's.id',
        's.tags.list.element',
        's.attrs.key_value.key',
        's.attrs.key_value.value',
        's.inner.x',
        's.inner.ys.list.element',
      ]);
      expectDeepStructChildren(reader.readTableSync());
    });

    test('map<list> and map<struct{list}>', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_map_children.parquet'),
      );
      expect(reader.numRows, 4);
      expectLeafPaths(reader, <String>[
        'id',
        'ml.key_value.key',
        'ml.key_value.value.list.element',
        'ms.key_value.key',
        'ms.key_value.value.a',
        'ms.key_value.value.b.list.element',
      ]);
      expectDeepMapChildren(reader.readTableSync());
    });
  });

  group('vendored apache/parquet-testing deep fixtures: read', () {
    test('three-level list with null elements', () {
      expectOfficialNestedLists(
        ParquetReader.fromBytes(webFixture('nested_lists.snappy.parquet'))
            .readTableSync(),
      );
    });

    test('map of maps with null/empty values', () {
      expectOfficialNestedMaps(
        ParquetReader.fromBytes(webFixture('nested_maps.snappy.parquet'))
            .readTableSync(),
      );
    });

    test('legacy two-level LIST rule', () {
      expectOfficialOldListStructure(
        ParquetReader.fromBytes(webFixture('old_list_structure.parquet'))
            .readTableSync(),
      );
    });

    test('MAP without a value field', () {
      final reader = ParquetReader.fromBytes(
        webFixture('map_no_value.parquet'),
      );
      expectOfficialMapNoValue(reader.readTableSync());
      final map = reader.readTableSync()['my_map_no_v'] as MapColumn;
      expect(map.values, isA<NullColumn>());
    });

    test('bare repeated group without LIST annotation', () {
      expectOfficialBareRepeatedGroup(
        ParquetReader.fromBytes(webFixture('repeated_no_annotation.parquet'))
            .readTableSync(),
      );
    });

    test('bare repeated leaves without LIST annotation', () {
      expectOfficialBareRepeatedLeaf(
        ParquetReader.fromBytes(
          webFixture('repeated_primitive_no_list.parquet'),
        ).readTableSync(),
      );
    });

    test('empty list column', () {
      expectOfficialNullList(
        ParquetReader.fromBytes(webFixture('null_list.parquet'))
            .readTableSync(),
      );
    });
  });

  group('deep projection', () {
    test('generated list<struct> keeps only the selected field', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_list_struct.parquet'),
      );
      final table = reader.readTableSync(columns: ['ls.list.element.a']);
      expect(table.columns.map((column) => column.name).toList(), <String>[
        'ls',
      ]);
      final ls = table['ls'] as ListColumn;
      final element = ls.child as StructColumn;
      expect(element.fields.keys, <String>['a']);
      expectColumnValues(ls, <Object?>[
        [
          {'a': 1},
          null,
        ],
        <Object?>[],
        null,
        [
          {'a': 2},
        ],
      ], reason: 'projected ls');
    });

    test('generated struct projection selects list and struct fields', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_struct_children.parquet'),
      );
      final table = reader.readTableSync(
        columns: ['s.tags.list.element', 's.inner.x'],
      );
      final s = table['s'] as StructColumn;
      expect(s.fields.keys.toSet(), <String>{'tags', 'inner'});
      final inner = s.fields['inner'] as StructColumn;
      expect(inner.fields.keys, <String>['x']);
      expectColumnValues(s, <Object?>[
        {
          'tags': ['a', 'b'],
          'inner': {'x': 1.5},
        },
        {'tags': <String>[], 'inner': null},
        {
          'tags': null,
          'inner': {'x': null},
        },
        null,
      ], reason: 'projected s');
    });

    test('generated map projection selects the list value leaf', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_map_children.parquet'),
      );
      final table = reader.readTableSync(
        columns: ['ml.key_value.value.list.element'],
      );
      final ml = table['ml'] as MapColumn;
      expect(ml.values, isA<ListColumn>());
      expectColumnValues(ml, <Object?>[
        {
          'a': [1, 2],
          'b': null,
        },
        <String, Object?>{},
        null,
        {'c': <int>[]},
      ], reason: 'projected ml');
    });

    test('official three-level list projection', () {
      final reader = ParquetReader.fromBytes(
        webFixture('nested_lists.snappy.parquet'),
      );
      final table = reader.readTableSync(columns: ['a']);
      expect(table.columns.map((column) => column.name).toList(), <String>[
        'a',
      ]);
      expect(table['a'], isA<ListColumn>());
      expectColumnValues(table['a'], <Object?>[
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
    });
  });

  group('writer round-trip of every deep fixture', () {
    for (final name in <String>[
      'deep_list_list.parquet',
      'deep_list_struct.parquet',
      'deep_struct_children.parquet',
      'deep_map_children.parquet',
      'nested_lists.snappy.parquet',
      'nested_maps.snappy.parquet',
      'old_list_structure.parquet',
      'map_no_value.parquet',
      'repeated_no_annotation.parquet',
      'repeated_primitive_no_list.parquet',
      'null_list.parquet',
    ]) {
      test(name, () {
        final reader = ParquetReader.fromBytes(webFixture(name));
        final original = reader.readTableSync();
        final expectedPaths = <String>[
          for (final column in reader.schema.columns) column.dotPath,
        ];
        for (final options in <ParquetWriterOptions>[
          const ParquetWriterOptions(),
          const ParquetWriterOptions(
            compression: CompressionType.snappy,
            dataPageVersion: 2,
            pageSize: 64,
          ),
        ]) {
          final back = roundTrip(original, options: options);
          expectTablesEquivalent(original, back, reason: name);
          expect(
            back.schema.columns.map((column) => column.dotPath).toList(),
            expectedPaths,
            reason: '$name leaf paths after write',
          );
        }
      });
    }

    test('multiple row groups keep deep columns in order', () {
      final reader = ParquetReader.fromBytes(
        webFixture('deep_struct_children.parquet'),
      );
      final group = reader.readTableSync();
      final writer = ParquetWriter(
        schema: reader.schema,
        options: const ParquetWriterOptions(compression: CompressionType.zstd),
      );
      writer.add(group);
      writer.add(group);
      final back = ParquetReader.fromBytes(writer.finish());
      expect(back.numRowGroups, 2);
      expect(back.numRows, group.numRows * 2);
      expectTablesEquivalent(group, back.readRowGroupSync(0));
      expectTablesEquivalent(group, back.readRowGroupSync(1));
    });
  });

  group('synthetic writer: arbitrary shapes', () {
    test('list<struct{a, tags: list}>', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
          numChildren: 2,
        ),
        SchemaElement(
          name: 'a',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
        SchemaElement(
          name: 'tags',
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
          type: PhysicalType.byteArray,
          repetitionType: FieldRepetitionType.optional,
          logicalType: StringLogicalType(),
        ),
      ]);
      final element = structColumn('element', <String, ParquetColumn>{
        'a': int32Column('a', [1, 2]),
        'tags': listColumn(
          'tags',
          <int>[0, 2, 2],
          stringColumn('element', ['x', 'y']),
          nulls: <bool>[false, true],
        ),
      });
      final column = listColumn('l', <int>[0, 2, 2], element);
      final table = tableOf(schema, <ParquetColumn>[column], 2);
      expectColumnValues(table['l'], <Object?>[
        [
          {
            'a': 1,
            'tags': ['x', 'y'],
          },
          {'a': 2, 'tags': null},
        ],
        <Object?>[],
      ], reason: 'input l');

      for (final options in <ParquetWriterOptions>[
        const ParquetWriterOptions(),
        const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
        ),
      ]) {
        final back = roundTrip(table, options: options);
        expectColumnValues(back['l'], <Object?>[
          [
            {
              'a': 1,
              'tags': ['x', 'y'],
            },
            {'a': 2, 'tags': null},
          ],
          <Object?>[],
        ], reason: 'round-trip l');
      }
    });

    test('struct{list, map, struct{list}}', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
        SchemaElement(
          name: 's',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 3,
        ),
        SchemaElement(
          name: 'tags',
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
          type: PhysicalType.byteArray,
          repetitionType: FieldRepetitionType.optional,
          logicalType: StringLogicalType(),
        ),
        SchemaElement(
          name: 'attrs',
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
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
        SchemaElement(
          name: 'inner',
          repetitionType: FieldRepetitionType.optional,
          numChildren: 2,
        ),
        SchemaElement(
          name: 'x',
          type: PhysicalType.double,
          repetitionType: FieldRepetitionType.optional,
        ),
        SchemaElement(
          name: 'ys',
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
      final tags = listColumn('tags', <int>[
        0,
        2,
        2,
      ], stringColumn('element', ['a', 'b']));
      final attrs = mapColumn(
        'attrs',
        <int>[0, 1, 1],
        stringColumn('key', ['k']),
        int32Column('value', [1]),
      );
      final inner = structColumn('inner', <String, ParquetColumn>{
        'x': float64Column('x', [1.5, null]),
        'ys': listColumn(
          'ys',
          <int>[0, 2, 2],
          int32Column('element', [1, 2]),
          nulls: <bool>[false, true],
        ),
      });
      final s = structColumn('s', <String, ParquetColumn>{
        'tags': tags,
        'attrs': attrs,
        'inner': inner,
      });
      final table = tableOf(schema, <ParquetColumn>[s], 2);
      final back = roundTrip(
        table,
        options: const ParquetWriterOptions(
          compression: CompressionType.zstd,
          dataPageVersion: 2,
        ),
      );
      expectTablesEquivalent(table, back, reason: 'struct children');
    });

    test('map<string, list<int32>> including a null list value', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
      final values = listColumn(
        'value',
        <int>[0, 2, 2],
        int32Column('element', [1, 2]),
        nulls: <bool>[false, true],
      );
      final column = mapColumn(
        'm',
        <int>[0, 2],
        stringColumn('key', ['k1', 'k2']),
        values,
      );
      final table = tableOf(schema, <ParquetColumn>[column], 1);
      final back = roundTrip(table);
      final map = back['m'] as MapColumn;
      expect(map.values, isA<ListColumn>());
      expect(map.getMap(0), <Object?, Object?>{
        'k1': [1, 2],
        'k2': null,
      });
    });

    test('MAP without a value keeps set semantics', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
      final column = mapColumn(
        'm',
        <int>[0, 3],
        int32Column('key', [1, 2, 3]),
        NullColumn(name: 'value', length: 3),
      );
      final back = roundTrip(tableOf(schema, <ParquetColumn>[column], 1));
      final map = back['m'] as MapColumn;
      expect(map.values, isA<NullColumn>());
      expect(map.getMap(0), <Object?, Object?>{1: null, 2: null, 3: null});
    });

    test('bare repeated leaf', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
        SchemaElement(
          name: 'r',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.repeated,
        ),
      ]);
      final column = listColumn('r', <int>[0, 3], int32Column('r', [1, 2, 3]));
      final back = roundTrip(tableOf(schema, <ParquetColumn>[column], 1));
      expectColumnValues(back['r'], <Object?>[
        [1, 2, 3],
      ], reason: 'r');
    });

    test('bare repeated group', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
      final element = structColumn('g', <String, ParquetColumn>{
        'a': int32Column('a', [10, 20]),
        'b': stringColumn('b', ['x', null]),
      });
      final column = listColumn('g', <int>[0, 2], element);
      final back = roundTrip(tableOf(schema, <ParquetColumn>[column], 1));
      expectColumnValues(back['g'], <Object?>[
        [
          {'a': 10, 'b': 'x'},
          {'a': 20, 'b': null},
        ],
      ], reason: 'g');
    });

    test('legacy LIST rule 1: repeated primitive is the element', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
      final column = listColumn('l', <int>[
        0,
        2,
        3,
      ], int32Column('vals', [10, 20, 30]));
      final back = roundTrip(tableOf(schema, <ParquetColumn>[column], 2));
      expectColumnValues(back['l'], <Object?>[
        [10, 20],
        [30],
      ], reason: 'l');
    });

    test('legacy LIST rule 2: repeated multi-field group is the element', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
      final element = structColumn('bag', <String, ParquetColumn>{
        'a': int32Column('a', [1, 2]),
        'b': stringColumn('b', ['x', null]),
      });
      final column = listColumn('l', <int>[0, 2], element);
      final back = roundTrip(tableOf(schema, <ParquetColumn>[column], 1));
      expectColumnValues(back['l'], <Object?>[
        [
          {'a': 1, 'b': 'x'},
          {'a': 2, 'b': null},
        ],
      ], reason: 'l');
    });

    test('legacy LIST rule 3: "_tuple" group is the element', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
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
      final element = structColumn('l_tuple', <String, ParquetColumn>{
        'x': int32Column('x', [7]),
      });
      final column = listColumn('l', <int>[0, 1], element);
      final back = roundTrip(tableOf(schema, <ParquetColumn>[column], 1));
      expectColumnValues(back['l'], <Object?>[
        [
          {'x': 7},
        ],
      ], reason: 'l');
    });

    test('required outer list, optional inner list, required element', () {
      final schema = schemaOf(const <SchemaElement>[
        schemaRoot,
        SchemaElement(
          name: 'll',
          repetitionType: FieldRepetitionType.required,
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
          repetitionType: FieldRepetitionType.required,
        ),
      ]);
      final inner = listColumn(
        'element',
        <int>[0, 2, 2, 2],
        int32Column('element', [1, 2]),
        nulls: <bool>[false, true, true],
      );
      final outer = listColumn('ll', <int>[0, 2, 3], inner);
      final table = tableOf(schema, <ParquetColumn>[outer], 2);
      final back = roundTrip(
        table,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
        ),
      );
      expectColumnValues(back['ll'], <Object?>[
        [
          [1, 2],
          null,
        ],
        [null],
      ], reason: 'll');
    });
  });
}
