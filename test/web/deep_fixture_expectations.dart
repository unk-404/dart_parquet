/// Shared expectations/builders for the v0.2 deep-nesting Web/Wasm suites.
///
/// No `dart:io` and no JS interop: the same helpers run under dart2js and
/// dart2wasm.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Canonical comparison
// ---------------------------------------------------------------------------

/// Canonicalises nested values so map iteration order is irrelevant.
///
/// Structs/rows are `Map<String, Object?>` from [StructColumn], maps are
/// `Map<Object?, Object?>` from [MapColumn]; both become sorted pair lists.
Object? canonicalize(Object? value) {
  if (value is Map) {
    final pairs = <List<Object?>>[
      for (final entry in value.entries)
        <Object?>[canonicalize(entry.key), canonicalize(entry.value)],
    ];
    pairs.sort((a, b) => a[0].toString().compareTo(b[0].toString()));
    return pairs;
  }
  if (value is List) {
    return <Object?>[for (final item in value) canonicalize(item)];
  }
  if (value is int) return BigInt.from(value);
  return value;
}

/// Asserts every row of [column] against [expected] using canonical equality.
void expectColumnValues(
  ParquetColumn column,
  List<Object?> expected, {
  String? reason,
}) {
  final label = reason ?? column.name;
  expect(column.length, expected.length, reason: '$label length');
  for (var row = 0; row < expected.length; row++) {
    expect(
      canonicalize(column.value(row)),
      canonicalize(expected[row]),
      reason: '$label row $row',
    );
  }
}

/// Asserts the reader's leaf paths, in schema order.
void expectLeafPaths(ParquetReader reader, List<String> expected) {
  expect(
    reader.schema.columns.map((column) => column.dotPath).toList(),
    expected,
  );
}

// ---------------------------------------------------------------------------
// Generated v0.2 deep-nesting fixtures
// ---------------------------------------------------------------------------

/// `deep_list_list.parquet`: `list<list<int32>>` with null/empty levels.
void expectDeepListList(ParquetTable table) {
  expect(table.numRows, 4);
  final ll = table['ll'] as ListColumn;
  expect(ll.child, isA<ListColumn>());
  expect((ll.child as ListColumn).child, isA<Int32Column>());
  expectColumnValues(ll, <Object?>[
    [
      [1, 2],
      [3],
    ],
    [<int>[]],
    null,
    [
      [4],
      null,
    ],
  ], reason: 'll');
  expectColumnValues(table['id'], <Object?>[0, 1, 2, 3], reason: 'id');
}

/// `deep_list_struct.parquet`: `list<struct>` and `list<struct{list}>`.
void expectDeepListStruct(ParquetTable table) {
  expect(table.numRows, 4);
  final ls = table['ls'] as ListColumn;
  expect(ls.child, isA<StructColumn>());
  expectColumnValues(ls, <Object?>[
    [
      {'a': 1, 'b': 'x'},
      null,
    ],
    <Object?>[],
    null,
    [
      {'a': 2, 'b': null},
    ],
  ], reason: 'ls');

  final lsn = table['lsn'] as ListColumn;
  final lsnElement = lsn.child as StructColumn;
  expect(lsnElement.fields['tags'], isA<ListColumn>());
  expectColumnValues(lsn, <Object?>[
    [
      {
        'a': 1,
        'tags': ['t1', 't2'],
      },
    ],
    <Object?>[],
    null,
    [
      {'a': 2, 'tags': null},
      {'a': 3, 'tags': <String>[]},
    ],
  ], reason: 'lsn');
}

/// `deep_struct_children.parquet`: struct containing list, map and struct/list.
void expectDeepStructChildren(ParquetTable table) {
  expect(table.numRows, 4);
  final s = table['s'] as StructColumn;
  expect(s.fields['tags'], isA<ListColumn>());
  expect(s.fields['attrs'], isA<MapColumn>());
  expect(s.fields['inner'], isA<StructColumn>());
  expectColumnValues(s, <Object?>[
    {
      'id': 1,
      'tags': ['a', 'b'],
      'attrs': {'k': 1},
      'inner': {
        'x': 1.5,
        'ys': [1, 2],
      },
    },
    {'id': 2, 'tags': <String>[], 'attrs': <String, int>{}, 'inner': null},
    {
      'id': 3,
      'tags': null,
      'attrs': null,
      'inner': {'x': null, 'ys': null},
    },
    null,
  ], reason: 's');
}

/// `deep_map_children.parquet`: map values that are lists or structs.
void expectDeepMapChildren(ParquetTable table) {
  expect(table.numRows, 4);
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
  ], reason: 'ml');

  final ms = table['ms'] as MapColumn;
  expect(ms.values, isA<StructColumn>());
  expectColumnValues(ms, <Object?>[
    {
      'k': {
        'a': 1,
        'b': ['x'],
      },
    },
    {'k2': null},
    <String, Object?>{},
    null,
  ], reason: 'ms');
}

// ---------------------------------------------------------------------------
// Vendored apache/parquet-testing deep fixtures
// ---------------------------------------------------------------------------

void expectOfficialNestedLists(ParquetTable table) {
  expect(table.numRows, 3);
  final a = table['a'] as ListColumn;
  expect(a.child, isA<ListColumn>());
  expect((a.child as ListColumn).child, isA<ListColumn>());
  expect(
    ((a.child as ListColumn).child as ListColumn).child,
    isA<BinaryColumn>(),
    reason: 'no silent flattening of the three list levels',
  );
  expectColumnValues(a, <Object?>[
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
  expectColumnValues(table['b'], <Object?>[1, 1, 1], reason: 'b');
}

void expectOfficialNestedMaps(ParquetTable table) {
  expect(table.numRows, 6);
  final a = table['a'] as MapColumn;
  expect(a.values, isA<MapColumn>());
  expectColumnValues(a, <Object?>[
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
  expectColumnValues(table['b'], <Object?>[1, 1, 1, 1, 1, 1], reason: 'b');
  expectColumnValues(table['c'], <Object?>[
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
  ], reason: 'c');
}

void expectOfficialOldListStructure(ParquetTable table) {
  expect(table.numRows, 1);
  final a = table['a'] as ListColumn;
  expect(a.child, isA<ListColumn>(), reason: 'legacy outer element is a list');
  expectColumnValues(a, <Object?>[
    [
      [1, 2],
      [3, 4],
    ],
  ], reason: 'a');
}

void expectOfficialMapNoValue(ParquetTable table) {
  expect(table.numRows, 3);
  final noValue = table['my_map_no_v'] as MapColumn;
  expect(noValue.values, isA<NullColumn>());
  expectColumnValues(table['my_map'] as MapColumn, <Object?>[
    {1: null, 2: null, 3: null},
    {4: null, 5: null, 6: null},
    {7: null, 8: null, 9: null},
  ], reason: 'my_map');
  expectColumnValues(noValue, <Object?>[
    {1: null, 2: null, 3: null},
    {4: null, 5: null, 6: null},
    {7: null, 8: null, 9: null},
  ], reason: 'my_map_no_v');
  expectColumnValues(table['my_list'] as ListColumn, <Object?>[
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9],
  ], reason: 'my_list');
}

void expectOfficialBareRepeatedGroup(ParquetTable table) {
  expect(table.numRows, 6);
  expectColumnValues(table['id'], <Object?>[1, 2, 3, 4, 5, 6], reason: 'id');
  final phones = table['phoneNumbers'] as StructColumn;
  expect(phones.fields['phone'], isA<ListColumn>());
  expect((phones.fields['phone'] as ListColumn).child, isA<StructColumn>());
  expectColumnValues(phones, <Object?>[
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
}

void expectOfficialBareRepeatedLeaf(ParquetTable table) {
  expect(table.numRows, 4);
  expectColumnValues(table['Int32_list'], <Object?>[
    [0, 1, 2, 3],
    <int>[],
    [4],
    [5, 6, 7, 8],
  ], reason: 'Int32_list');
  expectColumnValues(table['String_list'], <Object?>[
    ['foo', 'zero', 'one', 'two'],
    ['three'],
    ['four'],
    ['five', 'six', 'seven', 'eight'],
  ], reason: 'String_list');
  final group = table['group_of_lists'] as StructColumn;
  expect(group.fields['Int32_list_in_group'], isA<ListColumn>());
  expectColumnValues(group, <Object?>[
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
}

void expectOfficialNullList(ParquetTable table) {
  expect(table.numRows, 1);
  expectColumnValues(table['emptylist'], <Object?>[
    <int>[],
  ], reason: 'emptylist');
}

// ---------------------------------------------------------------------------
// Builders for synthetic writer round-trips
// ---------------------------------------------------------------------------

Validity validityOf(List<bool> nulls) {
  final validity = Validity(nulls.length);
  for (var i = 0; i < nulls.length; i++) {
    if (nulls[i]) validity.setNull(i);
  }
  return validity;
}

Int32Column int32Column(String name, List<int?> values) {
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

Float64Column float64Column(String name, List<double?> values) {
  final validity = Validity(values.length);
  final data = Float64List(values.length);
  for (var i = 0; i < values.length; i++) {
    final value = values[i];
    if (value == null) {
      validity.setNull(i);
    } else {
      data[i] = value;
    }
  }
  return Float64Column(name: name, values: data, validity: validity);
}

BinaryColumn stringColumn(String name, List<String?> values) {
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

StructColumn structColumn(
  String name,
  Map<String, ParquetColumn> fields, {
  List<bool>? nulls,
}) {
  final length = fields.values.first.length;
  for (final field in fields.values) {
    if (field.length != length) {
      throw ArgumentError('struct fields must have equal lengths');
    }
  }
  return StructColumn(
    name: name,
    length: length,
    fields: fields,
    validity: validityOf(nulls ?? List<bool>.filled(length, false)),
  );
}

ListColumn listColumn(
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
    validity: validityOf(nulls ?? List<bool>.filled(length, false)),
  );
}

MapColumn mapColumn(
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
    validity: validityOf(nulls ?? List<bool>.filled(length, false)),
  );
}

ParquetSchema schemaOf(List<SchemaElement> elements) =>
    ParquetSchema.fromSchemaElements(elements);

ParquetTable tableOf(
  ParquetSchema schema,
  List<ParquetColumn> columns,
  int numRows,
) => ParquetTable(schema: schema, columns: columns, numRows: numRows);

const SchemaElement schemaRoot = SchemaElement(name: 'schema', numChildren: 1);

/// Writes [table] in memory and reads it back (single row group).
ParquetTable roundTrip(
  ParquetTable table, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
}) {
  final writer = ParquetWriter(schema: table.schema, options: options);
  writer.add(table);
  return ParquetReader.fromBytes(writer.finish()).readTableSync();
}

/// Asserts that [actual] has the same columns/classes/values as [expected].
void expectTablesEquivalent(
  ParquetTable expected,
  ParquetTable actual, {
  String? reason,
}) {
  final label = reason ?? '';
  expect(actual.numRows, expected.numRows, reason: '$label row count');
  expect(
    actual.columns.map((column) => column.name).toList(),
    expected.columns.map((column) => column.name).toList(),
    reason: '$label column names',
  );
  expect(
    actual.columns.map((column) => column.runtimeType).toList(),
    expected.columns.map((column) => column.runtimeType).toList(),
    reason: '$label column classes',
  );
  for (var i = 0; i < expected.columns.length; i++) {
    final a = expected.columns[i];
    final b = actual.columns[i];
    expect(b.length, a.length, reason: '$label ${a.name} length');
    for (var row = 0; row < a.length; row++) {
      expect(
        canonicalize(b.value(row)),
        canonicalize(a.value(row)),
        reason: '$label ${a.name} row $row',
      );
    }
  }
}
