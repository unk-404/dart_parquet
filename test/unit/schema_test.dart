@TestOn('vm')
library;

import 'dart:io';

import 'package:parquet/src/format/footer.dart';
import 'package:parquet/src/format/parquet_format.dart';
import 'package:parquet/src/schema/schema.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:parquet/src/util/errors.dart';
import 'package:test/test.dart';

Bytes fixtureBytes(String name) =>
    Bytes.fromUint8List(File('test/fixtures/$name').readAsBytesSync());

ParquetSchema schemaFor(String fixture) => ParquetSchema.fromFileMetaData(
  parseFooter(fixtureBytes(fixture)).fileMetadata,
);

void main() {
  group('flat schema', () {
    late ParquetSchema schema;

    setUpAll(() {
      schema = schemaFor('plain_lz4.parquet');
    });

    test('builds the root group', () {
      expect(schema.root.name, 'schema');
      expect(schema.root.parent, isNull);
      expect(schema.root.isGroup, isTrue);
      expect(schema.root.isLeaf, isFalse);
      expect(schema.root.repetition, FieldRepetitionType.required);
      expect(schema.root.defLevel, 0);
      expect(schema.root.repLevel, 0);
      expect(schema.root.children, hasLength(9));
      expect(schema.root.path, isEmpty);
    });

    test('enumerates leaves in depth-first order', () {
      expect(schema.numColumns, 9);
      expect(schema.numLeaves, 9);
      expect(schema.columns.map((column) => column.path).toList(), [
        ['i64'],
        ['str'],
        ['nullable_i32'],
        ['ts_us'],
        ['f64'],
        ['dec'],
        ['bool'],
        ['bin'],
        ['fixed16'],
      ]);
      for (var i = 0; i < schema.columns.length; i++) {
        expect(schema.columns[i].leafIndex, i);
        expect(schema.column(i), same(schema.columns[i]));
      }
    });

    test('computes physical types, lengths and levels', () {
      expect(schema.columns[0].physicalType, PhysicalType.int64);
      expect(schema.columns[1].physicalType, PhysicalType.byteArray);
      expect(schema.columns[2].physicalType, PhysicalType.int32);
      expect(schema.columns[5].physicalType, PhysicalType.fixedLenByteArray);
      expect(schema.columns[5].typeLength, 8);
      expect(schema.columns[8].typeLength, 16);
      expect(schema.columns[0].typeLength, isNull);
      for (final column in schema.columns) {
        expect(column.maxDefLevel, 1);
        expect(column.maxRepLevel, 0);
        expect(column.repetition, FieldRepetitionType.optional);
      }
    });

    test('resolves logical types and converted fallbacks', () {
      expect(schema.columns[0].logicalType, isNull);
      expect(schema.columns[1].logicalType, isA<StringLogicalType>());
      expect(
        schema.columns[3].logicalType,
        const TimestampLogicalType(TimeUnit.micros, false),
      );
      expect(schema.columns[5].logicalType, const DecimalLogicalType(2, 18));
      expect(schema.columns[5].convertedType, ConvertedType.decimal);
      expect(schema.columns[5].scale, 2);
      expect(schema.columns[5].precision, 18);
    });

    test('looks columns up by index, path and name', () {
      expect(schema.columnByPath(['i64']), same(schema.column(0)));
      expect(schema.columnByPath(['missing']), isNull);
      expect(schema.columnByPath([]), isNull);
      expect(schema.columnByPathString('ts_us'), same(schema.column(3)));
      expect(schema.columnByPathString(''), isNull);
      expect(schema.columnByName('str'), same(schema.column(1)));
      expect(schema.columnByName('nope'), isNull);
      expect(schema.column(0).dotPath, 'i64');
      expect(schema.column(0).name, 'i64');
    });

    test('rejects out-of-range column indexes', () {
      expect(() => schema.column(9), throwsA(isA<ParquetRangeException>()));
      expect(() => schema.column(-1), throwsA(isA<ParquetRangeException>()));
    });
  });

  group('nested schema', () {
    late ParquetSchema schema;

    setUpAll(() {
      schema = schemaFor('nested_snappy.parquet');
    });

    test('computes definition and repetition levels', () {
      expect(schema.numColumns, 6);
      final expected = <String, (int, int, PhysicalType)>{
        'id': (1, 0, PhysicalType.int32),
        'list_int.list.element': (3, 1, PhysicalType.int32),
        'struct.a': (2, 0, PhysicalType.int32),
        'struct.b': (2, 0, PhysicalType.byteArray),
        'map.key_value.key': (2, 1, PhysicalType.byteArray),
        'map.key_value.value': (3, 1, PhysicalType.int64),
      };
      for (final entry in expected.entries) {
        final column = schema.columnByPathString(entry.key);
        expect(column, isNotNull, reason: entry.key);
        expect(column!.maxDefLevel, entry.value.$1, reason: entry.key);
        expect(column.maxRepLevel, entry.value.$2, reason: entry.key);
        expect(column.physicalType, entry.value.$3, reason: entry.key);
      }
    });

    test('builds group nodes and logical annotations', () {
      final list = schema.root.children[1];
      expect(list.name, 'list_int');
      expect(list.isGroup, isTrue);
      expect(list.logicalType, isA<ListLogicalType>());
      expect(list.children, hasLength(1));
      final repeatedList = list.children.single;
      expect(repeatedList.name, 'list');
      expect(repeatedList.repetition, FieldRepetitionType.repeated);
      expect(repeatedList.repLevel, 1);
      expect(repeatedList.children.single.name, 'element');
      expect(repeatedList.children.single.maxDefLevel, 3);

      final struct = schema.root.children[2];
      expect(struct.name, 'struct');
      expect(struct.logicalType, isNull);
      expect(struct.children.map((child) => child.name), ['a', 'b']);

      final map = schema.root.children[3];
      expect(map.name, 'map');
      expect(map.logicalType, isA<MapLogicalType>());
      final keyValue = map.children.single;
      expect(keyValue.name, 'key_value');
      expect(keyValue.repetition, FieldRepetitionType.repeated);
      expect(keyValue.children.map((child) => child.name), ['key', 'value']);
      expect(keyValue.children[0].repetition, FieldRepetitionType.required);
      expect(keyValue.children[1].repetition, FieldRepetitionType.optional);
    });
  });

  group('logical type fallbacks', () {
    test('derives logical types from converted types', () {
      final schema = ParquetSchema.fromSchemaElements(const [
        SchemaElement(name: 'schema', numChildren: 3),
        SchemaElement(
          name: 'utf8_col',
          type: PhysicalType.byteArray,
          repetitionType: FieldRepetitionType.optional,
          convertedType: ConvertedType.utf8,
        ),
        SchemaElement(
          name: 'dec_col',
          type: PhysicalType.fixedLenByteArray,
          typeLength: 16,
          repetitionType: FieldRepetitionType.optional,
          convertedType: ConvertedType.decimal,
          scale: 2,
          precision: 10,
        ),
        SchemaElement(
          name: 'plain_col',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
      ]);

      final utf8 = schema.columnByPath(['utf8_col'])!;
      expect(utf8.logicalType, isA<StringLogicalType>());
      expect(utf8.node.declaredLogicalType, isNull);

      final decimal = schema.columnByPath(['dec_col'])!;
      expect(decimal.logicalType, const DecimalLogicalType(2, 10));

      final plain = schema.columnByPath(['plain_col'])!;
      expect(plain.logicalType, isNull);
      expect(plain.maxDefLevel, 0);
      expect(plain.maxRepLevel, 0);
      expect(plain.repetition, FieldRepetitionType.required);
    });

    test('parses every LogicalType union member', () {
      expect(
        LogicalType.fromThrift({1: <int, Object?>{}}),
        isA<StringLogicalType>(),
      );
      expect(
        LogicalType.fromThrift({
          5: {1: 3, 2: 12},
        }),
        const DecimalLogicalType(3, 12),
      );
      expect(
        LogicalType.fromThrift({
          7: {
            1: true,
            2: {3: <int, Object?>{}},
          },
        }),
        const TimeLogicalType(TimeUnit.nanos, true),
      );
      expect(
        LogicalType.fromThrift({
          8: {
            1: false,
            2: {1: <int, Object?>{}},
          },
        }),
        const TimestampLogicalType(TimeUnit.millis, false),
      );
      expect(
        LogicalType.fromThrift({
          10: {1: 16, 2: false},
        }),
        const IntegerLogicalType(16, false),
      );
      expect(LogicalType.fromThrift({11: {}}), isA<UnknownLogicalType>());
      expect(LogicalType.fromThrift({12: {}}), isA<JsonLogicalType>());
      expect(LogicalType.fromThrift({13: {}}), isA<BsonLogicalType>());
      expect(LogicalType.fromThrift({14: {}}), isA<UuidLogicalType>());
      expect(LogicalType.fromThrift({15: {}}), isA<Float16LogicalType>());
      expect(
        LogicalType.fromThrift({
          17: {1: 'OGC:CRS84'},
        }),
        const GeometryLogicalType('OGC:CRS84'),
      );
      expect(
        LogicalType.fromThrift({
          18: {1: 'X', 2: 3},
        }),
        const GeographyLogicalType('X', EdgeInterpolationAlgorithm.andoyer),
      );
      expect(LogicalType.fromThrift({19: {}}), isA<FileLogicalType>());
      expect(LogicalType.fromThrift(null), isNull);
      expect(LogicalType.fromThrift({}), isNull);
    });

    test('rejects malformed logical types', () {
      expect(
        () => LogicalType.fromThrift({
          5: {1: 2},
        }),
        throwsA(isA<ParquetFormatException>()),
      );
      expect(
        () => LogicalType.fromThrift({
          10: {1: 7, 2: true},
        }),
        throwsA(isA<ParquetFormatException>()),
      );
      // Unknown union members are annotations; keep reading the physical
      // column instead of failing the whole file.
      expect(LogicalType.fromThrift({99: {}}), isA<UnknownLogicalType>());
    });
  });

  group('malformed schemas', () {
    void expectFormatError(List<SchemaElement> elements) {
      expect(
        () => ParquetSchema.fromSchemaElements(elements),
        throwsA(isA<ParquetFormatException>()),
      );
    }

    test('empty schema', () {
      expectFormatError(const []);
    });

    test('root must be a group', () {
      expectFormatError(const [
        SchemaElement(
          name: 'schema',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.required,
        ),
      ]);
    });

    test('root must declare num_children', () {
      expectFormatError(const [SchemaElement(name: 'schema')]);
    });

    test('non-root nodes need a repetition', () {
      expectFormatError(const [
        SchemaElement(name: 'schema', numChildren: 1),
        SchemaElement(name: 'x', type: PhysicalType.int32),
      ]);
    });

    test('groups need num_children', () {
      expectFormatError(const [
        SchemaElement(name: 'schema', numChildren: 1),
        SchemaElement(
          name: 'group',
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
    });

    test('a node cannot be both leaf and group', () {
      expectFormatError(const [
        SchemaElement(name: 'schema', numChildren: 1),
        SchemaElement(
          name: 'x',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
          numChildren: 1,
        ),
      ]);
    });

    test('child counts must fit the flattened list', () {
      expectFormatError(const [
        SchemaElement(name: 'schema', numChildren: 2),
        SchemaElement(
          name: 'x',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
    });

    test('trailing elements are rejected', () {
      expectFormatError(const [
        SchemaElement(name: 'schema', numChildren: 1),
        SchemaElement(
          name: 'x',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
        SchemaElement(
          name: 'y',
          type: PhysicalType.int32,
          repetitionType: FieldRepetitionType.optional,
        ),
      ]);
    });

    test('negative child counts are rejected', () {
      expectFormatError(const [SchemaElement(name: 'schema', numChildren: -1)]);
    });
  });
}
