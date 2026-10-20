@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  group('schema metadata', () {
    test('every logical type roundtrips through the footer', () {
      final schema = _logicalTypeSchema();
      final bytes = ParquetWriter(schema: schema).finish();

      // Magic and footer length framing.
      expect(bytes.sublist(0, 4), [0x50, 0x41, 0x52, 0x31]);
      expect(bytes.sublist(bytes.length - 4), [0x50, 0x41, 0x52, 0x31]);
      final footerLength = ByteData.sublistView(bytes)
          .getUint32(bytes.length - 8, Endian.little);
      expect(footerLength, greaterThan(0));
      expect(footerLength + 8, lessThan(bytes.length));

      final reader = ParquetReader.fromBytes(bytes);
      expect(reader.numRowGroups, 0);
      expect(reader.numRows, 0);
      expect(reader.version, 1);
      expect(reader.createdBy, 'parquet-dart version 0.1.3');
      expect(reader.metadata.fileMetadata.rowGroups, isEmpty);

      final expected = <String, LogicalType>{
        'str': const StringLogicalType(),
        'enm': const EnumLogicalType(),
        'dec': const DecimalLogicalType(2, 9),
        'date': const DateLogicalType(),
        'time_millis': const TimeLogicalType(TimeUnit.millis, false),
        'time_micros': const TimeLogicalType(TimeUnit.micros, true),
        'ts_millis': const TimestampLogicalType(TimeUnit.millis, false),
        'ts_micros': const TimestampLogicalType(TimeUnit.micros, true),
        'ts_nanos': const TimestampLogicalType(TimeUnit.nanos, true),
        'i8': const IntegerLogicalType(8, true),
        'u64': const IntegerLogicalType(64, false),
        'unknown': const UnknownLogicalType(),
        'json': const JsonLogicalType(),
        'bson': const BsonLogicalType(),
        'uuid': const UuidLogicalType(),
        'float16': const Float16LogicalType(),
        'variant': const VariantLogicalType(1),
        'geometry': const GeometryLogicalType('OGC:CRS84'),
        'geography': const GeographyLogicalType(
          'OGC:CRS84',
          EdgeInterpolationAlgorithm.spherical,
        ),
      };
      for (final entry in expected.entries) {
        final descriptor = reader.schema.columnByPathString(entry.key);
        expect(descriptor, isNotNull, reason: entry.key);
        expect(descriptor!.logicalType, entry.value, reason: entry.key);
      }

      // Converted types are preserved alongside the modern annotation.
      expect(
        reader.schema.columnByPathString('str')!.convertedType,
        ConvertedType.utf8,
      );
      expect(
        reader.schema.columnByPathString('dec')!.convertedType,
        ConvertedType.decimal,
      );
      expect(reader.schema.columnByPathString('dec')!.scale, 2);
      expect(reader.schema.columnByPathString('dec')!.precision, 9);
      expect(
        reader.schema.columnByPathString('date')!.convertedType,
        ConvertedType.date,
      );
      expect(
        reader.schema.columnByPathString('u64')!.convertedType,
        ConvertedType.uint64,
      );

      // Fixed lengths and field ids.
      expect(reader.schema.columnByPathString('uuid')!.typeLength, 16);
      expect(reader.schema.columnByPathString('float16')!.typeLength, 2);
      expect(
        reader.schema.columnByPathString('with_field_id')!.node.fieldId,
        7,
      );

      // A FILE logical type annotates a group node.
      final fileNode = reader.schema.root.children.firstWhere(
        (node) => node.name == 'file',
      );
      expect(fileNode.logicalType, const FileLogicalType());
      expect(fileNode.children.length, 1);
      expect(fileNode.children.single.name, 'a');
    });

    test('footer records key/value metadata and createdBy', () {
      final bytes = ParquetWriter(
        schema: (ParquetSchemaBuilder()..addInt32('a')).build(),
        options: const ParquetWriterOptions(
          createdBy: 'unit-test version 0.0',
          keyValueMetadata: <String, String>{'k1': 'v1', 'k2': 'v2'},
        ),
      ).finish();
      final reader = ParquetReader.fromBytes(bytes);
      expect(reader.createdBy, 'unit-test version 0.0');
      expect(reader.metadata.fileMetadata.keyValueMetadata!.length, 2);
      expect(reader.metadata.fileMetadata.lookupMetadata('k1'), 'v1');
      expect(reader.metadata.fileMetadata.lookupMetadata('k2'), 'v2');
      expect(reader.metadata.fileMetadata.lookupMetadata('nope'), isNull);
    });
  });
}

ParquetSchema _logicalTypeSchema() {
  final elements = <SchemaElement>[
    const SchemaElement(name: 'schema', numChildren: 21),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'str',
      convertedType: ConvertedType.utf8,
      logicalType: StringLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'enm',
      convertedType: ConvertedType.enumType,
      logicalType: EnumLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.optional,
      name: 'dec',
      convertedType: ConvertedType.decimal,
      scale: 2,
      precision: 9,
      logicalType: DecimalLogicalType(2, 9),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.optional,
      name: 'date',
      convertedType: ConvertedType.date,
      logicalType: DateLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.optional,
      name: 'time_millis',
      convertedType: ConvertedType.timeMillis,
      logicalType: TimeLogicalType(TimeUnit.millis, false),
    ),
    const SchemaElement(
      type: PhysicalType.int64,
      repetitionType: FieldRepetitionType.optional,
      name: 'time_micros',
      convertedType: ConvertedType.timeMicros,
      logicalType: TimeLogicalType(TimeUnit.micros, true),
    ),
    const SchemaElement(
      type: PhysicalType.int64,
      repetitionType: FieldRepetitionType.optional,
      name: 'ts_millis',
      convertedType: ConvertedType.timestampMillis,
      logicalType: TimestampLogicalType(TimeUnit.millis, false),
    ),
    const SchemaElement(
      type: PhysicalType.int64,
      repetitionType: FieldRepetitionType.optional,
      name: 'ts_micros',
      convertedType: ConvertedType.timestampMicros,
      logicalType: TimestampLogicalType(TimeUnit.micros, true),
    ),
    const SchemaElement(
      type: PhysicalType.int64,
      repetitionType: FieldRepetitionType.optional,
      name: 'ts_nanos',
      logicalType: TimestampLogicalType(TimeUnit.nanos, true),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.required,
      name: 'i8',
      convertedType: ConvertedType.int8,
      logicalType: IntegerLogicalType(8, true),
    ),
    const SchemaElement(
      type: PhysicalType.int64,
      repetitionType: FieldRepetitionType.optional,
      name: 'u64',
      convertedType: ConvertedType.uint64,
      logicalType: IntegerLogicalType(64, false),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.optional,
      name: 'unknown',
      logicalType: UnknownLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'json',
      convertedType: ConvertedType.json,
      logicalType: JsonLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'bson',
      convertedType: ConvertedType.bson,
      logicalType: BsonLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.fixedLenByteArray,
      typeLength: 16,
      repetitionType: FieldRepetitionType.optional,
      name: 'uuid',
      logicalType: UuidLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.fixedLenByteArray,
      typeLength: 2,
      repetitionType: FieldRepetitionType.optional,
      name: 'float16',
      logicalType: Float16LogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'variant',
      logicalType: VariantLogicalType(1),
    ),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'geometry',
      logicalType: GeometryLogicalType('OGC:CRS84'),
    ),
    const SchemaElement(
      type: PhysicalType.byteArray,
      repetitionType: FieldRepetitionType.optional,
      name: 'geography',
      logicalType: GeographyLogicalType(
        'OGC:CRS84',
        EdgeInterpolationAlgorithm.spherical,
      ),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.optional,
      name: 'with_field_id',
      fieldId: 7,
    ),
    const SchemaElement(
      name: 'file',
      repetitionType: FieldRepetitionType.optional,
      numChildren: 1,
      logicalType: FileLogicalType(),
    ),
    const SchemaElement(
      type: PhysicalType.int32,
      repetitionType: FieldRepetitionType.required,
      name: 'a',
    ),
  ];
  return ParquetSchema.fromSchemaElements(elements);
}
