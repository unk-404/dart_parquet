@TestOn('vm')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  group('ParquetWriterOptions', () {
    test('defaults match the v0.1.2 contract', () {
      const options = ParquetWriterOptions();
      expect(options.compression, CompressionType.uncompressed);
      expect(options.useDictionary, isTrue);
      expect(options.dictionaryPageSizeLimit, 1024 * 1024);
      expect(options.dataPageVersion, 1);
      expect(options.writeStatistics, isTrue);
      expect(options.pageSize, 1024 * 1024);
      expect(options.createdBy, 'parquet-dart version 0.1.2');
      expect(options.keyValueMetadata, isNull);
      expect(options.codec, isNotNull);
      expect(options.writeCrc, isFalse);
      expect(options.writePageIndex, isFalse);
      expect(options.writeBloomFilter, isFalse);
      expect(options.bloomFilterFpp, 0.01);
      expect(options.bloomFilterMaxBytes, 1024 * 1024);
    });

    test('validate rejects invalid page versions and sizes', () {
      expect(
        () => const ParquetWriterOptions(dataPageVersion: 3).validate(),
        throwsArgumentError,
      );
      expect(
        () => const ParquetWriterOptions(dataPageVersion: 0).validate(),
        throwsArgumentError,
      );
      expect(
        () => const ParquetWriterOptions(pageSize: 0).validate(),
        throwsArgumentError,
      );
      expect(
        () =>
            const ParquetWriterOptions(dictionaryPageSizeLimit: -1).validate(),
        throwsArgumentError,
      );
      expect(
        () => const ParquetWriterOptions(bloomFilterFpp: 0).validate(),
        throwsArgumentError,
      );
      expect(
        () => const ParquetWriterOptions(bloomFilterFpp: 1).validate(),
        throwsArgumentError,
      );
      expect(
        () => const ParquetWriterOptions(bloomFilterMaxBytes: 31).validate(),
        throwsArgumentError,
      );
    });

    test('v0.1.2 features default to off', () {
      const options = ParquetWriterOptions();
      expect(options.writeCrc, isFalse);
      expect(options.writePageIndex, isFalse);
      expect(options.writeBloomFilter, isFalse);
    });

    test('writer constructor validates options', () {
      final schema = (ParquetSchemaBuilder()..addInt32('a')).build();
      expect(
        () => ParquetWriter(
          schema: schema,
          options: const ParquetWriterOptions(dataPageVersion: 5),
        ),
        throwsArgumentError,
      );
      expect(
        () => ParquetWriter(
          schema: schema,
          options: const ParquetWriterOptions(pageSize: -1),
        ),
        throwsArgumentError,
      );
    });
  });

  group('ParquetSchemaBuilder', () {
    test('builds flat leaves with logical annotations', () {
      final schema =
          (ParquetSchemaBuilder(rootName: 'root')
                ..addInt64('id', nullable: false)
                ..addInt32('u32', unsigned: true)
                ..addFloat32('f32')
                ..addFloat64('f64')
                ..addBoolean('flag')
                ..addBinary('name', utf8: true)
                ..addBinary('blob')
                ..addFixedLenByteArray('fx', 16)
                ..addInt96('legacy')
                ..addDecimal('amount', precision: 18, scale: 2))
              .build();

      expect(schema.root.name, 'root');
      expect(schema.root.children.length, 10);
      expect(schema.numColumns, 10);

      final id = schema.columnByPathString('id')!;
      expect(id.physicalType, PhysicalType.int64);
      expect(id.repetition, FieldRepetitionType.required);
      expect(id.maxDefLevel, 0);

      final u32 = schema.columnByPathString('u32')!;
      expect(u32.logicalType, const IntegerLogicalType(32, false));
      expect(u32.convertedType, ConvertedType.uint32);

      final name = schema.columnByPathString('name')!;
      expect(name.logicalType, const StringLogicalType());
      expect(name.convertedType, ConvertedType.utf8);
      expect(name.maxDefLevel, 1);

      final fx = schema.columnByPathString('fx')!;
      expect(fx.physicalType, PhysicalType.fixedLenByteArray);
      expect(fx.typeLength, 16);

      final legacy = schema.columnByPathString('legacy')!;
      expect(legacy.physicalType, PhysicalType.int96);

      final amount = schema.columnByPathString('amount')!;
      expect(amount.physicalType, PhysicalType.int64);
      expect(amount.logicalType, const DecimalLogicalType(2, 18));
      expect(amount.convertedType, ConvertedType.decimal);
      expect(amount.scale, 2);
      expect(amount.precision, 18);
    });

    test('selects decimal physical types by precision', () {
      final schema =
          (ParquetSchemaBuilder()
                ..addDecimal('small', precision: 9, scale: 2)
                ..addDecimal('medium', precision: 18, scale: 2)
                ..addDecimal('big', precision: 30, scale: 2)
                ..addDecimal('fixed', precision: 20, scale: 2, fixedLength: 9))
              .build();
      expect(
        schema.columnByPathString('small')!.physicalType,
        PhysicalType.int32,
      );
      expect(
        schema.columnByPathString('medium')!.physicalType,
        PhysicalType.int64,
      );
      expect(
        schema.columnByPathString('big')!.physicalType,
        PhysicalType.fixedLenByteArray,
      );
      expect(schema.columnByPathString('big')!.typeLength, 13);
      expect(
        schema.columnByPathString('fixed')!.physicalType,
        PhysicalType.fixedLenByteArray,
      );
      expect(schema.columnByPathString('fixed')!.typeLength, 9);
    });

    test('rejects invalid decimal parameters and non-leaf elements', () {
      final builder = ParquetSchemaBuilder();
      expect(
        () => builder.addDecimal('a', precision: 0, scale: 0),
        throwsArgumentError,
      );
      expect(
        () => builder.addDecimal('a', precision: 10, scale: 11),
        throwsArgumentError,
      );
      expect(() => builder.addFixedLenByteArray('a', 0), throwsArgumentError);
      expect(
        () => builder.add(const SchemaElement(name: 'group', numChildren: 1)),
        throwsArgumentError,
      );
    });
  });

  group('unsupported schema shapes', () {
    test('list of list is rejected', () {
      expect(
        () => ParquetWriter(
          schema: _schemaOf([
            const SchemaElement(name: 'schema', numChildren: 1),
            const SchemaElement(
              name: 'outer',
              repetitionType: FieldRepetitionType.optional,
              numChildren: 1,
              logicalType: ListLogicalType(),
            ),
            const SchemaElement(
              name: 'list',
              repetitionType: FieldRepetitionType.repeated,
              numChildren: 1,
            ),
            const SchemaElement(
              name: 'element',
              repetitionType: FieldRepetitionType.optional,
              numChildren: 1,
              logicalType: ListLogicalType(),
            ),
            const SchemaElement(
              name: 'list',
              repetitionType: FieldRepetitionType.repeated,
              numChildren: 1,
            ),
            const SchemaElement(
              type: PhysicalType.int32,
              name: 'element',
              repetitionType: FieldRepetitionType.optional,
            ),
          ]),
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('map with a struct value is rejected', () {
      expect(
        () => ParquetWriter(
          schema: _schemaOf([
            const SchemaElement(name: 'schema', numChildren: 1),
            const SchemaElement(
              name: 'm',
              repetitionType: FieldRepetitionType.optional,
              numChildren: 1,
              logicalType: MapLogicalType(),
            ),
            const SchemaElement(
              name: 'key_value',
              repetitionType: FieldRepetitionType.repeated,
              numChildren: 2,
            ),
            const SchemaElement(
              type: PhysicalType.byteArray,
              name: 'key',
              repetitionType: FieldRepetitionType.required,
            ),
            const SchemaElement(
              name: 'value',
              repetitionType: FieldRepetitionType.optional,
              numChildren: 1,
            ),
            const SchemaElement(
              type: PhysicalType.int32,
              name: 'a',
              repetitionType: FieldRepetitionType.optional,
            ),
          ]),
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('list inside a struct is rejected', () {
      expect(
        () => ParquetWriter(
          schema: _schemaOf([
            const SchemaElement(name: 'schema', numChildren: 1),
            const SchemaElement(
              name: 's',
              repetitionType: FieldRepetitionType.optional,
              numChildren: 1,
            ),
            const SchemaElement(
              name: 'l',
              repetitionType: FieldRepetitionType.optional,
              numChildren: 1,
              logicalType: ListLogicalType(),
            ),
            const SchemaElement(
              name: 'list',
              repetitionType: FieldRepetitionType.repeated,
              numChildren: 1,
            ),
            const SchemaElement(
              type: PhysicalType.int32,
              name: 'element',
              repetitionType: FieldRepetitionType.optional,
            ),
          ]),
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('bare repeated group is rejected', () {
      expect(
        () => ParquetWriter(
          schema: _schemaOf([
            const SchemaElement(name: 'schema', numChildren: 1),
            const SchemaElement(
              name: 'g',
              repetitionType: FieldRepetitionType.repeated,
              numChildren: 1,
            ),
            const SchemaElement(
              type: PhysicalType.int32,
              name: 'a',
              repetitionType: FieldRepetitionType.required,
            ),
          ]),
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('bare repeated leaf is rejected', () {
      expect(
        () => ParquetWriter(
          schema: _schemaOf([
            const SchemaElement(name: 'schema', numChildren: 1),
            const SchemaElement(
              type: PhysicalType.int32,
              repetitionType: FieldRepetitionType.repeated,
              name: 'vals',
            ),
          ]),
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('empty schema is rejected', () {
      expect(
        () => ParquetWriter(
          schema: _schemaOf([
            const SchemaElement(name: 'schema', numChildren: 0),
          ]),
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });
  });
}

ParquetSchema _schemaOf(List<SchemaElement> elements) =>
    ParquetSchema.fromSchemaElements(elements);
