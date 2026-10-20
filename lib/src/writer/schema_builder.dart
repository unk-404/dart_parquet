/// Small helper for building flat Parquet schemas in code.
///
/// The writer accepts any [ParquetSchema] (for example one parsed from an
/// existing file), but tests and applications that create data from scratch
/// need a way to describe columns. All columns added here are top-level leaves;
/// nested schemas still have to be assembled from `SchemaElement`s.
library;

import '../format/parquet_format.dart';
import '../schema/schema.dart';

/// Builds a flat [ParquetSchema] one column at a time.
///
/// ```dart
/// final schema = (ParquetSchemaBuilder()
///       ..addInt64('id')
///       ..addBinary('name', utf8: true))
///     .build();
/// ```
final class ParquetSchemaBuilder {
  /// Creates an empty builder with the given root group name.
  ParquetSchemaBuilder({this.rootName = 'schema'});

  /// Name of the root schema group.
  final String rootName;

  final List<SchemaElement> _fields = <SchemaElement>[];

  /// Number of columns added so far.
  int get numColumns => _fields.length;

  /// Adds a pre-built leaf [element].
  void add(SchemaElement element) {
    if (!element.isLeaf) {
      throw ArgumentError.value(
        element.name,
        'element',
        'ParquetSchemaBuilder only supports leaf columns',
      );
    }
    _fields.add(element);
  }

  /// Adds an INT32 column.
  void addInt32(String name, {bool nullable = true, bool unsigned = false}) {
    add(
      _leaf(
        name,
        PhysicalType.int32,
        repetition: nullable ? FieldRepetitionType.optional : null,
        convertedType: unsigned ? ConvertedType.uint32 : null,
        logicalType: unsigned ? const IntegerLogicalType(32, false) : null,
      ),
    );
  }

  /// Adds an INT64 column.
  void addInt64(String name, {bool nullable = true, bool unsigned = false}) {
    add(
      _leaf(
        name,
        PhysicalType.int64,
        repetition: nullable ? FieldRepetitionType.optional : null,
        convertedType: unsigned ? ConvertedType.uint64 : null,
        logicalType: unsigned ? const IntegerLogicalType(64, false) : null,
      ),
    );
  }

  /// Adds a FLOAT column.
  void addFloat32(String name, {bool nullable = true}) {
    add(_leaf(name, PhysicalType.float, repetition: _repetition(nullable)));
  }

  /// Adds a DOUBLE column.
  void addFloat64(String name, {bool nullable = true}) {
    add(_leaf(name, PhysicalType.double, repetition: _repetition(nullable)));
  }

  /// Adds a BOOLEAN column.
  void addBoolean(String name, {bool nullable = true}) {
    add(_leaf(name, PhysicalType.boolean, repetition: _repetition(nullable)));
  }

  /// Adds a BYTE_ARRAY column. When [utf8] is true it is annotated as STRING.
  void addBinary(String name, {bool nullable = true, bool utf8 = false}) {
    add(
      _leaf(
        name,
        PhysicalType.byteArray,
        repetition: _repetition(nullable),
        convertedType: utf8 ? ConvertedType.utf8 : null,
        logicalType: utf8 ? const StringLogicalType() : null,
      ),
    );
  }

  /// Adds a FIXED_LEN_BYTE_ARRAY column of [length] bytes.
  void addFixedLenByteArray(String name, int length, {bool nullable = true}) {
    if (length <= 0) {
      throw ArgumentError.value(length, 'length', 'must be positive');
    }
    add(
      _leaf(
        name,
        PhysicalType.fixedLenByteArray,
        repetition: _repetition(nullable),
        typeLength: length,
      ),
    );
  }

  /// Adds a legacy INT96 column.
  void addInt96(String name, {bool nullable = true}) {
    add(_leaf(name, PhysicalType.int96, repetition: _repetition(nullable)));
  }

  /// Adds a DECIMAL column.
  ///
  /// The physical type follows the usual precision rules: INT32 up to 9
  /// digits, INT64 up to 18 digits and a fixed-length byte array otherwise
  /// (or when [fixedLength] is given explicitly).
  void addDecimal(
    String name, {
    required int precision,
    required int scale,
    int? fixedLength,
    bool nullable = true,
  }) {
    if (precision <= 0 || precision > 38) {
      throw ArgumentError.value(
        precision,
        'precision',
        'must be between 1 and 38',
      );
    }
    if (scale < 0 || scale > precision) {
      throw ArgumentError.value(scale, 'scale', 'must be 0..precision');
    }
    final PhysicalType physicalType;
    int? typeLength;
    if (fixedLength != null) {
      if (fixedLength <= 0) {
        throw ArgumentError.value(fixedLength, 'fixedLength', 'must be > 0');
      }
      physicalType = PhysicalType.fixedLenByteArray;
      typeLength = fixedLength;
    } else if (precision <= 9) {
      physicalType = PhysicalType.int32;
    } else if (precision <= 18) {
      physicalType = PhysicalType.int64;
    } else {
      physicalType = PhysicalType.fixedLenByteArray;
      typeLength = (precision * 3.3219280948873626 / 8).ceil();
    }
    add(
      _leaf(
        name,
        physicalType,
        repetition: _repetition(nullable),
        typeLength: typeLength,
        convertedType: ConvertedType.decimal,
        scale: scale,
        precision: precision,
        logicalType: DecimalLogicalType(scale, precision),
      ),
    );
  }

  /// Builds the schema.
  ParquetSchema build() {
    return ParquetSchema.fromSchemaElements(<SchemaElement>[
      SchemaElement(name: rootName, numChildren: _fields.length),
      ..._fields,
    ]);
  }
}

FieldRepetitionType _repetition(bool nullable) =>
    nullable ? FieldRepetitionType.optional : FieldRepetitionType.required;

SchemaElement _leaf(
  String name,
  PhysicalType type, {
  FieldRepetitionType? repetition,
  int? typeLength,
  ConvertedType? convertedType,
  LogicalType? logicalType,
  int? scale,
  int? precision,
}) {
  return SchemaElement(
    type: type,
    typeLength: typeLength,
    repetitionType: repetition ?? FieldRepetitionType.required,
    name: name,
    convertedType: convertedType,
    scale: scale,
    precision: precision,
    logicalType: logicalType,
  );
}
