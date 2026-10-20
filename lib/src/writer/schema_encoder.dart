/// Converts the reader's schema model back into Thrift `SchemaElement` maps.
library;

import '../format/thrift.dart';
import '../schema/schema.dart';

/// Flattens [schema] into the depth-first `SchemaElement` list stored in
/// `FileMetaData.schema`.
List<Map<int, Object?>> encodeSchemaElements(ParquetSchema schema) {
  final out = <Map<int, Object?>>[];
  _encodeNode(schema.root, out, isRoot: true);
  return out;
}

void _encodeNode(
  SchemaNode node,
  List<Map<int, Object?>> out, {
  required bool isRoot,
}) {
  final fields = <int, Object?>{};

  // Field ids follow parquet.thrift SchemaElement.
  final physicalType = node.physicalType;
  if (physicalType != null) {
    fields[1] = ThriftI32(physicalType.value);
  }
  if (node.typeLength != null) {
    fields[2] = ThriftI32(node.typeLength!);
  }
  if (!isRoot) {
    fields[3] = ThriftI32(node.repetition.value);
  }
  fields[4] = node.name;
  if (node.isGroup) {
    fields[5] = ThriftI32(node.children.length);
  }
  if (node.convertedType != null) {
    fields[6] = ThriftI32(node.convertedType!.value);
  }
  if (node.scale != null) {
    fields[7] = ThriftI32(node.scale!);
  }
  if (node.precision != null) {
    fields[8] = ThriftI32(node.precision!);
  }
  if (node.fieldId != null) {
    fields[9] = ThriftI32(node.fieldId!);
  }
  final declared = node.declaredLogicalType;
  if (declared != null) {
    fields[10] = _encodeLogicalType(declared);
  }

  out.add(fields);
  for (final child in node.children) {
    _encodeNode(child, out, isRoot: false);
  }
}

Map<int, Object?> _encodeLogicalType(LogicalType logicalType) {
  switch (logicalType) {
    case StringLogicalType():
      return {1: <int, Object?>{}};
    case MapLogicalType():
      return {2: <int, Object?>{}};
    case ListLogicalType():
      return {3: <int, Object?>{}};
    case EnumLogicalType():
      return {4: <int, Object?>{}};
    case DecimalLogicalType(:final scale, :final precision):
      return {
        5: <int, Object?>{1: ThriftI32(scale), 2: ThriftI32(precision)},
      };
    case DateLogicalType():
      return {6: <int, Object?>{}};
    case TimeLogicalType(:final unit, :final isAdjustedToUtc):
      return {
        7: <int, Object?>{1: isAdjustedToUtc, 2: _encodeTimeUnit(unit)},
      };
    case TimestampLogicalType(:final unit, :final isAdjustedToUtc):
      return {
        8: <int, Object?>{1: isAdjustedToUtc, 2: _encodeTimeUnit(unit)},
      };
    case IntegerLogicalType(:final bitWidth, :final isSigned):
      return {
        10: <int, Object?>{1: ThriftByte(bitWidth), 2: isSigned},
      };
    case UnknownLogicalType():
      return {11: <int, Object?>{}};
    case JsonLogicalType():
      return {12: <int, Object?>{}};
    case BsonLogicalType():
      return {13: <int, Object?>{}};
    case UuidLogicalType():
      return {14: <int, Object?>{}};
    case Float16LogicalType():
      return {15: <int, Object?>{}};
    case VariantLogicalType(:final specificationVersion):
      final payload = <int, Object?>{};
      if (specificationVersion != null) {
        payload[1] = ThriftByte(specificationVersion);
      }
      return {16: payload};
    case GeometryLogicalType(:final crs):
      final payload = <int, Object?>{};
      if (crs != null) payload[1] = crs;
      return {17: payload};
    case GeographyLogicalType(:final crs, :final algorithm):
      final payload = <int, Object?>{};
      if (crs != null) payload[1] = crs;
      if (algorithm != null) payload[2] = ThriftI32(algorithm.value);
      return {18: payload};
    case FileLogicalType():
      return {19: <int, Object?>{}};
  }
}

Map<int, Object?> _encodeTimeUnit(TimeUnit unit) {
  final fieldId = switch (unit) {
    TimeUnit.millis => 1,
    TimeUnit.micros => 2,
    TimeUnit.nanos => 3,
  };
  return {fieldId: <int, Object?>{}};
}
