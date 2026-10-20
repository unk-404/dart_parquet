/// Parquet logical and converted type annotations.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'enums.dart';
import '../util/errors.dart';

/// Unit of a `TIME` or `TIMESTAMP` logical type.
enum TimeUnit {
  /// Milliseconds.
  millis,

  /// Microseconds.
  micros,

  /// Nanoseconds.
  nanos,
}

/// Discriminator for [LogicalType] subclasses.
enum LogicalTypeKind {
  /// UTF-8 string.
  string,

  /// Map.
  map,

  /// List.
  list,

  /// Enum encoded as a string.
  enumType,

  /// Fixed-point decimal.
  decimal,

  /// Calendar date.
  date,

  /// Time of day.
  time,

  /// Date and time.
  timestamp,

  /// Signed or unsigned integer with a specified bit width.
  integer,

  /// Always null.
  unknown,

  /// Embedded JSON document.
  json,

  /// Embedded BSON document.
  bson,

  /// UUID.
  uuid,

  /// IEEE-754 half precision value.
  float16,

  /// Variant value.
  variant,

  /// Geospatial geometry.
  geometry,

  /// Geospatial geography.
  geography,

  /// A reference to a file or byte range.
  file,
}

/// Edge interpolation algorithm for the `GEOGRAPHY` logical type.
enum EdgeInterpolationAlgorithm {
  /// Spherical interpolation.
  spherical(0),

  /// Vincenty interpolation.
  vincenty(1),

  /// Thomas interpolation.
  thomas(2),

  /// Andoyer interpolation.
  andoyer(3),

  /// Karney interpolation.
  karney(4);

  const EdgeInterpolationAlgorithm(this.value);

  /// Thrift enum value.
  final int value;

  /// Parses a Thrift enum value, returning `null` for unknown values.
  static EdgeInterpolationAlgorithm? fromValue(int value) {
    for (final candidate in values) {
      if (candidate.value == value) return candidate;
    }
    return null;
  }
}

/// Base class for Parquet's `LogicalType` union.
///
/// Use [LogicalType.fromThrift] to parse the raw Thrift union. Empty unions
/// (`STRING`, `MAP`, ...) are represented by const singletons, e.g.
/// `const StringLogicalType()`.
sealed class LogicalType {
  /// Const constructor for subclasses.
  const LogicalType();

  /// Which union member this is.
  LogicalTypeKind get kind;

  /// Parses a raw Thrift `LogicalType` union map, or returns `null` when the
  /// union is absent/empty.
  static LogicalType? fromThrift(Map<int, Object?>? raw) {
    if (raw == null || raw.isEmpty) return null;
    // A union has exactly one field; tolerate files that carry extras by
    // using the first one in field-id order (the canonical member).
    final fieldId = raw.keys.reduce((a, b) => a < b ? a : b);
    final payload = raw[fieldId];
    switch (fieldId) {
      case 1:
        return const StringLogicalType();
      case 2:
        return const MapLogicalType();
      case 3:
        return const ListLogicalType();
      case 4:
        return const EnumLogicalType();
      case 5:
        final value = _asMap(payload, 'DECIMAL');
        return DecimalLogicalType(
          _requiredInt(value, 1, 'DecimalType.scale'),
          _requiredInt(value, 2, 'DecimalType.precision'),
        );
      case 6:
        return const DateLogicalType();
      case 7:
        final value = _asMap(payload, 'TIME');
        return TimeLogicalType(
          _parseTimeUnit(value[2], 'TimeType.unit'),
          _requiredBool(value, 1, 'TimeType.isAdjustedToUTC'),
        );
      case 8:
        final value = _asMap(payload, 'TIMESTAMP');
        return TimestampLogicalType(
          _parseTimeUnit(value[2], 'TimestampType.unit'),
          _requiredBool(value, 1, 'TimestampType.isAdjustedToUTC'),
        );
      case 10:
        final value = _asMap(payload, 'INTEGER');
        final bitWidth = _requiredInt(value, 1, 'IntType.bitWidth');
        if (bitWidth != 8 &&
            bitWidth != 16 &&
            bitWidth != 32 &&
            bitWidth != 64) {
          throw ParquetFormatException(
            'IntType.bitWidth must be 8, 16, 32 or 64, got $bitWidth',
          );
        }
        return IntegerLogicalType(
          bitWidth,
          _requiredBool(value, 2, 'IntType.isSigned'),
        );
      case 11:
        return const UnknownLogicalType();
      case 12:
        return const JsonLogicalType();
      case 13:
        return const BsonLogicalType();
      case 14:
        return const UuidLogicalType();
      case 15:
        return const Float16LogicalType();
      case 16:
        final value = _asMap(payload, 'VARIANT');
        return VariantLogicalType(_optionalInt(value, 1));
      case 17:
        final value = _asMap(payload, 'GEOMETRY');
        return GeometryLogicalType(_optionalString(value, 1));
      case 18:
        final value = _asMap(payload, 'GEOGRAPHY');
        final algorithm = _optionalInt(value, 2);
        return GeographyLogicalType(
          _optionalString(value, 1),
          algorithm == null
              ? null
              : EdgeInterpolationAlgorithm.fromValue(algorithm),
        );
      case 19:
        return const FileLogicalType();
      default:
        // New writers may add logical-type union members. They are annotations
        // only; readers must still be able to read the physical column.
        return const UnknownLogicalType();
    }
  }
}

/// `STRING` logical type: UTF-8 encoded byte array.
final class StringLogicalType extends LogicalType {
  /// Creates the `STRING` annotation.
  const StringLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.string;

  @override
  bool operator ==(Object other) => other is StringLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `MAP` logical type.
final class MapLogicalType extends LogicalType {
  /// Creates the `MAP` annotation.
  const MapLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.map;

  @override
  bool operator ==(Object other) => other is MapLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `LIST` logical type.
final class ListLogicalType extends LogicalType {
  /// Creates the `LIST` annotation.
  const ListLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.list;

  @override
  bool operator ==(Object other) => other is ListLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `ENUM` logical type.
final class EnumLogicalType extends LogicalType {
  /// Creates the `ENUM` annotation.
  const EnumLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.enumType;

  @override
  bool operator ==(Object other) => other is EnumLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `DECIMAL` logical type.
final class DecimalLogicalType extends LogicalType {
  /// Creates a decimal annotation with [scale] and [precision].
  const DecimalLogicalType(this.scale, this.precision);

  /// Number of fractional digits.
  final int scale;

  /// Maximum number of digits.
  final int precision;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.decimal;

  @override
  bool operator ==(Object other) =>
      other is DecimalLogicalType &&
      other.scale == scale &&
      other.precision == precision;

  @override
  int get hashCode => Object.hash(kind, scale, precision);

  @override
  String toString() => 'DecimalLogicalType($scale, $precision)';
}

/// `DATE` logical type.
final class DateLogicalType extends LogicalType {
  /// Creates the `DATE` annotation.
  const DateLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.date;

  @override
  bool operator ==(Object other) => other is DateLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `TIME` logical type.
final class TimeLogicalType extends LogicalType {
  /// Creates a time annotation for [unit].
  const TimeLogicalType(this.unit, this.isAdjustedToUtc);

  /// Unit of the stored value.
  final TimeUnit unit;

  /// Whether the value is UTC-normalised.
  final bool isAdjustedToUtc;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.time;

  @override
  bool operator ==(Object other) =>
      other is TimeLogicalType &&
      other.unit == unit &&
      other.isAdjustedToUtc == isAdjustedToUtc;

  @override
  int get hashCode => Object.hash(kind, unit, isAdjustedToUtc);

  @override
  String toString() => 'TimeLogicalType($unit, $isAdjustedToUtc)';
}

/// `TIMESTAMP` logical type.
final class TimestampLogicalType extends LogicalType {
  /// Creates a timestamp annotation for [unit].
  const TimestampLogicalType(this.unit, this.isAdjustedToUtc);

  /// Unit of the stored value.
  final TimeUnit unit;

  /// Whether the value is UTC-normalised.
  final bool isAdjustedToUtc;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.timestamp;

  @override
  bool operator ==(Object other) =>
      other is TimestampLogicalType &&
      other.unit == unit &&
      other.isAdjustedToUtc == isAdjustedToUtc;

  @override
  int get hashCode => Object.hash(kind, unit, isAdjustedToUtc);

  @override
  String toString() => 'TimestampLogicalType($unit, $isAdjustedToUtc)';
}

/// `INTEGER` logical type.
final class IntegerLogicalType extends LogicalType {
  /// Creates an integer annotation.
  const IntegerLogicalType(this.bitWidth, this.isSigned);

  /// Meaningful bits: 8, 16, 32 or 64.
  final int bitWidth;

  /// Whether the value is signed.
  final bool isSigned;

  /// Alias matching the Thrift field name.
  bool get signed => isSigned;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.integer;

  @override
  bool operator ==(Object other) =>
      other is IntegerLogicalType &&
      other.bitWidth == bitWidth &&
      other.isSigned == isSigned;

  @override
  int get hashCode => Object.hash(kind, bitWidth, isSigned);

  @override
  String toString() => 'IntegerLogicalType($bitWidth, $isSigned)';
}

/// `UNKNOWN` logical type (all values are null).
final class UnknownLogicalType extends LogicalType {
  /// Creates the `UNKNOWN` annotation.
  const UnknownLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.unknown;

  @override
  bool operator ==(Object other) => other is UnknownLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `JSON` logical type.
final class JsonLogicalType extends LogicalType {
  /// Creates the `JSON` annotation.
  const JsonLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.json;

  @override
  bool operator ==(Object other) => other is JsonLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `BSON` logical type.
final class BsonLogicalType extends LogicalType {
  /// Creates the `BSON` annotation.
  const BsonLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.bson;

  @override
  bool operator ==(Object other) => other is BsonLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `UUID` logical type.
final class UuidLogicalType extends LogicalType {
  /// Creates the `UUID` annotation.
  const UuidLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.uuid;

  @override
  bool operator ==(Object other) => other is UuidLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `FLOAT16` logical type.
final class Float16LogicalType extends LogicalType {
  /// Creates the `FLOAT16` annotation.
  const Float16LogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.float16;

  @override
  bool operator ==(Object other) => other is Float16LogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// `VARIANT` logical type.
final class VariantLogicalType extends LogicalType {
  /// Creates a variant annotation.
  const VariantLogicalType([this.specificationVersion]);

  /// Version of the variant specification used by the writer, if any.
  final int? specificationVersion;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.variant;

  @override
  bool operator ==(Object other) =>
      other is VariantLogicalType &&
      other.specificationVersion == specificationVersion;

  @override
  int get hashCode => Object.hash(kind, specificationVersion);
}

/// `GEOMETRY` logical type.
final class GeometryLogicalType extends LogicalType {
  /// Creates a geometry annotation.
  const GeometryLogicalType([this.crs]);

  /// Optional coordinate reference system definition.
  final String? crs;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.geometry;

  @override
  bool operator ==(Object other) =>
      other is GeometryLogicalType && other.crs == crs;

  @override
  int get hashCode => Object.hash(kind, crs);
}

/// `GEOGRAPHY` logical type.
final class GeographyLogicalType extends LogicalType {
  /// Creates a geography annotation.
  const GeographyLogicalType([this.crs, this.algorithm]);

  /// Optional coordinate reference system definition.
  final String? crs;

  /// Optional edge interpolation algorithm.
  final EdgeInterpolationAlgorithm? algorithm;

  @override
  LogicalTypeKind get kind => LogicalTypeKind.geography;

  @override
  bool operator ==(Object other) =>
      other is GeographyLogicalType &&
      other.crs == crs &&
      other.algorithm == algorithm;

  @override
  int get hashCode => Object.hash(kind, crs, algorithm);
}

/// `FILE` logical type.
final class FileLogicalType extends LogicalType {
  /// Creates the `FILE` annotation.
  const FileLogicalType();

  @override
  LogicalTypeKind get kind => LogicalTypeKind.file;

  @override
  bool operator ==(Object other) => other is FileLogicalType;

  @override
  int get hashCode => kind.hashCode;
}

/// Derives the effective logical type for a legacy `ConvertedType`.
///
/// Parquet files written before `LogicalType` existed (and many files since)
/// only carry a [ConvertedType]. Readers are expected to treat those as the
/// equivalent logical annotation.
LogicalType? logicalTypeFromConverted(
  ConvertedType? convertedType, {
  int? scale,
  int? precision,
}) {
  if (convertedType == null) return null;
  return switch (convertedType) {
    ConvertedType.utf8 => const StringLogicalType(),
    ConvertedType.map => const MapLogicalType(),
    ConvertedType.mapKeyValue => null,
    ConvertedType.list => const ListLogicalType(),
    ConvertedType.enumType => const EnumLogicalType(),
    ConvertedType.decimal => DecimalLogicalType(scale ?? 0, precision ?? 0),
    ConvertedType.date => const DateLogicalType(),
    ConvertedType.timeMillis => const TimeLogicalType(TimeUnit.millis, false),
    ConvertedType.timeMicros => const TimeLogicalType(TimeUnit.micros, false),
    ConvertedType.timestampMillis => const TimestampLogicalType(
      TimeUnit.millis,
      false,
    ),
    ConvertedType.timestampMicros => const TimestampLogicalType(
      TimeUnit.micros,
      false,
    ),
    ConvertedType.uint8 => const IntegerLogicalType(8, false),
    ConvertedType.uint16 => const IntegerLogicalType(16, false),
    ConvertedType.uint32 => const IntegerLogicalType(32, false),
    ConvertedType.uint64 => const IntegerLogicalType(64, false),
    ConvertedType.int8 => const IntegerLogicalType(8, true),
    ConvertedType.int16 => const IntegerLogicalType(16, true),
    ConvertedType.int32 => const IntegerLogicalType(32, true),
    ConvertedType.int64 => const IntegerLogicalType(64, true),
    ConvertedType.json => const JsonLogicalType(),
    ConvertedType.bson => const BsonLogicalType(),
    ConvertedType.interval => null,
  };
}

Map<int, Object?> _asMap(Object? value, String context) {
  if (value is Map<int, Object?>) return value;
  throw ParquetFormatException(
    'expected a struct for $context, got ${value.runtimeType}',
  );
}

int _requiredInt(Map<int, Object?> map, int fieldId, String context) {
  final value = map[fieldId];
  if (value is! int) {
    throw ParquetFormatException('missing required integer $context');
  }
  return value;
}

int? _optionalInt(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  return value is int ? value : null;
}

bool _requiredBool(Map<int, Object?> map, int fieldId, String context) {
  final value = map[fieldId];
  if (value is! bool) {
    throw ParquetFormatException('missing required boolean $context');
  }
  return value;
}

String? _optionalString(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  if (value == null) return null;
  if (value is Uint8List) return _utf8Decode(value, fieldId);
  if (value is String) return value;
  throw ParquetFormatException('field $fieldId is not a string');
}

String _utf8Decode(Uint8List bytes, int fieldId) {
  try {
    return const Utf8Decoder().convert(bytes);
  } on FormatException catch (error) {
    throw ParquetFormatException('field $fieldId is not valid UTF-8', error);
  }
}

TimeUnit _parseTimeUnit(Object? value, String context) {
  if (value is! Map<int, Object?> || value.isEmpty) {
    throw ParquetFormatException('missing $context');
  }
  final fieldId = value.keys.first;
  return switch (fieldId) {
    1 => TimeUnit.millis,
    2 => TimeUnit.micros,
    3 => TimeUnit.nanos,
    _ => throw ParquetFormatException('unknown $context field $fieldId'),
  };
}
