/// Variant binary encoding: value model and JSON conversion.
///
/// A [VariantValue] represents one Variant value. It deliberately keeps the
/// concrete type of the wire value (for example `int8` versus `int64`, or a
/// timestamp with/without a time zone) so that decoding and re-encoding is
/// lossless.
library;

import 'dart:convert';
import 'dart:typed_data';

/// The Variant primitive and container types.
enum VariantType {
  /// JSON null.
  nullValue,

  /// Boolean.
  boolean,

  /// Signed 8-bit integer.
  int8,

  /// Signed 16-bit integer.
  int16,

  /// Signed 32-bit integer.
  int32,

  /// Signed 64-bit integer.
  int64,

  /// IEEE-754 double.
  doubleValue,

  /// 32-bit decimal.
  decimal4,

  /// 64-bit decimal.
  decimal8,

  /// 128-bit decimal.
  decimal16,

  /// Days since the Unix epoch.
  date,

  /// Timestamp with time zone, microseconds since the Unix epoch.
  timestampMicros,

  /// Timestamp without time zone, microseconds since the Unix epoch.
  timestampNtzMicros,

  /// IEEE-754 32-bit float.
  float,

  /// Arbitrary bytes.
  binary,

  /// UTF-8 string.
  string,

  /// Time without time zone, microseconds since midnight.
  time,

  /// Timestamp with time zone, nanoseconds since the Unix epoch.
  timestampNanos,

  /// Timestamp without time zone, nanoseconds since the Unix epoch.
  timestampNtzNanos,

  /// UUID.
  uuid,

  /// Ordered sequence of Variant values.
  array,

  /// String-keyed collection of Variant values.
  object,

  /// Half-precision float convenience type. The Variant binary encoding has no
  /// `FLOAT16` primitive; values of this type are widened to `FLOAT` (32-bit)
  /// when encoded and decode back as [VariantFloat].
  float16,
}

/// Integer widths available to [VariantInt].
enum VariantIntWidth {
  /// 1-byte signed integer.
  int8(VariantType.int8, 3, 1, -128, 127),

  /// 2-byte signed integer.
  int16(VariantType.int16, 4, 2, -32768, 32767),

  /// 4-byte signed integer.
  int32(VariantType.int32, 5, 4, -2147483648, 2147483647),

  /// 8-byte signed integer.
  int64(VariantType.int64, 6, 8, null, null);

  const VariantIntWidth(
    this.type,
    this.primitiveId,
    this.byteWidth,
    this.minValue,
    this.maxValue,
  );

  /// Model type corresponding to this width.
  final VariantType type;

  /// Variant primitive type id.
  final int primitiveId;

  /// Number of bytes on the wire.
  final int byteWidth;

  /// Minimum value representable by this width.
  final int? minValue;

  /// Maximum value representable by this width.
  final int? maxValue;

  /// Whether [value] fits this width.
  ///
  /// `int64` has no representable bounds in JavaScript, so every Dart `int`
  /// fits it on both the VM and the web.
  bool contains(int value) =>
      minValue == null || (value >= minValue! && value <= maxValue!);

  /// Smallest width that can represent [value].
  static VariantIntWidth smallestFor(int value) {
    for (final width in values) {
      if (width.contains(value)) return width;
    }
    throw ArgumentError.value(value, 'value', 'does not fit in int64');
  }
}

/// Decimal widths available to [VariantDecimal].
enum VariantDecimalWidth {
  /// 4-byte unscaled integer, at most 9 digits.
  decimal4(VariantType.decimal4, 8, 4, 9),

  /// 8-byte unscaled integer, at most 18 digits.
  decimal8(VariantType.decimal8, 9, 8, 18),

  /// 16-byte unscaled integer, at most 38 digits.
  decimal16(VariantType.decimal16, 10, 16, 38);

  const VariantDecimalWidth(
    this.type,
    this.primitiveId,
    this.byteWidth,
    this.maxPrecision,
  );

  /// Model type corresponding to this width.
  final VariantType type;

  /// Variant primitive type id.
  final int primitiveId;

  /// Number of bytes of the unscaled value on the wire.
  final int byteWidth;

  /// Maximum decimal precision representable by this width.
  final int maxPrecision;

  /// Largest unscaled value allowed by [maxPrecision].
  BigInt get maxUnscaledValue => BigInt.from(10).pow(maxPrecision) - BigInt.one;

  /// Whether this width can represent `unscaled * 10^-scale`.
  bool canRepresent(BigInt unscaled, int scale) =>
      scale >= 0 && scale <= maxPrecision && unscaled.abs() <= maxUnscaledValue;
}

/// Unit of a [VariantTimestamp].
enum VariantTimeUnit {
  /// Microseconds since the Unix epoch.
  micros,

  /// Nanoseconds since the Unix epoch.
  nanos,
}

/// A decoded Variant value.
///
/// Use [VariantValue.fromJson] for a JSON-compatible value or one of the
/// concrete subclasses (for example [VariantString] or [VariantObject]) to
/// control the exact wire type.
sealed class VariantValue {
  /// Const constructor for subclasses.
  const VariantValue();

  /// The concrete Variant type.
  VariantType get type;

  /// Converts this value to a JSON-compatible Dart value.
  ///
  /// Binary values use Base64 strings, UUIDs use the canonical hyphenated
  /// form, decimals use their exact decimal string, and temporal values use
  /// ISO-8601 strings. Integers are returned as Dart `int`; on dart2js,
  /// `int64` values outside the exact IEEE-754 range (±2^53) therefore inherit
  /// JavaScript number precision limits.
  Object? toJson();

  /// Adds object field names contained in this value to [fieldNames].
  void collectFieldNames(Set<String> fieldNames) {}

  /// Converts a JSON-compatible value into a Variant value.
  ///
  /// JSON numbers become the smallest integer type that fits or
  /// [VariantDouble] for fractional numbers. [Uint8List] becomes
  /// [VariantBinary]; maps must have string keys.
  static VariantValue fromJson(Object? json, {int maxDepth = 128}) =>
      _fromJson(json, 0, maxDepth);

  /// The singleton null value.
  static const VariantNull nullValue = VariantNull();

  /// Creates a boolean value.
  static VariantBoolean boolean(bool value) => VariantBoolean(value);

  /// Creates an integer, choosing the smallest width when [width] is omitted.
  static VariantInt integer(int value, [VariantIntWidth? width]) =>
      VariantInt(value, width);

  /// Creates an int8 value.
  static VariantInt int8(int value) => VariantInt(value, VariantIntWidth.int8);

  /// Creates an int16 value.
  static VariantInt int16(int value) =>
      VariantInt(value, VariantIntWidth.int16);

  /// Creates an int32 value.
  static VariantInt int32(int value) =>
      VariantInt(value, VariantIntWidth.int32);

  /// Creates an int64 value.
  static VariantInt int64(int value) =>
      VariantInt(value, VariantIntWidth.int64);

  /// Creates a double value.
  static VariantDouble doubleValue(double value) => VariantDouble(value);

  /// Creates a float value.
  static VariantFloat floatValue(double value) => VariantFloat(value);

  /// Creates a string value.
  static VariantString string(String value) => VariantString(value);

  /// Creates a binary value.
  static VariantBinary binary(Iterable<int> value) => VariantBinary(value);

  /// Creates an array value.
  static VariantArray array(Iterable<VariantValue> values) =>
      VariantArray(values);

  /// Creates an object value.
  static VariantObject object(Map<String, VariantValue> fields) =>
      VariantObject(fields);

  @override
  String toString() => toJson().toString();
}

/// The Variant null value.
final class VariantNull extends VariantValue {
  /// Creates the null value.
  const VariantNull();

  @override
  VariantType get type => VariantType.nullValue;

  @override
  Object? toJson() => null;

  @override
  bool operator ==(Object other) => other is VariantNull;

  @override
  int get hashCode => VariantType.nullValue.hashCode;
}

/// A Variant boolean.
final class VariantBoolean extends VariantValue {
  /// Creates a boolean value.
  const VariantBoolean(this.value);

  /// Boolean value.
  final bool value;

  @override
  VariantType get type => VariantType.boolean;

  @override
  Object? toJson() => value;

  @override
  bool operator ==(Object other) =>
      other is VariantBoolean && other.value == value;

  @override
  int get hashCode => Object.hash(VariantType.boolean, value);
}

/// A Variant signed integer with a concrete width.
final class VariantInt extends VariantValue {
  /// Creates an integer.
  ///
  /// When [width] is omitted the smallest width that fits [value] is used.
  factory VariantInt(int value, [VariantIntWidth? width]) {
    final actualWidth = width ?? VariantIntWidth.smallestFor(value);
    if (!actualWidth.contains(value)) {
      throw ArgumentError.value(
        value,
        'value',
        'does not fit in ${actualWidth.name}',
      );
    }
    return VariantInt._(value, actualWidth);
  }

  VariantInt._(this.value, this.width);

  /// Creates an integer from a [BigInt], failing if it does not fit in int64.
  factory VariantInt.fromBigInt(BigInt value, [VariantIntWidth? width]) {
    final intValue = value.toInt();
    if (BigInt.from(intValue) != value) {
      throw ArgumentError.value(value, 'value', 'does not fit in int64');
    }
    return VariantInt(intValue, width);
  }

  /// Integer value.
  ///
  /// On dart2js, `int64` values outside the exact IEEE-754 range (±2^53) lose
  /// precision. Native Dart integers are exact.
  final int value;

  /// Wire width.
  final VariantIntWidth width;

  @override
  VariantType get type => width.type;

  @override
  Object? toJson() => value;

  @override
  bool operator ==(Object other) =>
      other is VariantInt && other.value == value && other.width == width;

  @override
  int get hashCode => Object.hash(type, value);
}

/// A Variant IEEE-754 double.
final class VariantDouble extends VariantValue {
  /// Creates a double value.
  const VariantDouble(this.value);

  /// Double value.
  final double value;

  @override
  VariantType get type => VariantType.doubleValue;

  @override
  Object? toJson() => value;

  @override
  bool operator ==(Object other) =>
      other is VariantDouble && other.value == value;

  @override
  int get hashCode => Object.hash(VariantType.doubleValue, value);
}

/// A Variant IEEE-754 32-bit float, stored as a Dart [double].
final class VariantFloat extends VariantValue {
  /// Creates a float value.
  const VariantFloat(this.value);

  /// Float value (promoted to double).
  final double value;

  @override
  VariantType get type => VariantType.float;

  @override
  Object? toJson() => value;

  @override
  bool operator ==(Object other) =>
      other is VariantFloat && other.value == value;

  @override
  int get hashCode => Object.hash(VariantType.float, value);
}

/// A half-precision float convenience value.
///
/// The Variant binary encoding does not define a `FLOAT16` primitive. This
/// class exists for callers that hold half-precision data; it is widened to a
/// 32-bit [VariantFloat] on the wire and therefore decodes as [VariantFloat].
final class VariantFloat16 extends VariantValue {
  /// Creates a half-precision float value.
  const VariantFloat16(this.value);

  /// Value, promoted to double.
  final double value;

  @override
  VariantType get type => VariantType.float16;

  @override
  Object? toJson() => value;

  @override
  bool operator ==(Object other) =>
      other is VariantFloat16 && other.value == value;

  @override
  int get hashCode => Object.hash(VariantType.float16, value);
}

/// A Variant fixed-point decimal.
final class VariantDecimal extends VariantValue {
  /// Creates a decimal from its unscaled integer and [scale].
  ///
  /// When [width] is omitted the smallest decimal width that can represent the
  /// value is used. The scale must be between 0 and 38 and the unscaled value
  /// must fit the chosen width's precision.
  factory VariantDecimal(
    BigInt unscaled, {
    int scale = 0,
    VariantDecimalWidth? width,
  }) {
    if (scale < 0 || scale > 38) {
      throw ArgumentError.value(scale, 'scale', 'must be between 0 and 38');
    }
    final actualWidth = width ?? _smallestDecimalWidth(unscaled, scale);
    if (!actualWidth.canRepresent(unscaled, scale)) {
      throw ArgumentError(
        'unscaled value $unscaled with scale $scale does not fit in '
        '${actualWidth.name}',
      );
    }
    return VariantDecimal._(unscaled, scale, actualWidth);
  }

  VariantDecimal._(this.unscaledValue, this.scale, this.width);

  /// Creates a decimal from an integer unscaled value.
  factory VariantDecimal.fromInt(
    int unscaled, {
    int scale = 0,
    VariantDecimalWidth? width,
  }) => VariantDecimal(BigInt.from(unscaled), scale: scale, width: width);

  /// Unscaled integer value.
  final BigInt unscaledValue;

  /// Number of fractional digits.
  final int scale;

  /// Wire width.
  final VariantDecimalWidth width;

  /// Number of significant decimal digits (at least 1).
  int get precision {
    final digits = unscaledValue.abs().toString().length;
    return digits == 0 ? 1 : digits;
  }

  @override
  VariantType get type => width.type;

  /// Exact decimal representation, for example `12.34`.
  String get decimalString {
    final negative = unscaledValue.isNegative;
    var digits = unscaledValue.abs().toString();
    if (scale == 0) return '${negative ? '-' : ''}$digits';
    if (digits.length <= scale) {
      digits = digits.padLeft(scale + 1, '0');
    }
    final split = digits.length - scale;
    return '${negative ? '-' : ''}${digits.substring(0, split)}.'
        '${digits.substring(split)}';
  }

  @override
  Object? toJson() => decimalString;

  @override
  bool operator ==(Object other) =>
      other is VariantDecimal &&
      other.unscaledValue == unscaledValue &&
      other.scale == scale &&
      other.width == width;

  @override
  int get hashCode => Object.hash(type, unscaledValue, scale);
}

/// A Variant date, stored as days since the Unix epoch.
final class VariantDate extends VariantValue {
  /// Creates a date.
  VariantDate(this.daysSinceEpoch) {
    if (daysSinceEpoch < -2147483648 || daysSinceEpoch > 2147483647) {
      throw ArgumentError.value(
        daysSinceEpoch,
        'daysSinceEpoch',
        'does not fit in a signed 32-bit integer',
      );
    }
  }

  /// Days since 1970-01-01.
  final int daysSinceEpoch;

  @override
  VariantType get type => VariantType.date;

  @override
  Object? toJson() => _formatDate(daysSinceEpoch);

  @override
  bool operator ==(Object other) =>
      other is VariantDate && other.daysSinceEpoch == daysSinceEpoch;

  @override
  int get hashCode => Object.hash(VariantType.date, daysSinceEpoch);
}

/// A Variant time without time zone, stored as microseconds since midnight.
final class VariantTime extends VariantValue {
  /// Creates a time value.
  VariantTime(this.microsSinceMidnight) {
    if (microsSinceMidnight < 0 || microsSinceMidnight >= 86400000000) {
      throw ArgumentError.value(
        microsSinceMidnight,
        'microsSinceMidnight',
        'must be in [0, 86_400_000_000)',
      );
    }
  }

  /// Microseconds since midnight.
  final int microsSinceMidnight;

  @override
  VariantType get type => VariantType.time;

  @override
  Object? toJson() => _formatTime(microsSinceMidnight);

  @override
  bool operator ==(Object other) =>
      other is VariantTime && other.microsSinceMidnight == microsSinceMidnight;

  @override
  int get hashCode => Object.hash(VariantType.time, microsSinceMidnight);
}

/// A Variant timestamp.
final class VariantTimestamp extends VariantValue {
  /// Creates a timestamp from [value] in [unit].
  const VariantTimestamp({
    required this.value,
    required this.unit,
    required this.isAdjustedToUtc,
  });

  /// Creates a microsecond timestamp.
  factory VariantTimestamp.micros(int value, {bool isAdjustedToUtc = true}) =>
      VariantTimestamp(
        value: value,
        unit: VariantTimeUnit.micros,
        isAdjustedToUtc: isAdjustedToUtc,
      );

  /// Creates a nanosecond timestamp.
  factory VariantTimestamp.nanos(int value, {bool isAdjustedToUtc = true}) =>
      VariantTimestamp(
        value: value,
        unit: VariantTimeUnit.nanos,
        isAdjustedToUtc: isAdjustedToUtc,
      );

  /// Microseconds or nanoseconds since the Unix epoch.
  final int value;

  /// Unit of [value].
  final VariantTimeUnit unit;

  /// Whether the value is UTC-normalised.
  final bool isAdjustedToUtc;

  @override
  VariantType get type {
    if (unit == VariantTimeUnit.micros) {
      return isAdjustedToUtc
          ? VariantType.timestampMicros
          : VariantType.timestampNtzMicros;
    }
    return isAdjustedToUtc
        ? VariantType.timestampNanos
        : VariantType.timestampNtzNanos;
  }

  @override
  Object? toJson() => _formatTimestamp(value, unit, isAdjustedToUtc);

  @override
  bool operator ==(Object other) =>
      other is VariantTimestamp &&
      other.value == value &&
      other.unit == unit &&
      other.isAdjustedToUtc == isAdjustedToUtc;

  @override
  int get hashCode => Object.hash(type, value);
}

/// A Variant byte string.
final class VariantBinary extends VariantValue {
  /// Creates a binary value, copying [value].
  VariantBinary(Iterable<int> value)
    : bytes = Uint8List.fromList(value.toList(growable: false));

  /// Bytes.
  final Uint8List bytes;

  @override
  VariantType get type => VariantType.binary;

  @override
  Object? toJson() => base64Encode(bytes);

  @override
  bool operator ==(Object other) =>
      other is VariantBinary && _bytesEqual(bytes, other.bytes);

  @override
  int get hashCode => Object.hash(VariantType.binary, Object.hashAll(bytes));
}

/// A Variant UTF-8 string.
final class VariantString extends VariantValue {
  /// Creates a string value.
  const VariantString(this.value);

  /// String value.
  final String value;

  @override
  VariantType get type => VariantType.string;

  @override
  Object? toJson() => value;

  @override
  bool operator ==(Object other) =>
      other is VariantString && other.value == value;

  @override
  int get hashCode => Object.hash(VariantType.string, value);
}

/// A Variant UUID stored as 16 big-endian bytes.
final class VariantUuid extends VariantValue {
  /// Creates a UUID from exactly 16 bytes.
  VariantUuid(Iterable<int> value)
    : bytes = Uint8List.fromList(value.toList(growable: false)) {
    if (bytes.length != 16) {
      throw ArgumentError.value(
        bytes.length,
        'value',
        'UUID must be exactly 16 bytes',
      );
    }
  }

  /// Creates a UUID from a canonical hyphenated string.
  factory VariantUuid.fromString(String value) {
    final hex = value.replaceAll('-', '');
    if (hex.length != 32) {
      throw ArgumentError.value(value, 'value', 'invalid UUID');
    }
    final bytes = Uint8List(16);
    for (var i = 0; i < 16; i++) {
      final parsed = int.tryParse(hex.substring(i * 2, i * 2 + 2), radix: 16);
      if (parsed == null) {
        throw ArgumentError.value(value, 'value', 'invalid UUID');
      }
      bytes[i] = parsed;
    }
    return VariantUuid(bytes);
  }

  /// Raw UUID bytes, in big-endian order.
  final Uint8List bytes;

  @override
  VariantType get type => VariantType.uuid;

  /// Canonical lowercase hyphenated UUID string.
  String get uuidString {
    final hex = StringBuffer();
    for (var i = 0; i < bytes.length; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) hex.write('-');
      hex.write(bytes[i].toRadixString(16).padLeft(2, '0'));
    }
    return hex.toString();
  }

  @override
  Object? toJson() => uuidString;

  @override
  bool operator ==(Object other) =>
      other is VariantUuid && _bytesEqual(bytes, other.bytes);

  @override
  int get hashCode => Object.hash(VariantType.uuid, Object.hashAll(bytes));
}

/// A Variant array.
final class VariantArray extends VariantValue {
  /// Creates an array, copying [elements].
  VariantArray(Iterable<VariantValue> elements)
    : elements = List<VariantValue>.unmodifiable(elements);

  /// Creates an empty array.
  VariantArray.empty() : elements = const <VariantValue>[];

  /// Array elements.
  final List<VariantValue> elements;

  /// Number of elements.
  int get length => elements.length;

  /// Element at [index].
  VariantValue operator [](int index) => elements[index];

  @override
  VariantType get type => VariantType.array;

  @override
  Object? toJson() => <Object?>[
    for (final element in elements) element.toJson(),
  ];

  @override
  void collectFieldNames(Set<String> fieldNames) {
    for (final element in elements) {
      element.collectFieldNames(fieldNames);
    }
  }

  @override
  bool operator ==(Object other) {
    if (other is! VariantArray || other.elements.length != elements.length) {
      return false;
    }
    for (var i = 0; i < elements.length; i++) {
      if (other.elements[i] != elements[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(VariantType.array, Object.hashAll(elements));
}

/// A Variant object with string field names.
final class VariantObject extends VariantValue {
  /// Creates an object from [fields], copying the map.
  VariantObject(Map<String, VariantValue> fields)
    : fields = Map<String, VariantValue>.unmodifiable(
        Map<String, VariantValue>.from(fields),
      );

  /// Creates an empty object.
  VariantObject.empty() : fields = const <String, VariantValue>{};

  /// Object fields.
  final Map<String, VariantValue> fields;

  /// Number of fields.
  int get length => fields.length;

  /// Field value for [name], or `null`.
  VariantValue? operator [](String name) => fields[name];

  /// Whether a field named [name] exists.
  bool containsKey(String name) => fields.containsKey(name);

  @override
  VariantType get type => VariantType.object;

  @override
  Object? toJson() => <String, Object?>{
    for (final entry in fields.entries) entry.key: entry.value.toJson(),
  };

  @override
  void collectFieldNames(Set<String> fieldNames) {
    for (final entry in fields.entries) {
      fieldNames.add(entry.key);
      entry.value.collectFieldNames(fieldNames);
    }
  }

  @override
  bool operator ==(Object other) {
    if (other is! VariantObject || other.fields.length != fields.length) {
      return false;
    }
    for (final entry in fields.entries) {
      if (!other.fields.containsKey(entry.key) ||
          other.fields[entry.key] != entry.value) {
        return false;
      }
    }
    return true;
  }

  @override
  int get hashCode {
    final names = fields.keys.toList()..sort();
    var hash = Object.hash(VariantType.object, fields.length);
    for (final name in names) {
      hash = Object.hash(hash, name, fields[name]);
    }
    return hash;
  }
}

VariantDecimalWidth _smallestDecimalWidth(BigInt unscaled, int scale) {
  for (final width in VariantDecimalWidth.values) {
    if (width.canRepresent(unscaled, scale)) return width;
  }
  throw ArgumentError(
    'decimal value $unscaled with scale $scale exceeds 38 digits',
  );
}

VariantValue _fromJson(Object? json, int depth, int maxDepth) {
  if (depth > maxDepth) {
    throw ArgumentError('JSON value exceeds nesting depth $maxDepth');
  }
  if (json == null) return const VariantNull();
  if (json is bool) return VariantBoolean(json);
  if (json is int) return VariantInt(json);
  if (json is double) return VariantDouble(json);
  if (json is String) return VariantString(json);
  if (json is Uint8List) return VariantBinary(json);
  if (json is List) {
    return VariantArray(<VariantValue>[
      for (final element in json) _fromJson(element, depth + 1, maxDepth),
    ]);
  }
  if (json is Map) {
    final fields = <String, VariantValue>{};
    for (final entry in json.entries) {
      final key = entry.key;
      if (key is! String) {
        throw ArgumentError.value(
          key,
          'json',
          'Variant object keys must be strings',
        );
      }
      fields[key] = _fromJson(entry.value, depth + 1, maxDepth);
    }
    return VariantObject(fields);
  }
  throw ArgumentError.value(json, 'json', 'not JSON-compatible');
}

bool _bytesEqual(Uint8List a, Uint8List b) {
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}

String _two(int value) => value.toString().padLeft(2, '0');

String _year(int value) {
  if (value < 0) return '-${(-value).toString().padLeft(4, '0')}';
  return value.toString().padLeft(4, '0');
}

String _formatDate(int daysSinceEpoch) {
  try {
    final value = DateTime.utc(1970).add(Duration(days: daysSinceEpoch));
    return '${_year(value.year)}-${_two(value.month)}-${_two(value.day)}';
  } on Object {
    // Outside DateTime's range; keep a deterministic representation.
    return daysSinceEpoch.toString();
  }
}

String _formatTime(int microsSinceMidnight) {
  final hours = microsSinceMidnight ~/ 3600000000;
  final minutes = (microsSinceMidnight % 3600000000) ~/ 60000000;
  final seconds = (microsSinceMidnight % 60000000) ~/ 1000000;
  final micros = microsSinceMidnight % 1000000;
  return '${_two(hours)}:${_two(minutes)}:${_two(seconds)}.'
      '${micros.toString().padLeft(6, '0')}';
}

String _formatTimestamp(int value, VariantTimeUnit unit, bool isAdjustedToUtc) {
  final nanos = unit == VariantTimeUnit.nanos;
  final micros = nanos ? _floorDiv(value, 1000) : value;
  final extraNanos = nanos ? value - micros * 1000 : 0;
  try {
    final timestamp = DateTime.fromMicrosecondsSinceEpoch(micros, isUtc: true);
    final base =
        '${_year(timestamp.year)}-${_two(timestamp.month)}-'
        '${_two(timestamp.day)}T${_two(timestamp.hour)}:'
        '${_two(timestamp.minute)}:${_two(timestamp.second)}';
    final microsOfSecond = timestamp.millisecond * 1000 + timestamp.microsecond;
    final fraction = nanos
        ? '${microsOfSecond.toString().padLeft(6, '0')}'
              '${extraNanos.toString().padLeft(3, '0')}'
        : microsOfSecond.toString().padLeft(6, '0');
    return '$base.$fraction${isAdjustedToUtc ? 'Z' : ''}';
  } on Object {
    return value.toString();
  }
}

int _floorDiv(int value, int divisor) {
  final quotient = value ~/ divisor;
  final remainder = value % divisor;
  return remainder != 0 && value.isNegative ? quotient - 1 : quotient;
}
