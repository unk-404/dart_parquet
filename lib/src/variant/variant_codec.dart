/// Variant binary encoding: value encoder/decoder.
library;

import 'dart:convert';
import 'dart:typed_data';

import '../util/errors.dart';
import 'variant_metadata.dart';
import 'variant_value.dart';

/// Maximum number of nested objects/arrays accepted by the codec.
///
/// This matches arrow-rs' `MAX_NESTING_DEPTH` and serde_json's default
/// recursion limit.
const int maxVariantNestingDepth = 128;

/// Encodes and decodes the Variant value binary.
///
/// The metadata dictionary is passed separately because a Variant value is
/// only meaningful together with its metadata. See `Variant` for the pair.
abstract final class VariantCodec {
  /// Decodes one Variant value.
  ///
  /// [valueBytes] must contain exactly one encoded Variant value; trailing
  /// bytes are rejected. Throws [ParquetFormatException] for malformed input
  /// and [ParquetUnsupportedException] for an unknown primitive type.
  static VariantValue decode(
    List<int> valueBytes,
    VariantMetadata metadata, {
    int maxDepth = maxVariantNestingDepth,
  }) {
    final bytes = valueBytes is Uint8List
        ? valueBytes
        : Uint8List.fromList(valueBytes);
    final reader = _VariantReader(bytes);
    final value = _decodeValue(reader, metadata, 0, maxDepth);
    if (reader.position != reader.limit) {
      throw ParquetFormatException(
        'Variant value has ${reader.remaining} trailing byte(s)',
      );
    }
    return value;
  }

  /// Alias for [decode].
  static VariantValue decodeValue(
    List<int> valueBytes,
    VariantMetadata metadata, {
    int maxDepth = maxVariantNestingDepth,
  }) => decode(valueBytes, metadata, maxDepth: maxDepth);

  /// Encodes [value] using [metadata].
  ///
  /// Throws [ArgumentError] when [value] cannot be represented (for example an
  /// object field missing from [metadata]) and when the nesting depth exceeds
  /// [maxDepth].
  static Uint8List encode(
    VariantValue value,
    VariantMetadata metadata, {
    int maxDepth = maxVariantNestingDepth,
  }) => _encodeValue(value, metadata, 0, maxDepth);

  /// Alias for [encode].
  static Uint8List encodeValue(
    VariantValue value,
    VariantMetadata metadata, {
    int maxDepth = maxVariantNestingDepth,
  }) => encode(value, metadata, maxDepth: maxDepth);
}

VariantValue _decodeValue(
  _VariantReader reader,
  VariantMetadata metadata,
  int depth,
  int maxDepth,
) {
  final header = reader.readUint8();
  final basicType = header & 0x03;
  switch (basicType) {
    case 0:
      return _decodePrimitive(reader, header >> 2);
    case 1:
      final length = header >> 2;
      return VariantString(
        _decodeUtf8(reader.readBytes(length), 'short string'),
      );
    case 2:
      return _decodeObject(reader, header >> 2, metadata, depth, maxDepth);
    case 3:
      return _decodeArray(reader, header >> 2, metadata, depth, maxDepth);
    default:
      throw ParquetFormatException('invalid Variant basic type $basicType');
  }
}

VariantValue _decodePrimitive(_VariantReader reader, int primitiveId) {
  switch (primitiveId) {
    case 0:
      return const VariantNull();
    case 1:
      return const VariantBoolean(true);
    case 2:
      return const VariantBoolean(false);
    case 3:
      return VariantInt(reader.readInt8(), VariantIntWidth.int8);
    case 4:
      return VariantInt(reader.readInt16(), VariantIntWidth.int16);
    case 5:
      return VariantInt(reader.readInt32(), VariantIntWidth.int32);
    case 6:
      return VariantInt(reader.readInt64(), VariantIntWidth.int64);
    case 7:
      return VariantDouble(reader.readFloat64());
    case 8:
    case 9:
    case 10:
      return _decodeDecimal(reader, primitiveId);
    case 11:
      return VariantDate(reader.readInt32());
    case 12:
      return VariantTimestamp.micros(reader.readInt64());
    case 13:
      return VariantTimestamp.micros(
        reader.readInt64(),
        isAdjustedToUtc: false,
      );
    case 14:
      return VariantFloat(reader.readFloat32());
    case 15:
      final length = reader.readUint32();
      if (length > reader.remaining) {
        throw ParquetFormatException(
          'Variant binary length $length exceeds remaining '
          '${reader.remaining} bytes',
        );
      }
      return VariantBinary(reader.readBytes(length));
    case 16:
      final length = reader.readUint32();
      if (length > reader.remaining) {
        throw ParquetFormatException(
          'Variant string length $length exceeds remaining '
          '${reader.remaining} bytes',
        );
      }
      return VariantString(_decodeUtf8(reader.readBytes(length), 'string'));
    case 17:
      final micros = reader.readInt64();
      if (micros < 0 || micros >= 86400000000) {
        throw ParquetFormatException(
          'Variant time value $micros is outside [0, 86_400_000_000)',
        );
      }
      return VariantTime(micros);
    case 18:
      return VariantTimestamp.nanos(reader.readInt64());
    case 19:
      return VariantTimestamp.nanos(reader.readInt64(), isAdjustedToUtc: false);
    case 20:
      final uuid = reader.readBytes(16);
      return VariantUuid(uuid);
    default:
      throw ParquetUnsupportedException(
        'unknown Variant primitive type $primitiveId',
      );
  }
}

VariantValue _decodeDecimal(_VariantReader reader, int primitiveId) {
  final width = switch (primitiveId) {
    8 => VariantDecimalWidth.decimal4,
    9 => VariantDecimalWidth.decimal8,
    10 => VariantDecimalWidth.decimal16,
    _ => throw ParquetFormatException(
      'invalid Variant decimal primitive $primitiveId',
    ),
  };
  final scale = reader.readUint8();
  if (scale > width.maxPrecision) {
    throw ParquetFormatException(
      'Variant ${width.name} scale $scale exceeds maximum precision '
      '${width.maxPrecision}',
    );
  }
  final unscaled = reader.readSignedBigInt(width.byteWidth);
  if (unscaled.abs() > width.maxUnscaledValue) {
    throw ParquetFormatException(
      'Variant ${width.name} unscaled value $unscaled exceeds '
      '${width.maxPrecision} digits',
    );
  }
  return VariantDecimal(unscaled, scale: scale, width: width);
}

VariantValue _decodeObject(
  _VariantReader reader,
  int valueHeader,
  VariantMetadata metadata,
  int depth,
  int maxDepth,
) {
  if (depth >= maxDepth) {
    throw ParquetFormatException(
      'Variant nesting depth exceeds the maximum of $maxDepth',
    );
  }

  final isLarge = (valueHeader & 0x10) != 0;
  final fieldIdSize = ((valueHeader >> 2) & 0x03) + 1;
  final fieldOffsetSize = (valueHeader & 0x03) + 1;
  final countSize = isLarge ? 4 : 1;
  final count = reader.readUnsigned(countSize);

  final requiredBytes =
      count * (fieldIdSize + fieldOffsetSize) + fieldOffsetSize;
  if (requiredBytes > reader.remaining) {
    throw ParquetFormatException(
      'Variant object with $count fields needs $requiredBytes bytes but only '
      '${reader.remaining} remain',
    );
  }

  final fieldIds = List<int>.generate(
    count,
    (_) => reader.readUnsigned(fieldIdSize),
    growable: false,
  );
  final offsets = List<int>.generate(
    count + 1,
    (_) => reader.readUnsigned(fieldOffsetSize),
    growable: false,
  );
  // Unlike arrays, object field values may be stored in any order, so the
  // first field offset is not necessarily zero. Only the final offset (the
  // end of the object's value data) is used to size the object.
  final valueLength = offsets.last;
  if (valueLength > reader.remaining) {
    throw ParquetFormatException(
      'Variant object value data length $valueLength exceeds remaining '
      '${reader.remaining} bytes',
    );
  }
  for (final offset in offsets) {
    if (offset > valueLength) {
      throw ParquetFormatException(
        'Variant object field offset $offset exceeds value data length '
        '$valueLength',
      );
    }
  }

  final names = <String>[];
  for (var i = 0; i < count; i++) {
    final fieldId = fieldIds[i];
    if (fieldId >= metadata.length) {
      throw ParquetFormatException(
        'Variant object field id $fieldId is outside the metadata dictionary '
        '(${metadata.length} entries)',
      );
    }
    final name = metadata[fieldId];
    if (names.isNotEmpty &&
        compareVariantStrings(names[names.length - 1], name) >= 0) {
      throw ParquetFormatException(
        'Variant object field names are not unique and lexicographically '
        'ordered',
      );
    }
    names.add(name);
  }

  final valueStart = reader.position;
  final valueEnd = valueStart + valueLength;
  final fields = <String, VariantValue>{};
  var maximumEnd = valueStart;
  for (var i = 0; i < count; i++) {
    final child = _VariantReader(
      reader.bytes,
      position: valueStart + offsets[i],
      limit: valueEnd,
    );
    final value = _decodeValue(child, metadata, depth + 1, maxDepth);
    if (child.position > maximumEnd) maximumEnd = child.position;
    fields[names[i]] = value;
  }

  if (count == 0) {
    if (valueLength != 0) {
      throw ParquetFormatException(
        'Variant object has $valueLength trailing value byte(s)',
      );
    }
  } else if (maximumEnd != valueEnd) {
    throw ParquetFormatException(
      'Variant object values end at ${maximumEnd - valueStart}, expected '
      '$valueLength',
    );
  }

  reader.position = valueEnd;
  return VariantObject(fields);
}

VariantValue _decodeArray(
  _VariantReader reader,
  int valueHeader,
  VariantMetadata metadata,
  int depth,
  int maxDepth,
) {
  if (depth >= maxDepth) {
    throw ParquetFormatException(
      'Variant nesting depth exceeds the maximum of $maxDepth',
    );
  }

  final isLarge = (valueHeader & 0x04) != 0;
  final offsetSize = (valueHeader & 0x03) + 1;
  final countSize = isLarge ? 4 : 1;
  final count = reader.readUnsigned(countSize);

  final requiredBytes = (count + 1) * offsetSize;
  if (requiredBytes > reader.remaining) {
    throw ParquetFormatException(
      'Variant array with $count elements needs $requiredBytes offset bytes '
      'but only ${reader.remaining} remain',
    );
  }

  final offsets = List<int>.generate(
    count + 1,
    (_) => reader.readUnsigned(offsetSize),
    growable: false,
  );
  if (offsets.first != 0) {
    throw ParquetFormatException(
      'Variant array first offset is ${offsets.first}, expected 0',
    );
  }
  for (var i = 0; i < count; i++) {
    if (offsets[i] >= offsets[i + 1]) {
      throw ParquetFormatException(
        'Variant array offsets are not strictly increasing at element $i',
      );
    }
  }

  final valueLength = offsets.last;
  if (valueLength > reader.remaining) {
    throw ParquetFormatException(
      'Variant array value data length $valueLength exceeds remaining '
      '${reader.remaining} bytes',
    );
  }
  if (count == 0 && valueLength != 0) {
    throw ParquetFormatException(
      'Variant array has $valueLength trailing value byte(s)',
    );
  }

  final valueStart = reader.position;
  final elements = <VariantValue>[];
  for (var i = 0; i < count; i++) {
    final start = valueStart + offsets[i];
    final end = valueStart + offsets[i + 1];
    final child = _VariantReader(reader.bytes, position: start, limit: end);
    elements.add(_decodeValue(child, metadata, depth + 1, maxDepth));
    if (child.position != end) {
      throw ParquetFormatException(
        'Variant array element $i occupies ${child.position - start} bytes, '
        'offset table says ${end - start}',
      );
    }
  }

  reader.position = valueStart + valueLength;
  return VariantArray(elements);
}

Uint8List _encodeValue(
  VariantValue value,
  VariantMetadata metadata,
  int depth,
  int maxDepth,
) {
  switch (value) {
    case VariantNull():
      return Uint8List.fromList(const [0x00]);
    case VariantBoolean(:final value):
      return Uint8List.fromList(<int>[value ? 0x04 : 0x08]);
    case VariantInt(:final value, :final width):
      final builder = BytesBuilder(copy: false)
        ..addByte(width.primitiveId << 2);
      _writeSignedLittleEndian(builder, BigInt.from(value), width.byteWidth);
      return builder.takeBytes();
    case VariantDouble(:final value):
      final builder = BytesBuilder(copy: false)..addByte(7 << 2);
      _writeFloat64(builder, value);
      return builder.takeBytes();
    case VariantFloat(:final value):
      return _encodeFloat32(value);
    case VariantFloat16(:final value):
      // The Variant binary encoding has no FLOAT16 primitive; widen to FLOAT.
      return _encodeFloat32(value);
    case VariantDecimal(:final unscaledValue, :final scale, :final width):
      final builder = BytesBuilder(copy: false)
        ..addByte(width.primitiveId << 2)
        ..addByte(scale);
      _writeSignedLittleEndian(builder, unscaledValue, width.byteWidth);
      return builder.takeBytes();
    case VariantDate(:final daysSinceEpoch):
      final builder = BytesBuilder(copy: false)..addByte(11 << 2);
      _writeSignedLittleEndian(builder, BigInt.from(daysSinceEpoch), 4);
      return builder.takeBytes();
    case VariantTimestamp(:final value, :final unit, :final isAdjustedToUtc):
      _checkSigned64(value, 'Variant timestamp');
      final primitiveId = switch ((unit, isAdjustedToUtc)) {
        (VariantTimeUnit.micros, true) => 12,
        (VariantTimeUnit.micros, false) => 13,
        (VariantTimeUnit.nanos, true) => 18,
        (VariantTimeUnit.nanos, false) => 19,
      };
      final builder = BytesBuilder(copy: false)..addByte(primitiveId << 2);
      _writeSignedLittleEndian(builder, BigInt.from(value), 8);
      return builder.takeBytes();
    case VariantTime(:final microsSinceMidnight):
      final builder = BytesBuilder(copy: false)..addByte(17 << 2);
      _writeSignedLittleEndian(builder, BigInt.from(microsSinceMidnight), 8);
      return builder.takeBytes();
    case VariantBinary(:final bytes):
      final builder = BytesBuilder(copy: false)..addByte(15 << 2);
      _writeLength(builder, bytes.length);
      builder.add(bytes);
      return builder.takeBytes();
    case VariantString(:final value):
      return _encodeString(value);
    case VariantUuid(:final bytes):
      final builder = BytesBuilder(copy: false)..addByte(20 << 2);
      builder.add(bytes);
      return builder.takeBytes();
    case VariantArray():
      return _encodeArray(value, metadata, depth, maxDepth);
    case VariantObject():
      return _encodeObject(value, metadata, depth, maxDepth);
  }
}

Uint8List _encodeFloat32(double value) {
  final builder = BytesBuilder(copy: false)..addByte(14 << 2);
  final data = ByteData(4)..setFloat32(0, value, Endian.little);
  for (var i = 0; i < 4; i++) {
    builder.addByte(data.getUint8(i));
  }
  return builder.takeBytes();
}

Uint8List _encodeString(String value) {
  final bytes = utf8.encode(value);
  final builder = BytesBuilder(copy: false);
  if (bytes.length <= 63) {
    builder.addByte((bytes.length << 2) | 1);
  } else {
    builder.addByte(16 << 2);
    _writeLength(builder, bytes.length);
  }
  builder.add(bytes);
  return builder.takeBytes();
}

Uint8List _encodeArray(
  VariantArray value,
  VariantMetadata metadata,
  int depth,
  int maxDepth,
) {
  if (depth >= maxDepth) {
    throw ArgumentError(
      'Variant nesting depth exceeds the maximum of $maxDepth',
    );
  }
  final encoded = <Uint8List>[
    for (final element in value.elements)
      _encodeValue(element, metadata, depth + 1, maxDepth),
  ];
  var valueLength = 0;
  for (final element in encoded) {
    valueLength += element.length;
  }
  if (valueLength > 0xFFFFFFFF) {
    throw ArgumentError('Variant array values exceed 4 GiB');
  }

  final count = encoded.length;
  if (count > 0xFFFFFFFF) {
    throw ArgumentError('Variant array exceeds 2^32 elements');
  }
  final isLarge = count > 0xFF;
  final countSize = isLarge ? 4 : 1;
  final offsetSize = _unsignedSize(valueLength);
  final valueHeader = (isLarge ? 0x04 : 0x00) | (offsetSize - 1);

  final builder = BytesBuilder(copy: false)..addByte((valueHeader << 2) | 3);
  _writeUnsigned(builder, count, countSize);
  var offset = 0;
  _writeUnsigned(builder, 0, offsetSize);
  for (final element in encoded) {
    offset += element.length;
    _writeUnsigned(builder, offset, offsetSize);
  }
  for (final element in encoded) {
    builder.add(element);
  }
  return builder.takeBytes();
}

Uint8List _encodeObject(
  VariantObject value,
  VariantMetadata metadata,
  int depth,
  int maxDepth,
) {
  if (depth >= maxDepth) {
    throw ArgumentError(
      'Variant nesting depth exceeds the maximum of $maxDepth',
    );
  }

  final entries = value.fields.entries.toList()
    ..sort((a, b) => compareVariantStrings(a.key, b.key));
  final ids = <int>[];
  final encoded = <Uint8List>[];
  for (final entry in entries) {
    final fieldId = metadata.idOf(entry.key);
    if (fieldId == null) {
      throw ArgumentError.value(
        entry.key,
        'metadata',
        'field is not present in the Variant metadata dictionary',
      );
    }
    ids.add(fieldId);
    encoded.add(_encodeValue(entry.value, metadata, depth + 1, maxDepth));
  }

  var valueLength = 0;
  for (final element in encoded) {
    valueLength += element.length;
  }
  if (valueLength > 0xFFFFFFFF) {
    throw ArgumentError('Variant object values exceed 4 GiB');
  }

  final count = entries.length;
  if (count > 0xFFFFFFFF) {
    throw ArgumentError('Variant object exceeds 2^32 fields');
  }
  var maxFieldId = 0;
  for (final id in ids) {
    if (id > maxFieldId) maxFieldId = id;
  }
  final isLarge = count > 0xFF;
  final countSize = isLarge ? 4 : 1;
  final fieldIdSize = _unsignedSize(maxFieldId);
  final offsetSize = _unsignedSize(valueLength);
  final valueHeader =
      (isLarge ? 0x10 : 0x00) | ((fieldIdSize - 1) << 2) | (offsetSize - 1);

  final builder = BytesBuilder(copy: false)..addByte((valueHeader << 2) | 2);
  _writeUnsigned(builder, count, countSize);
  for (final id in ids) {
    _writeUnsigned(builder, id, fieldIdSize);
  }
  var offset = 0;
  _writeUnsigned(builder, 0, offsetSize);
  for (final element in encoded) {
    offset += element.length;
    _writeUnsigned(builder, offset, offsetSize);
  }
  for (final element in encoded) {
    builder.add(element);
  }
  return builder.takeBytes();
}

int _unsignedSize(int value) {
  if (value <= 0xFF) return 1;
  if (value <= 0xFFFF) return 2;
  if (value <= 0xFFFFFF) return 3;
  return 4;
}

void _checkSigned64(int value, String what) {
  if (value < -9223372036854775808 || value > 9223372036854775807) {
    throw ArgumentError.value(
      value,
      what,
      'does not fit in a signed 64-bit value',
    );
  }
}

void _writeLength(BytesBuilder builder, int length) {
  if (length > 0xFFFFFFFF) {
    throw ArgumentError.value(length, 'length', 'exceeds 4 GiB');
  }
  _writeUnsigned(builder, length, 4);
}

void _writeUnsigned(BytesBuilder builder, int value, int size) {
  if (value < 0) {
    throw ArgumentError.value(value, 'value', 'must be non-negative');
  }
  var remaining = value;
  for (var i = 0; i < size; i++) {
    builder.addByte(remaining & 0xFF);
    remaining ~/= 256;
  }
  if (remaining != 0) {
    throw ArgumentError.value(value, 'value', 'does not fit in $size bytes');
  }
}

void _writeSignedLittleEndian(BytesBuilder builder, BigInt value, int width) {
  final modulus = BigInt.one << (width * 8);
  var remaining = value % modulus;
  for (var i = 0; i < width; i++) {
    builder.addByte((remaining & BigInt.from(0xFF)).toInt());
    remaining >>= 8;
  }
}

void _writeFloat64(BytesBuilder builder, double value) {
  final data = ByteData(8)..setFloat64(0, value, Endian.little);
  for (var i = 0; i < 8; i++) {
    builder.addByte(data.getUint8(i));
  }
}

String _decodeUtf8(Uint8List bytes, String context) {
  try {
    return utf8.decode(bytes, allowMalformed: false);
  } on FormatException catch (error) {
    throw ParquetFormatException('Variant $context is not valid UTF-8', error);
  }
}

final class _VariantReader {
  _VariantReader(this.bytes, {this.position = 0, int? limit})
    : limit = limit ?? bytes.length {
    if (position < 0 || position > this.limit || this.limit > bytes.length) {
      throw ParquetRangeException(
        'invalid Variant reader range: position=$position '
        'limit=${this.limit} length=${bytes.length}',
      );
    }
  }

  final Uint8List bytes;
  int position;
  final int limit;

  int get remaining => limit - position;

  void _need(int count) {
    if (count < 0 || position + count > limit) {
      throw ParquetFormatException(
        'Variant value ended at $position, need $count byte(s) but only '
        '$remaining remain',
      );
    }
  }

  int readUint8() {
    _need(1);
    return bytes[position++];
  }

  int readUint16() {
    _need(2);
    final value = bytes[position] + bytes[position + 1] * 256;
    position += 2;
    return value;
  }

  int readUint32() {
    _need(4);
    final value =
        bytes[position] +
        bytes[position + 1] * 256 +
        bytes[position + 2] * 65536 +
        bytes[position + 3] * 16777216;
    position += 4;
    return value;
  }

  int readInt8() {
    final value = readUint8();
    return value >= 0x80 ? value - 0x100 : value;
  }

  int readInt16() {
    final value = readUint16();
    return value >= 0x8000 ? value - 0x10000 : value;
  }

  int readInt32() {
    final value = readUint32();
    return value >= 0x80000000 ? value - 0x100000000 : value;
  }

  int readInt64() => readSignedBigInt(8).toInt();

  BigInt readSignedBigInt(int width) {
    _need(width);
    var unsigned = BigInt.zero;
    for (var i = 0; i < width; i++) {
      unsigned |= BigInt.from(bytes[position + i]) << (8 * i);
    }
    position += width;
    if ((bytes[position - 1] & 0x80) != 0) {
      unsigned -= BigInt.one << (width * 8);
    }
    return unsigned;
  }

  double readFloat32() {
    final bits = readUint32();
    final data = ByteData(4)..setUint32(0, bits, Endian.little);
    return data.getFloat32(0, Endian.little);
  }

  double readFloat64() {
    final low = readUint32();
    final high = readUint32();
    final data = ByteData(8)
      ..setUint32(0, low, Endian.little)
      ..setUint32(4, high, Endian.little);
    return data.getFloat64(0, Endian.little);
  }

  Uint8List readBytes(int length) {
    _need(length);
    final start = position;
    position += length;
    return Uint8List.sublistView(bytes, start, start + length);
  }

  int readUnsigned(int size) {
    _need(size);
    var value = 0;
    var multiplier = 1;
    for (var i = 0; i < size; i++) {
      value += bytes[position + i] * multiplier;
      multiplier *= 256;
    }
    position += size;
    return value;
  }
}
