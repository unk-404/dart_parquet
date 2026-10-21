/// Decoders for the Parquet PLAIN encoding.
///
/// PLAIN stores values back-to-back with no header. Multi-byte primitives are
/// little-endian. BYTE_ARRAY values are a 4-byte little-endian length followed
/// by that many bytes; FIXED_LEN_BYTE_ARRAY values are simply `typeLength`
/// bytes each.
///
/// The returned containers are the package's canonical decoded shapes:
/// * [Int32List] for INT32,
/// * [Int64Buffer] for INT64 (exact on the web as well),
/// * [Float32List] / [Float64List] for FLOAT / DOUBLE,
/// * [Uint8List] of `0`/`1` for BOOLEAN,
/// * [ByteArrayData] for BYTE_ARRAY,
/// * [FixedLenData] for FIXED_LEN_BYTE_ARRAY (and INT96).
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import '../util/int64.dart' show Int64Buffer;

/// Decoded BYTE_ARRAY / BYTE_STREAM_SPLIT FLBA data.
///
/// Values are stored contiguously in [data]; value `i` occupies
/// `data[offsets[i] .. offsets[i + 1])`. [offsets] has `numValues + 1` entries
/// and starts at 0. Empty values are represented by equal consecutive offsets.
final class ByteArrayData {
  ByteArrayData(this.data, this.offsets);

  /// Concatenated value bytes.
  final Uint8List data;

  /// Start offset of every value plus the end offset of the last value.
  final Uint32List offsets;

  /// Number of decoded values.
  int get numValues => offsets.isEmpty ? 0 : offsets.length - 1;

  /// Returns the bytes of value [index] as a zero-copy view.
  Uint8List valueAt(int index) {
    if (index < 0 || index >= numValues) {
      throw RangeError.range(index, 0, numValues - 1, 'index');
    }
    final start = offsets[index];
    final end = offsets[index + 1];
    return Uint8List.sublistView(data, start, end);
  }
}

/// Decoded FIXED_LEN_BYTE_ARRAY data.
///
/// Value `i` occupies `data[i * width .. (i + 1) * width)`.
final class FixedLenData {
  FixedLenData(this.data, this.width);

  /// Fixed-size values concatenated.
  final Uint8List data;

  /// Byte width of each value (`type_length` in the schema).
  final int width;

  /// Number of decoded values.
  int get numValues => width == 0 ? 0 : data.length ~/ width;

  /// Represents the same values as a [ByteArrayData] with uniform offsets.
  ByteArrayData toByteArrayData() {
    if (width <= 0) {
      throw ParquetFormatException('invalid fixed length $width');
    }
    final count = data.length ~/ width;
    final offsets = Uint32List(count + 1);
    for (var i = 0; i <= count; i++) {
      offsets[i] = i * width;
    }
    return ByteArrayData(data, offsets);
  }

  /// Returns the bytes of value [index] as a zero-copy view.
  Uint8List valueAt(int index) {
    if (width <= 0) {
      throw ParquetFormatException('invalid fixed length $width');
    }
    RangeError.checkValidRange(index, index + 1, numValues);
    return Uint8List.sublistView(data, index * width, (index + 1) * width);
  }
}

void _checkCount(int numValues) {
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
}

void _requireBytes(Bytes data, int needed, String what) {
  if (data.length < needed) {
    throw ParquetEofException(
      '$what truncated: need $needed bytes, only ${data.length} available',
    );
  }
}

/// Decodes PLAIN BOOLEAN values as bytes of `0` and `1`.
///
/// Booleans are packed least significant bit first, eight values per byte, with
/// the final byte padded.
Uint8List decodePlainBoolean(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = (numValues + 7) >>> 3;
  _requireBytes(data, needed, 'PLAIN boolean data');
  final out = Uint8List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = (data.getUint8Unchecked(i >>> 3) >>> (i & 7)) & 1;
  }
  return out;
}

/// Decodes PLAIN INT32 values (little-endian).
Int32List decodePlainInt32(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 4;
  _requireBytes(data, needed, 'PLAIN INT32 data');
  final source = data.asUint8List();
  if (Endian.host == Endian.little &&
      source.offsetInBytes % 4 == 0 &&
      source.length >= needed) {
    return Int32List.view(source.buffer, source.offsetInBytes, numValues);
  }
  final byteData = data.asByteData();
  final out = Int32List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = byteData.getInt32(i * 4, Endian.little);
  }
  return out;
}

/// Decodes PLAIN INT64 values (little-endian) into an exact [Int64Buffer].
Int64Buffer decodePlainInt64(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 8;
  _requireBytes(data, needed, 'PLAIN INT64 data');
  final source = data.asUint8List();
  if (Endian.host == Endian.little) {
    return Int64Buffer.fromBytes(source, length: numValues);
  }
  final byteData = data.asByteData();
  final out = Int64Buffer(numValues);
  for (var i = 0; i < numValues; i++) {
    final lo = byteData.getUint32(i * 8, Endian.little);
    final hi = byteData.getUint32(i * 8 + 4, Endian.little);
    out.setLimbs(i, lo, hi);
  }
  return out;
}

/// Decodes PLAIN FLOAT (32-bit) values (little-endian).
Float32List decodePlainFloat32(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 4;
  _requireBytes(data, needed, 'PLAIN FLOAT data');
  final source = data.asUint8List();
  if (Endian.host == Endian.little &&
      source.offsetInBytes % 4 == 0 &&
      source.length >= needed) {
    return Float32List.view(source.buffer, source.offsetInBytes, numValues);
  }
  final byteData = data.asByteData();
  final out = Float32List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = byteData.getFloat32(i * 4, Endian.little);
  }
  return out;
}

/// Decodes PLAIN DOUBLE (64-bit) values (little-endian).
Float64List decodePlainFloat64(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 8;
  _requireBytes(data, needed, 'PLAIN DOUBLE data');
  final source = data.asUint8List();
  if (Endian.host == Endian.little &&
      source.offsetInBytes % 8 == 0 &&
      source.length >= needed) {
    return Float64List.view(source.buffer, source.offsetInBytes, numValues);
  }
  final byteData = data.asByteData();
  final out = Float64List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = byteData.getFloat64(i * 8, Endian.little);
  }
  return out;
}

/// Decodes PLAIN BYTE_ARRAY values.
ByteArrayData decodePlainByteArray(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final source = data.asUint8List();
  final sourceLength = source.length;
  var position = 0;
  var total = 0;
  for (var i = 0; i < numValues; i++) {
    if (position + 4 > sourceLength) {
      throw ParquetEofException(
        'PLAIN BYTE_ARRAY length prefix for value $i is truncated at '
        '$position',
      );
    }
    final length =
        source[position] |
        (source[position + 1] << 8) |
        (source[position + 2] << 16) |
        (source[position + 3] << 24);
    position += 4;
    if (length < 0 || length > sourceLength - position) {
      throw ParquetEofException(
        'PLAIN BYTE_ARRAY value $i declares $length bytes but only '
        '${sourceLength - position} remain',
      );
    }
    total += length;
    if (total > 0xFFFFFFFF) {
      throw ParquetRangeException(
        'PLAIN BYTE_ARRAY data exceeds 4 GiB (offset space of Uint32List)',
      );
    }
    position += length;
  }

  final out = Uint8List(total);
  final offsets = Uint32List(numValues + 1);
  var sourcePosition = 0;
  var target = 0;
  for (var i = 0; i < numValues; i++) {
    final length =
        source[sourcePosition] |
        (source[sourcePosition + 1] << 8) |
        (source[sourcePosition + 2] << 16) |
        (source[sourcePosition + 3] << 24);
    sourcePosition += 4;
    offsets[i] = target;
    if (length > 0) {
      out.setRange(target, target + length, source, sourcePosition);
      sourcePosition += length;
      target += length;
    }
  }
  offsets[numValues] = target;
  return ByteArrayData(out, offsets);
}

/// Decodes PLAIN FIXED_LEN_BYTE_ARRAY values of [typeLength] bytes each.
FixedLenData decodePlainFixedLenByteArray(
  Bytes data, {
  required int numValues,
  required int typeLength,
}) {
  _checkCount(numValues);
  if (typeLength <= 0) {
    throw ParquetFormatException(
      'invalid FIXED_LEN_BYTE_ARRAY type length $typeLength',
    );
  }
  final needed = numValues * typeLength;
  _requireBytes(data, needed, 'PLAIN FIXED_LEN_BYTE_ARRAY data');
  final out = Uint8List(needed);
  if (needed > 0) {
    out.setRange(0, needed, data.asUint8List());
  }
  return FixedLenData(out, typeLength);
}

/// Decodes PLAIN INT96 values (12 bytes each) as [FixedLenData].
///
/// INT96 is a legacy timestamp representation; the raw 12 bytes are returned so
/// the timestamp layer can choose how to interpret them.
FixedLenData decodePlainInt96(Bytes data, {required int numValues}) {
  return decodePlainFixedLenByteArray(
    data,
    numValues: numValues,
    typeLength: 12,
  );
}
