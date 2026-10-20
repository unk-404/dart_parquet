/// Repetition / definition level decoding.
///
/// Parquet stores levels as `numRequiredBits(maxLevel)` bit-wide unsigned
/// integers. The encoding depends on the page version and the encoding declared
/// in the page header:
///
/// * [Encoding.rle] - RLE / bit-packed hybrid. In data pages v1 the byte stream
///   is prefixed with a 4-byte little-endian length ([lengthPrefix] must be
///   `true`); in data pages v2 no prefix is present.
/// * [Encoding.bitPacked] - deprecated, most significant bit first packing with
///   no length prefix. The byte count is derived from `numValues`.
///
/// Levels are at most `maxLevel`; for practical schemas that is `<= 255`, so a
/// [Uint8List] is returned. Deeper schemas (> 255 levels) are rejected with a
/// [ParquetUnsupportedException].
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../format/enums.dart' show Encoding;
import '../util/errors.dart';
import 'bitpacking.dart' show packedByteLength, requiredBits, unpackBitsMSB;
import 'rle.dart' show decodeRleBitPacked, readRleBitPackedLength;

/// Decodes [numValues] repetition or definition levels.
///
/// [lengthPrefix] must be `true` for RLE levels stored in a data page v1. It is
/// ignored for the deprecated `BIT_PACKED` encoding unless the caller knows
/// that the legacy writer emitted a length prefix as well; pass `false` for the
/// standard `BIT_PACKED` layout.
Uint8List decodeLevels(
  Bytes data, {
  required int maxLevel,
  required int numValues,
  required Encoding encoding,
  bool lengthPrefix = false,
}) {
  if (maxLevel < 0) {
    throw ArgumentError.value(maxLevel, 'maxLevel', 'must not be negative');
  }
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  if (maxLevel > 0xFF) {
    throw ParquetUnsupportedException(
      'definition/repetition levels deeper than 255 are not supported '
      '(maxLevel=$maxLevel)',
    );
  }
  if (encoding != Encoding.rle && encoding != Encoding.bitPacked) {
    throw ParquetFormatException('invalid level encoding $encoding');
  }
  if (maxLevel == 0) {
    // Nothing is stored for a single-level (required) column.
    return Uint8List(numValues);
  }

  final bitWidth = requiredBits(maxLevel);
  final out = Uint8List(numValues);

  if (encoding == Encoding.rle) {
    final payload = lengthPrefix ? _lengthPrefixedPayload(data) : data;
    final values = decodeRleBitPacked(
      payload,
      bitWidth: bitWidth,
      numValues: numValues,
    );
    for (var i = 0; i < numValues; i++) {
      final value = values[i];
      if (value > maxLevel) {
        throw ParquetFormatException(
          'level $value at index $i exceeds the maximum level $maxLevel',
        );
      }
      out[i] = value;
    }
    return out;
  }

  // Deprecated BIT_PACKED levels: MSB-first packing, no length prefix by
  // default. The packed size follows from the value count.
  Bytes payload;
  if (lengthPrefix) {
    payload = _lengthPrefixedPayload(data);
  } else {
    final needed = packedByteLength(bitWidth, numValues);
    if (data.length < needed) {
      throw ParquetEofException(
        'BIT_PACKED levels truncated: need $needed bytes, only ${data.length} '
        'available',
      );
    }
    payload = data.slice(0, needed);
  }
  final values = Uint32List(numValues);
  unpackBitsMSB(payload, bitWidth: bitWidth, numValues: numValues, out: values);
  for (var i = 0; i < numValues; i++) {
    final value = values[i];
    if (value > maxLevel) {
      throw ParquetFormatException(
        'level $value at index $i exceeds the maximum level $maxLevel',
      );
    }
    out[i] = value;
  }
  return out;
}

/// Convenience wrapper that returns levels as a [Uint32List] and supports the
/// full 16-bit level range used by the format.
Uint32List decodeLevelsWide(
  Bytes data, {
  required int maxLevel,
  required int numValues,
  required Encoding encoding,
  bool lengthPrefix = false,
}) {
  if (maxLevel < 0 || maxLevel > 0xFFFF) {
    throw ParquetFormatException('invalid maximum level $maxLevel');
  }
  if (encoding != Encoding.rle && encoding != Encoding.bitPacked) {
    throw ParquetFormatException('invalid level encoding $encoding');
  }
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  if (maxLevel == 0) return Uint32List(numValues);
  final bitWidth = requiredBits(maxLevel);
  final out = Uint32List(numValues);
  if (encoding == Encoding.rle) {
    final payload = lengthPrefix ? _lengthPrefixedPayload(data) : data;
    final values = decodeRleBitPacked(
      payload,
      bitWidth: bitWidth,
      numValues: numValues,
    );
    for (var i = 0; i < numValues; i++) {
      final value = values[i];
      if (value > maxLevel) {
        throw ParquetFormatException(
          'level $value at index $i exceeds the maximum level $maxLevel',
        );
      }
      out[i] = value;
    }
    return out;
  }
  final needed = packedByteLength(bitWidth, numValues);
  if (lengthPrefix) {
    unpackBitsMSB(
      _lengthPrefixedPayload(data),
      bitWidth: bitWidth,
      numValues: numValues,
      out: out,
    );
  } else {
    if (data.length < needed) {
      throw ParquetEofException(
        'BIT_PACKED levels truncated: need $needed bytes, only ${data.length} '
        'available',
      );
    }
    unpackBitsMSB(data, bitWidth: bitWidth, numValues: numValues, out: out);
  }
  for (var i = 0; i < numValues; i++) {
    final value = out[i];
    if (value > maxLevel) {
      throw ParquetFormatException(
        'level $value at index $i exceeds the maximum level $maxLevel',
      );
    }
  }
  return out;
}

Bytes _lengthPrefixedPayload(Bytes data) {
  final length = readRleBitPackedLength(data);
  final end = 4 + length;
  if (end > data.length) {
    throw ParquetEofException(
      'level stream declares $length bytes at offset 4 but only '
      '${data.length - 4} are available',
    );
  }
  return data.slice(4, end);
}
