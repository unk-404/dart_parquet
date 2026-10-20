/// Bit-packing primitives used by the Parquet encodings.
///
/// The modern Parquet encodings (RLE/bit-packed hybrid, DELTA_BINARY_PACKED,
/// PLAIN booleans and BYTE_STREAM_SPLIT) pack values little-endian bit order:
/// the first value occupies the least significant bits of the first byte and
/// subsequent values follow towards the most significant end.
///
/// The deprecated [Encoding.bitPacked] level encoding packs values the other way
/// around (most significant bit first); that order is implemented by
/// [readBitsMSB] / [unpackBitsMSB].
///
/// All functions operate on a zero-copy `Bytes` view and never read past its
/// end: truncated input raises a [ParquetEofException].
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';

/// Number of bits required to represent values in `[0, maxValue]`.
///
/// Returns 0 for a maximum of 0, matching the Parquet notion of a zero bit-wide
/// (constant zero) stream.
int requiredBits(int maxValue) {
  if (maxValue < 0) {
    throw ArgumentError.value(maxValue, 'maxValue', 'must not be negative');
  }
  var bits = 0;
  var value = maxValue;
  while (value != 0) {
    bits++;
    value >>>= 1;
  }
  return bits;
}

/// Number of bytes needed to hold [numValues] values of [bitWidth] bits.
///
/// The result is rounded up to a whole byte, as every Parquet bit-packed
/// stream is byte aligned.
int packedByteLength(int bitWidth, int numValues) {
  if (bitWidth < 0) {
    throw ArgumentError.value(bitWidth, 'bitWidth', 'must not be negative');
  }
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  return (bitWidth * numValues + 7) >>> 3;
}

void _checkBitWidth(int bitWidth, int max) {
  if (bitWidth < 0 || bitWidth > max) {
    throw ParquetFormatException(
      'invalid bit width $bitWidth (expected 0..$max)',
    );
  }
}

/// Reads a single little-endian (LSB-first) packed value of [bitWidth] bits.
///
/// [bitOffset] is an absolute bit position inside [data]. Values of up to 32
/// bits are supported; larger values use [readBitsLSBLimbs].
int readBitsLSB(Bytes data, int bitOffset, int bitWidth) {
  _checkBitWidth(bitWidth, 32);
  if (bitWidth == 0) return 0;
  if (bitOffset < 0) {
    throw RangeError.value(bitOffset, 'bitOffset', 'must not be negative');
  }

  final byteStart = bitOffset >>> 3;
  var bitInByte = bitOffset & 7;
  final byteCount = (bitInByte + bitWidth + 7) >>> 3;
  if (byteStart + byteCount > data.length) {
    throw ParquetEofException(
      'bit-packed stream truncated: need $byteCount bytes at $byteStart, '
      'only ${data.length} bytes available',
    );
  }

  // Byte-wise assembly keeps every intermediate value within 32 bits, which
  // makes this exact on both the VM and dart2js.
  var value = 0;
  var outShift = 0;
  var remaining = bitWidth;
  var position = byteStart;
  while (remaining > 0) {
    var take = 8 - bitInByte;
    if (take > remaining) take = remaining;
    final chunk =
        (data.getUint8Unchecked(position) >>> bitInByte) & ((1 << take) - 1);
    value |= chunk << outShift;
    remaining -= take;
    outShift += take;
    bitInByte = 0;
    position++;
  }
  return value;
}

/// Reads a single little-endian packed value of up to 64 bits as two limbs.
///
/// The returned `lo` and `hi` are unsigned 32-bit values; together they form
/// the value's two's-complement 64-bit bit pattern. This representation is
/// exact on the web, where `int` cannot hold values above 2^53.
(int lo, int hi) readBitsLSBLimbs(Bytes data, int bitOffset, int bitWidth) {
  _checkBitWidth(bitWidth, 64);
  if (bitWidth <= 32) {
    return (readBitsLSB(data, bitOffset, bitWidth), 0);
  }
  final lo = readBitsLSB(data, bitOffset, 32);
  final hi = readBitsLSB(data, bitOffset + 32, bitWidth - 32);
  return (lo, hi);
}

/// Reads a single legacy (`BIT_PACKED`) value, most significant bit first.
int readBitsMSB(Bytes data, int bitOffset, int bitWidth) {
  _checkBitWidth(bitWidth, 32);
  if (bitWidth == 0) return 0;
  if (bitOffset < 0) {
    throw RangeError.value(bitOffset, 'bitOffset', 'must not be negative');
  }
  final byteCount = (bitWidth + 7) >>> 3;
  final byteStart = bitOffset >>> 3;
  if (byteStart + byteCount > data.length) {
    throw ParquetEofException(
      'bit-packed (legacy) stream truncated: need $byteCount bytes at '
      '$byteStart, only ${data.length} bytes available',
    );
  }
  var value = 0;
  for (var i = 0; i < bitWidth; i++) {
    final bitPosition = bitOffset + i;
    final bit =
        (data.getUint8Unchecked(bitPosition >>> 3) >>>
            (7 - (bitPosition & 7))) &
        1;
    value = (value << 1) | bit;
  }
  return value;
}

/// Unpacks [numValues] little-endian packed values into [out].
///
/// [outOffset] must leave room for all values in [out]. For `bitWidth == 0`
/// the stream occupies no bytes and [out] is filled with zeros.
void unpackBitsLSB(
  Bytes data, {
  required int bitWidth,
  required int numValues,
  required Uint32List out,
  int outOffset = 0,
}) {
  _checkBitWidth(bitWidth, 32);
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  if (outOffset < 0 || outOffset + numValues > out.length) {
    throw RangeError.range(
      outOffset,
      0,
      out.length - numValues,
      'outOffset',
      'not enough room in the output buffer',
    );
  }
  if (numValues == 0) return;
  if (bitWidth == 0) {
    out.fillRange(outOffset, outOffset + numValues, 0);
    return;
  }
  final needed = packedByteLength(bitWidth, numValues);
  if (data.length < needed) {
    throw ParquetEofException(
      'bit-packed stream truncated: need $needed bytes, only ${data.length} '
      'available',
    );
  }
  var bitOffset = 0;
  for (var i = 0; i < numValues; i++) {
    out[outOffset + i] = readBitsLSB(data, bitOffset, bitWidth);
    bitOffset += bitWidth;
  }
}

/// Like [unpackBitsLSB], but writes into an [Int32List].
void unpackBitsLSBInt32(
  Bytes data, {
  required int bitWidth,
  required int numValues,
  required Int32List out,
  int outOffset = 0,
}) {
  _checkBitWidth(bitWidth, 32);
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  if (outOffset < 0 || outOffset + numValues > out.length) {
    throw RangeError.range(
      outOffset,
      0,
      out.length - numValues,
      'outOffset',
      'not enough room in the output buffer',
    );
  }
  if (numValues == 0) return;
  if (bitWidth == 0) {
    out.fillRange(outOffset, outOffset + numValues, 0);
    return;
  }
  final needed = packedByteLength(bitWidth, numValues);
  if (data.length < needed) {
    throw ParquetEofException(
      'bit-packed stream truncated: need $needed bytes, only ${data.length} '
      'available',
    );
  }
  var bitOffset = 0;
  for (var i = 0; i < numValues; i++) {
    out[outOffset + i] = readBitsLSB(data, bitOffset, bitWidth);
    bitOffset += bitWidth;
  }
}

/// Unpacks [numValues] legacy (`BIT_PACKED`) values, most significant bit
/// first, into [out].
void unpackBitsMSB(
  Bytes data, {
  required int bitWidth,
  required int numValues,
  required Uint32List out,
  int outOffset = 0,
}) {
  _checkBitWidth(bitWidth, 32);
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  if (outOffset < 0 || outOffset + numValues > out.length) {
    throw RangeError.range(
      outOffset,
      0,
      out.length - numValues,
      'outOffset',
      'not enough room in the output buffer',
    );
  }
  if (numValues == 0) return;
  if (bitWidth == 0) {
    out.fillRange(outOffset, outOffset + numValues, 0);
    return;
  }
  final needed = packedByteLength(bitWidth, numValues);
  if (data.length < needed) {
    throw ParquetEofException(
      'bit-packed (legacy) stream truncated: need $needed bytes, only '
      '${data.length} available',
    );
  }
  var bitOffset = 0;
  for (var i = 0; i < numValues; i++) {
    out[outOffset + i] = readBitsMSB(data, bitOffset, bitWidth);
    bitOffset += bitWidth;
  }
}
