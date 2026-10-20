/// Decoders for the Parquet DELTA encodings.
///
/// Implemented encodings:
/// * `DELTA_BINARY_PACKED` for INT32 and INT64,
/// * `DELTA_LENGTH_BYTE_ARRAY`,
/// * `DELTA_BYTE_ARRAY` (incremental / prefix encoding).
///
/// A `DELTA_BINARY_PACKED` stream starts with the block size, the number of
/// miniblocks per block, the total value count and the zigzag-encoded first
/// value. Each block then stores a zigzag-encoded minimum delta, one bit width
/// byte per miniblock and the LSB-first bit-packed residuals
/// (`delta - minDelta`). Every *used* miniblock is padded to its full value
/// count, so the decoder must consume the full miniblock even when only a few
/// values are needed; the trailing (unused) miniblocks of the last block carry
/// no payload (their width bytes are ignored).
///
/// 64-bit arithmetic is exact on the web: the native implementation uses
/// `int`, while the web implementation uses `BigInt` for values that may exceed
/// 2^53.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/bytes.dart' show ByteReader;
import '../util/errors.dart';
import '../util/int64.dart' show Int64Buffer;
import 'bitpacking.dart' show readBitsLSB, readBitsLSBLimbs;
import 'plain.dart' show ByteArrayData;

/// Lazily detects the platform-specific [Int64Buffer] implementation.
final bool _nativeInt64 = Int64Buffer(1).isNative;

/// Internal header of a DELTA_BINARY_PACKED stream.
final class _DeltaHeader {
  _DeltaHeader({
    required this.blockSize,
    required this.miniBlocksPerBlock,
    required this.totalCount,
    required this.valuesPerMiniBlock,
  });

  final int blockSize;
  final int miniBlocksPerBlock;
  final int totalCount;
  final int valuesPerMiniBlock;
}

/// Reads and validates the block layout part of the header.
///
/// The first value follows this header and is decoded by the caller because its
/// type (int32 vs int64) affects the decoding path.
_DeltaHeader _readHeader(ByteReader reader) {
  final blockSize = reader.readVarint32();
  final miniBlocksPerBlock = reader.readVarint32();
  final totalCount = reader.readVarint32();

  if (miniBlocksPerBlock == 0) {
    throw ParquetFormatException(
      'DELTA_BINARY_PACKED header has zero miniblocks per block',
    );
  }
  if (blockSize == 0 || blockSize % 128 != 0) {
    throw ParquetFormatException(
      "DELTA_BINARY_PACKED block size $blockSize is not a positive multiple "
      'of 128',
    );
  }
  if (blockSize % miniBlocksPerBlock != 0) {
    throw ParquetFormatException(
      'DELTA_BINARY_PACKED block size $blockSize is not a multiple of '
      '$miniBlocksPerBlock miniblocks',
    );
  }
  final valuesPerMiniBlock = blockSize ~/ miniBlocksPerBlock;
  if (valuesPerMiniBlock % 32 != 0) {
    throw ParquetFormatException(
      'DELTA_BINARY_PACKED miniblock size $valuesPerMiniBlock is not a '
      'multiple of 32',
    );
  }
  return _DeltaHeader(
    blockSize: blockSize,
    miniBlocksPerBlock: miniBlocksPerBlock,
    totalCount: totalCount,
    valuesPerMiniBlock: valuesPerMiniBlock,
  );
}

void _checkExpectedCount(int? expectedCount, int actualCount, String encoding) {
  if (expectedCount != null && expectedCount != actualCount) {
    throw ParquetFormatException(
      '$encoding stream contains $actualCount values but the page declares '
      '$expectedCount',
    );
  }
}

Uint8List _readBitWidths(ByteReader reader, int count) {
  if (reader.remaining < count) {
    throw ParquetEofException(
      'DELTA_BINARY_PACKED block needs $count bit width bytes but only '
      '${reader.remaining} remain',
    );
  }
  final widths = Uint8List(count);
  for (var i = 0; i < count; i++) {
    widths[i] = reader.readUint8();
  }
  return widths;
}

/// Reads a zigzag-encoded 64-bit value as a native two's-complement `int`.
int _readZigZag64Native(ByteReader reader) {
  var result = 0;
  var shift = 0;
  while (true) {
    final byte = reader.readUint8();
    if (shift == 63 && (byte & 0x7E) != 0) {
      throw ParquetFormatException(
        'zigzag varint64 overflows at offset ${reader.position}',
      );
    }
    result |= (byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) break;
    shift += 7;
    if (shift >= 70) {
      throw ParquetFormatException(
        'zigzag varint64 overflow at offset ${reader.position}',
      );
    }
  }
  return (result >>> 1) ^ -(result & 1);
}

/// Reads a zigzag-encoded 64-bit value as a [BigInt] (web path).
BigInt _readZigZag64Big(ByteReader reader) {
  var result = BigInt.zero;
  var shift = 0;
  while (true) {
    final byte = reader.readUint8();
    if (shift == 63 && (byte & 0x7E) != 0) {
      throw ParquetFormatException(
        'zigzag varint64 overflows at offset ${reader.position}',
      );
    }
    result |= BigInt.from(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) break;
    shift += 7;
    if (shift >= 70) {
      throw ParquetFormatException(
        'zigzag varint64 overflow at offset ${reader.position}',
      );
    }
  }
  return (result >> 1) ^ -(result & BigInt.one);
}

/// Decodes a DELTA_BINARY_PACKED INT32 stream.
///
/// The value count is taken from the stream header. [expectedCount] optionally
/// cross-checks it against the page's value count (useful for validating
/// malformed input early).
Int32List decodeDeltaBinaryPackedInt32(Bytes data, {int? expectedCount}) {
  final reader = ByteReader(data);
  return _decodeDeltaInt32(reader, expectedCount: expectedCount);
}

/// Decodes a DELTA_BINARY_PACKED INT64 stream into an exact [Int64Buffer].
Int64Buffer decodeDeltaBinaryPackedInt64(Bytes data, {int? expectedCount}) {
  final reader = ByteReader(data);
  return _nativeInt64
      ? _decodeDeltaInt64Native(reader, expectedCount: expectedCount)
      : _decodeDeltaInt64Web(reader, expectedCount: expectedCount);
}

/// Decodes a DELTA_LENGTH_BYTE_ARRAY stream.
///
/// The values are a DELTA_BINARY_PACKED INT32 sequence of lengths followed by
/// the concatenated value bytes.
ByteArrayData decodeDeltaLengthByteArray(Bytes data, {int? expectedCount}) {
  final reader = ByteReader(data);
  final lengths = _decodeDeltaInt32(reader, expectedCount: expectedCount);
  final count = lengths.length;
  final dataStart = reader.position;

  var total = 0;
  for (var i = 0; i < count; i++) {
    final length = lengths[i];
    if (length < 0) {
      throw ParquetFormatException(
        'DELTA_LENGTH_BYTE_ARRAY has negative length $length at value $i',
      );
    }
    total += length;
    if (total > 0xFFFFFFFF) {
      throw ParquetRangeException('DELTA_LENGTH_BYTE_ARRAY data exceeds 4 GiB');
    }
  }
  if (data.length - dataStart < total) {
    throw ParquetEofException(
      'DELTA_LENGTH_BYTE_ARRAY needs $total value bytes at $dataStart but only '
      '${data.length - dataStart} remain',
    );
  }

  final out = Uint8List(total);
  final offsets = Uint32List(count + 1);
  final source = data.asUint8List();
  if (total > 0) {
    out.setRange(0, total, source, dataStart);
  }
  var target = 0;
  for (var i = 0; i < count; i++) {
    offsets[i] = target;
    target += lengths[i];
  }
  offsets[count] = target;
  return ByteArrayData(out, offsets);
}

/// Decodes a DELTA_BYTE_ARRAY stream (incremental prefix encoding).
///
/// Prefix lengths are DELTA_BINARY_PACKED INT32 values, followed by the suffixes
/// (encoded exactly like DELTA_LENGTH_BYTE_ARRAY). Value `i` is
/// `previous[0..prefixLength[i]] + suffix[i]`.
ByteArrayData decodeDeltaByteArray(Bytes data, {int? expectedCount}) {
  final reader = ByteReader(data);
  final prefixLengths = _decodeDeltaInt32(reader);
  final suffixLengths = _decodeDeltaInt32(reader);
  final count = prefixLengths.length;
  if (suffixLengths.length != count) {
    throw ParquetFormatException(
      'DELTA_BYTE_ARRAY prefix stream has $count values but suffix stream has '
      '${suffixLengths.length}',
    );
  }
  _checkExpectedCount(expectedCount, count, 'DELTA_BYTE_ARRAY');

  final suffixStart = reader.position;
  var suffixTotal = 0;
  for (var i = 0; i < count; i++) {
    final length = suffixLengths[i];
    if (length < 0) {
      throw ParquetFormatException(
        'DELTA_BYTE_ARRAY has negative suffix length $length at value $i',
      );
    }
    suffixTotal += length;
  }
  if (data.length - suffixStart < suffixTotal) {
    throw ParquetEofException(
      'DELTA_BYTE_ARRAY needs $suffixTotal suffix bytes at $suffixStart but '
      'only ${data.length - suffixStart} remain',
    );
  }

  // Validate prefixes and compute the total output size.
  var total = 0;
  var previousLength = 0;
  for (var i = 0; i < count; i++) {
    final prefix = prefixLengths[i];
    if (prefix < 0) {
      throw ParquetFormatException(
        'DELTA_BYTE_ARRAY has negative prefix length $prefix at value $i',
      );
    }
    if (prefix > previousLength) {
      throw ParquetFormatException(
        'DELTA_BYTE_ARRAY prefix length $prefix at value $i exceeds the '
        'previous value length $previousLength',
      );
    }
    previousLength = prefix + suffixLengths[i];
    total += previousLength;
    if (total > 0xFFFFFFFF) {
      throw ParquetRangeException('DELTA_BYTE_ARRAY data exceeds 4 GiB');
    }
  }

  final out = Uint8List(total);
  final offsets = Uint32List(count + 1);
  final source = data.asUint8List();
  var sourcePosition = suffixStart;
  var previousOffset = 0;
  previousLength = 0;
  var target = 0;
  for (var i = 0; i < count; i++) {
    final prefix = prefixLengths[i];
    final suffixLength = suffixLengths[i];
    offsets[i] = target;
    if (prefix > 0) {
      // The prefix is copied from the previous value already written to [out].
      out.setRange(target, target + prefix, out, previousOffset);
    }
    if (suffixLength > 0) {
      out.setRange(
        target + prefix,
        target + prefix + suffixLength,
        source,
        sourcePosition,
      );
      sourcePosition += suffixLength;
    }
    previousOffset = target;
    previousLength = prefix + suffixLength;
    target += previousLength;
  }
  offsets[count] = target;
  return ByteArrayData(out, offsets);
}

Int32List _decodeDeltaInt32(ByteReader reader, {int? expectedCount}) {
  final header = _readHeader(reader);
  final firstValue = reader.readZigZag32();
  _checkExpectedCount(expectedCount, header.totalCount, 'DELTA_BINARY_PACKED');

  final total = header.totalCount;
  final out = Int32List(total);
  if (total == 0) return out;
  out[0] = firstValue;

  final valuesPerMiniBlock = header.valuesPerMiniBlock;
  var written = 1;
  var current = firstValue;
  while (written < total) {
    final minDelta = reader.readZigZag32();
    final widths = _readBitWidths(reader, header.miniBlocksPerBlock);

    // Only the miniblocks of the current block are described by [widths]; the
    // last block may be partially filled, in which case only its used
    // miniblocks carry payload.
    final valuesInBlock = total - written < header.blockSize
        ? total - written
        : header.blockSize;
    var remainingInBlock = valuesInBlock;
    final usedMiniblocks =
        (valuesInBlock + valuesPerMiniBlock - 1) ~/ valuesPerMiniBlock;
    for (var m = 0; m < usedMiniblocks; m++) {
      final width = widths[m];
      if (width > 32) {
        throw ParquetFormatException(
          'DELTA_BINARY_PACKED INT32 miniblock bit width $width exceeds 32',
        );
      }
      final count = remainingInBlock < valuesPerMiniBlock
          ? remainingInBlock
          : valuesPerMiniBlock;
      // Every used miniblock, including a partially filled trailing one, is
      // padded to its full value count, so it always occupies whole bytes.
      final byteLength = (valuesPerMiniBlock * width) >>> 3;
      if (reader.remaining < byteLength) {
        throw ParquetEofException(
          'DELTA_BINARY_PACKED miniblock needs $byteLength bytes but only '
          '${reader.remaining} remain',
        );
      }
      final start = reader.position;
      if (width == 0) {
        for (var k = 0; k < count; k++) {
          current += minDelta;
          out[written++] = current;
        }
      } else {
        final payload = reader.bytes;
        var bitOffset = start * 8;
        for (var k = 0; k < count; k++) {
          final residual = readBitsLSB(payload, bitOffset, width);
          bitOffset += width;
          current += minDelta + residual;
          out[written++] = current;
        }
      }
      reader.position = start + byteLength;
      remainingInBlock -= count;
    }
  }
  return out;
}

Int64Buffer _decodeDeltaInt64Native(ByteReader reader, {int? expectedCount}) {
  final header = _readHeader(reader);
  final firstValue = _readZigZag64Native(reader);
  _checkExpectedCount(expectedCount, header.totalCount, 'DELTA_BINARY_PACKED');

  final total = header.totalCount;
  final out = Int64Buffer(total);
  if (total == 0) return out;
  out.setInt(0, firstValue);

  final valuesPerMiniBlock = header.valuesPerMiniBlock;
  var written = 1;
  var current = firstValue;
  while (written < total) {
    final minDelta = _readZigZag64Native(reader);
    final widths = _readBitWidths(reader, header.miniBlocksPerBlock);

    // Only the miniblocks of the current block are described by [widths]; the
    // last block may be partially filled, in which case only its used
    // miniblocks carry payload.
    final valuesInBlock = total - written < header.blockSize
        ? total - written
        : header.blockSize;
    var remainingInBlock = valuesInBlock;
    final usedMiniblocks =
        (valuesInBlock + valuesPerMiniBlock - 1) ~/ valuesPerMiniBlock;
    for (var m = 0; m < usedMiniblocks; m++) {
      final width = widths[m];
      if (width > 64) {
        throw ParquetFormatException(
          'DELTA_BINARY_PACKED INT64 miniblock bit width $width exceeds 64',
        );
      }
      final count = remainingInBlock < valuesPerMiniBlock
          ? remainingInBlock
          : valuesPerMiniBlock;
      final byteLength = (valuesPerMiniBlock * width) >>> 3;
      if (reader.remaining < byteLength) {
        throw ParquetEofException(
          'DELTA_BINARY_PACKED miniblock needs $byteLength bytes but only '
          '${reader.remaining} remain',
        );
      }
      final start = reader.position;
      if (width == 0) {
        for (var k = 0; k < count; k++) {
          current += minDelta;
          out.setInt(written++, current);
        }
      } else {
        final payload = reader.bytes;
        var bitOffset = start * 8;
        for (var k = 0; k < count; k++) {
          final (lo, hi) = readBitsLSBLimbs(payload, bitOffset, width);
          bitOffset += width;
          // Reassemble the unsigned residual into a native 64-bit two's
          // complement int; addition wraps at 64 bits like the reference
          // implementation.
          final residual = (hi << 32) | lo;
          current += minDelta + residual;
          out.setInt(written++, current);
        }
      }
      reader.position = start + byteLength;
      remainingInBlock -= count;
    }
  }
  return out;
}

Int64Buffer _decodeDeltaInt64Web(ByteReader reader, {int? expectedCount}) {
  final header = _readHeader(reader);
  final firstValue = _readZigZag64Big(reader);
  _checkExpectedCount(expectedCount, header.totalCount, 'DELTA_BINARY_PACKED');

  final total = header.totalCount;
  final out = Int64Buffer(total);
  if (total == 0) return out;
  out.setBigInt(0, firstValue);

  final valuesPerMiniBlock = header.valuesPerMiniBlock;
  var written = 1;
  var current = firstValue;
  while (written < total) {
    final minDelta = _readZigZag64Big(reader);
    final widths = _readBitWidths(reader, header.miniBlocksPerBlock);

    // Only the miniblocks of the current block are described by [widths]; the
    // last block may be partially filled, in which case only its used
    // miniblocks carry payload.
    final valuesInBlock = total - written < header.blockSize
        ? total - written
        : header.blockSize;
    var remainingInBlock = valuesInBlock;
    final usedMiniblocks =
        (valuesInBlock + valuesPerMiniBlock - 1) ~/ valuesPerMiniBlock;
    for (var m = 0; m < usedMiniblocks; m++) {
      final width = widths[m];
      if (width > 64) {
        throw ParquetFormatException(
          'DELTA_BINARY_PACKED INT64 miniblock bit width $width exceeds 64',
        );
      }
      final count = remainingInBlock < valuesPerMiniBlock
          ? remainingInBlock
          : valuesPerMiniBlock;
      final byteLength = (valuesPerMiniBlock * width) >>> 3;
      if (reader.remaining < byteLength) {
        throw ParquetEofException(
          'DELTA_BINARY_PACKED miniblock needs $byteLength bytes but only '
          '${reader.remaining} remain',
        );
      }
      final start = reader.position;
      if (width == 0) {
        for (var k = 0; k < count; k++) {
          current = _wrapInt64(current + minDelta);
          out.setBigInt(written++, current);
        }
      } else {
        final payload = reader.bytes;
        var bitOffset = start * 8;
        for (var k = 0; k < count; k++) {
          final (lo, hi) = readBitsLSBLimbs(payload, bitOffset, width);
          bitOffset += width;
          final residual = (BigInt.from(hi) << 32) | BigInt.from(lo);
          current = _wrapInt64(current + minDelta + residual);
          out.setBigInt(written++, current);
        }
      }
      reader.position = start + byteLength;
      remainingInBlock -= count;
    }
  }
  return out;
}

final BigInt _two64 = BigInt.one << 64;
final BigInt _two63 = BigInt.one << 63;

/// Reduces [value] modulo 2^64 and interprets it as a signed 64-bit integer.
BigInt _wrapInt64(BigInt value) {
  final unsigned = value & (_two64 - BigInt.one);
  return unsigned >= _two63 ? unsigned - _two64 : unsigned;
}
