/// Decoder for the Parquet RLE / bit-packed hybrid encoding.
///
/// Grammar (from the Parquet format specification):
///
/// ```text
/// rle-bit-packed-hybrid: <length>? <encoded-data>
/// encoded-data := <run>*
/// run          := <bit-packed-run> | <rle-run>
/// bit-packed-run := <bit-packed-header> <bit-packed-values>
/// rle-run        := <rle-header> <repeated-value>
/// ```
///
/// A run header is an unsigned LEB128 varint. Its low bit tells the run kind:
/// * `1`: bit-packed run. The run length is `(header >>> 1) * 8` values.
/// * `0`: RLE run. The run length is `header >>> 1` values and the value is
///   stored as `ceil(bitWidth / 8)` little-endian bytes.
///
/// The hybrid encoding is used both for repetition / definition levels and for
/// dictionary indices. In data pages v1 an RLE level stream is prefixed with a
/// 4-byte little-endian byte length; in data pages v2 and for dictionary
/// indices no length prefix is present.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/bytes.dart' show ByteReader;
import '../util/errors.dart';
import 'bitpacking.dart' show unpackBitsLSB;

const int _maxBitWidth = 32;

void _validateArgs(int bitWidth, int numValues) {
  if (bitWidth < 0 || bitWidth > _maxBitWidth) {
    throw ParquetFormatException(
      'invalid RLE/bit-packed bit width $bitWidth (expected 0..32)',
    );
  }
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
}

/// Decodes [numValues] values of an RLE / bit-packed hybrid stream.
///
/// [bitWidth] must be in `0..32`. A bit width of zero decodes as all zeros and
/// consumes no payload bytes. The stream may contain more runs than needed; the
/// trailing runs are ignored (they belong to the page padding).
Uint32List decodeRleBitPacked(
  Bytes data, {
  required int bitWidth,
  required int numValues,
}) {
  _validateArgs(bitWidth, numValues);
  final out = Uint32List(numValues);
  if (numValues == 0) return out;
  _decodeInto(ByteReader(data), bitWidth, out);
  return out;
}

/// Decodes an RLE / bit-packed hybrid stream with a 4-byte little-endian length
/// prefix (the layout used for data page v1 levels).
Uint32List decodeRleBitPackedLengthPrefixed(
  Bytes data, {
  required int bitWidth,
  required int numValues,
}) {
  _validateArgs(bitWidth, numValues);
  final length = readRleBitPackedLength(data);
  final end = 4 + length;
  if (end > data.length) {
    throw ParquetEofException(
      'RLE/bit-packed level stream declares $length bytes at offset 4 but only '
      '${data.length - 4} are available',
    );
  }
  return decodeRleBitPacked(
    data.slice(4, end),
    bitWidth: bitWidth,
    numValues: numValues,
  );
}

/// Reads the 4-byte little-endian byte length prefix of an RLE level stream.
int readRleBitPackedLength(Bytes data) {
  if (data.length < 4) {
    throw ParquetEofException(
      'need 4 bytes for the RLE length prefix, only ${data.length} available',
    );
  }
  return data.getUint32(0, endian: Endian.little);
}

/// Decodes [numValues] values and returns them as an [Int32List].
///
/// Dictionary indices are conceptually non-negative 32-bit values, but storing
/// them in an [Int32List] keeps the allocation compact and matches the physical
/// INT32 index type. Values with bit 31 set are preserved bit-for-bit.
Int32List decodeRleBitPackedInt32(
  Bytes data, {
  required int bitWidth,
  required int numValues,
}) {
  final values = decodeRleBitPacked(
    data,
    bitWidth: bitWidth,
    numValues: numValues,
  );
  final out = Int32List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = values[i];
  }
  return out;
}

void _decodeInto(ByteReader reader, int bitWidth, Uint32List out) {
  final numValues = out.length;
  final valueByteWidth = (bitWidth + 7) >>> 3;
  var written = 0;
  while (written < numValues) {
    if (reader.isEOF) {
      throw ParquetEofException(
        'RLE/bit-packed stream ended after $written of $numValues values',
      );
    }
    final header = reader.readVarint32();
    // fastparquet appends zero padding to pages; a zero header cannot encode
    // any values, so it may only appear after the requested values are done.
    if (header == 0) {
      throw ParquetEofException(
        'RLE/bit-packed stream terminated after $written of $numValues values',
      );
    }

    if ((header & 1) == 1) {
      final runValues = (header >>> 1) * 8;
      if (runValues == 0) {
        throw ParquetFormatException('zero-length bit-packed run');
      }
      final remaining = numValues - written;
      final take = runValues < remaining ? runValues : remaining;
      final byteStart = reader.position;
      // A bit-packed run always contains whole groups of 8 values and is
      // therefore byte aligned: width * groups bytes.
      final runBytes = (bitWidth * runValues) >>> 3;
      if (byteStart + runBytes > reader.limit) {
        throw ParquetEofException(
          'bit-packed run needs $runBytes bytes at $byteStart but the stream '
          'ends at ${reader.limit}',
        );
      }
      unpackBitsLSB(
        reader.bytes.slice(byteStart, byteStart + runBytes),
        bitWidth: bitWidth,
        numValues: take,
        out: out,
        outOffset: written,
      );
      reader.position = byteStart + runBytes;
      written += take;
    } else {
      final runLength = header >>> 1;
      if (runLength == 0) {
        throw ParquetFormatException('zero-length RLE run');
      }
      var value = 0;
      if (bitWidth > 0) {
        if (reader.remaining < valueByteWidth) {
          throw ParquetEofException(
            'RLE run value needs $valueByteWidth bytes but only '
            '${reader.remaining} remain',
          );
        }
        for (var i = 0; i < valueByteWidth; i++) {
          value |= reader.readUint8() << (8 * i);
        }
      }
      final remaining = numValues - written;
      final take = runLength < remaining ? runLength : remaining;
      out.fillRange(written, written + take, value);
      written += take;
    }
  }
}
