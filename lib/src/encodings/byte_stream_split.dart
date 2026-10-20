/// Decoder for the Parquet BYTE_STREAM_SPLIT encoding.
///
/// The encoder transposes the bytes of every value: all first bytes are stored
/// first, then all second bytes, and so on. For a stride of `k` the byte at
/// `input[j * numValues + i]` belongs to byte `j` of value `i`.
///
/// The stride is 4 for FLOAT and INT32, 8 for DOUBLE and INT64, and the
/// schema's `type_length` for FIXED_LEN_BYTE_ARRAY. INT32 / INT64 support
/// follows the Apache Arrow extension used by pyarrow, which also applies
/// BYTE_STREAM_SPLIT to integer columns.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import '../util/int64.dart' show Int64Buffer;
import 'plain.dart' show ByteArrayData;

void _checkAvailable(Bytes data, int needed, String what) {
  if (data.length < needed) {
    throw ParquetEofException(
      '$what truncated: need $needed bytes, only ${data.length} available',
    );
  }
}

void _checkCount(int numValues) {
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
}

/// Decodes BYTE_STREAM_SPLIT FLOAT values (stride 4).
Float32List decodeByteStreamSplitFloat32(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 4;
  _checkAvailable(data, needed, 'BYTE_STREAM_SPLIT FLOAT data');
  final source = data.asUint8List();
  final bytes = Uint8List(needed);
  for (var j = 0; j < 4; j++) {
    final streamOffset = j * numValues;
    for (var i = 0; i < numValues; i++) {
      bytes[i * 4 + j] = source[streamOffset + i];
    }
  }
  final byteData = ByteData.sublistView(bytes);
  final out = Float32List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = byteData.getFloat32(i * 4, Endian.little);
  }
  return out;
}

/// Decodes BYTE_STREAM_SPLIT DOUBLE values (stride 8).
Float64List decodeByteStreamSplitFloat64(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 8;
  _checkAvailable(data, needed, 'BYTE_STREAM_SPLIT DOUBLE data');
  final source = data.asUint8List();
  final bytes = Uint8List(needed);
  for (var j = 0; j < 8; j++) {
    final streamOffset = j * numValues;
    for (var i = 0; i < numValues; i++) {
      bytes[i * 8 + j] = source[streamOffset + i];
    }
  }
  final byteData = ByteData.sublistView(bytes);
  final out = Float64List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = byteData.getFloat64(i * 8, Endian.little);
  }
  return out;
}

/// Decodes BYTE_STREAM_SPLIT INT32 values (stride 4).
Int32List decodeByteStreamSplitInt32(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 4;
  _checkAvailable(data, needed, 'BYTE_STREAM_SPLIT INT32 data');
  final source = data.asUint8List();
  final bytes = Uint8List(needed);
  for (var j = 0; j < 4; j++) {
    final streamOffset = j * numValues;
    for (var i = 0; i < numValues; i++) {
      bytes[i * 4 + j] = source[streamOffset + i];
    }
  }
  final byteData = ByteData.sublistView(bytes);
  final out = Int32List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = byteData.getInt32(i * 4, Endian.little);
  }
  return out;
}

/// Decodes BYTE_STREAM_SPLIT INT64 values (stride 8) into an exact
/// [Int64Buffer].
Int64Buffer decodeByteStreamSplitInt64(Bytes data, {required int numValues}) {
  _checkCount(numValues);
  final needed = numValues * 8;
  _checkAvailable(data, needed, 'BYTE_STREAM_SPLIT INT64 data');
  final source = data.asUint8List();
  final bytes = Uint8List(needed);
  for (var j = 0; j < 8; j++) {
    final streamOffset = j * numValues;
    for (var i = 0; i < numValues; i++) {
      bytes[i * 8 + j] = source[streamOffset + i];
    }
  }
  final byteData = ByteData.sublistView(bytes);
  final out = Int64Buffer(numValues);
  for (var i = 0; i < numValues; i++) {
    final lo = byteData.getUint32(i * 8, Endian.little);
    final hi = byteData.getUint32(i * 8 + 4, Endian.little);
    out.setLimbs(i, lo, hi);
  }
  return out;
}

/// Decodes BYTE_STREAM_SPLIT FIXED_LEN_BYTE_ARRAY values.
///
/// The result is a [ByteArrayData] with uniform offsets (`i * typeLength`).
ByteArrayData decodeByteStreamSplitFixedLenByteArray(
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
  _checkAvailable(data, needed, 'BYTE_STREAM_SPLIT FIXED_LEN_BYTE_ARRAY data');
  if (needed > 0xFFFFFFFF) {
    throw ParquetRangeException(
      'BYTE_STREAM_SPLIT FIXED_LEN_BYTE_ARRAY data exceeds 4 GiB',
    );
  }

  final source = data.asUint8List();
  final out = Uint8List(needed);
  for (var j = 0; j < typeLength; j++) {
    final streamOffset = j * numValues;
    for (var i = 0; i < numValues; i++) {
      out[i * typeLength + j] = source[streamOffset + i];
    }
  }
  final offsets = Uint32List(numValues + 1);
  for (var i = 0; i <= numValues; i++) {
    offsets[i] = i * typeLength;
  }
  return ByteArrayData(out, offsets);
}
