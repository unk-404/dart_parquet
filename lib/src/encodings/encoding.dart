/// Small dispatcher and enum helpers for the Parquet encodings.
///
/// The concrete decoders live in the sibling libraries and are what the column
/// reader calls for a given page. This library only centralises the
/// encoding/physical-type decisions so the reader does not have to repeat them.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../format/enums.dart' show Encoding, PhysicalType;
import '../util/errors.dart';
import 'byte_stream_split.dart'
    show
        decodeByteStreamSplitFixedLenByteArray,
        decodeByteStreamSplitFloat32,
        decodeByteStreamSplitFloat64,
        decodeByteStreamSplitInt32,
        decodeByteStreamSplitInt64;
import 'delta.dart'
    show
        decodeDeltaBinaryPackedInt32,
        decodeDeltaBinaryPackedInt64,
        decodeDeltaByteArray,
        decodeDeltaLengthByteArray;
import 'dictionary.dart' show decodeDictionaryIndices;
import 'plain.dart'
    show
        ByteArrayData,
        FixedLenData,
        decodePlainBoolean,
        decodePlainByteArray,
        decodePlainFixedLenByteArray,
        decodePlainFloat32,
        decodePlainFloat64,
        decodePlainInt32,
        decodePlainInt64,
        decodePlainInt96;

export 'plain.dart' show ByteArrayData, FixedLenData;

/// Whether [encoding] stores dictionary indices.
bool isDictionaryEncoding(Encoding encoding) =>
    encoding == Encoding.plainDictionary || encoding == Encoding.rleDictionary;

/// Whether [encoding] is a byte-array delta encoding.
bool isByteArrayDeltaEncoding(Encoding encoding) =>
    encoding == Encoding.deltaLengthByteArray ||
    encoding == Encoding.deltaByteArray;

/// Encodings whose value payload can be decoded by this package.
///
/// This is deliberately a set instead of an exhaustive switch so that adding a
/// new format enum value (for example ALP) does not break this dispatcher; new
/// encodings simply report as unsupported until a decoder is added.
const Set<Encoding> _supportedEncodings = <Encoding>{
  Encoding.plain,
  Encoding.plainDictionary,
  Encoding.rle,
  Encoding.bitPacked,
  Encoding.deltaBinaryPacked,
  Encoding.deltaLengthByteArray,
  Encoding.deltaByteArray,
  Encoding.rleDictionary,
  Encoding.byteStreamSplit,
  Encoding.alp,
};

/// Whether [encoding] is supported by the read-side decoders.
bool isSupportedEncoding(Encoding encoding) =>
    _supportedEncodings.contains(encoding);

/// Validates that [encoding] can decode values of [physicalType].
///
/// Throws a [ParquetUnsupportedException] for encoding/type combinations that
/// the Parquet format does not define (or that this package does not implement
/// yet, such as the deprecated `BIT_PACKED` value encoding).
void validateEncodingForPhysicalType(
  Encoding encoding,
  PhysicalType physicalType,
) {
  if (encoding == Encoding.plain) {
    // PLAIN covers every physical type.
    return;
  }
  if (isDictionaryEncoding(encoding)) {
    // Dictionary indices are only meaningful for non-boolean values.
    if (physicalType == PhysicalType.boolean ||
        physicalType == PhysicalType.int96) {
      throw ParquetUnsupportedException(
        'dictionary encoding is not supported for $physicalType',
      );
    }
    return;
  }
  if (encoding == Encoding.deltaBinaryPacked) {
    if (physicalType != PhysicalType.int32 &&
        physicalType != PhysicalType.int64) {
      throw ParquetUnsupportedException(
        'DELTA_BINARY_PACKED is only defined for INT32 and INT64, got '
        '$physicalType',
      );
    }
    return;
  }
  if (encoding == Encoding.deltaLengthByteArray ||
      encoding == Encoding.deltaByteArray) {
    if (physicalType != PhysicalType.byteArray &&
        physicalType != PhysicalType.fixedLenByteArray) {
      throw ParquetUnsupportedException(
        '$encoding is only defined for BYTE_ARRAY and '
        'FIXED_LEN_BYTE_ARRAY, got $physicalType',
      );
    }
    return;
  }
  if (encoding == Encoding.byteStreamSplit) {
    if (physicalType != PhysicalType.float &&
        physicalType != PhysicalType.double &&
        physicalType != PhysicalType.int32 &&
        physicalType != PhysicalType.int64 &&
        physicalType != PhysicalType.fixedLenByteArray) {
      throw ParquetUnsupportedException(
        'BYTE_STREAM_SPLIT is only supported for FLOAT, DOUBLE, INT32, '
        'INT64 and FIXED_LEN_BYTE_ARRAY, got $physicalType',
      );
    }
    return;
  }
  if (encoding == Encoding.alp) {
    if (physicalType != PhysicalType.float &&
        physicalType != PhysicalType.double) {
      throw ParquetUnsupportedException(
        'ALP is only defined for FLOAT and DOUBLE, got $physicalType',
      );
    }
    return;
  }
  if (encoding == Encoding.rle || encoding == Encoding.bitPacked) {
    throw ParquetUnsupportedException(
      '$encoding is a levels-only encoding and cannot decode values',
    );
  }
  // Future format enum values (for example ALP) reach this branch until a
  // decoder is added.
  throw ParquetUnsupportedException(
    'unsupported encoding $encoding for $physicalType',
  );
}

/// Decodes a PLAIN value payload for [physicalType].
///
/// Returns `Int32List`, `Int64Buffer`, `Float32List`, `Float64List`,
/// `Uint8List` (booleans), [ByteArrayData] or [FixedLenData] as appropriate.
Object decodePlainValues(
  Bytes data, {
  required PhysicalType physicalType,
  required int numValues,
  int? typeLength,
}) {
  switch (physicalType) {
    case PhysicalType.boolean:
      return decodePlainBoolean(data, numValues: numValues);
    case PhysicalType.int32:
      return decodePlainInt32(data, numValues: numValues);
    case PhysicalType.int64:
      return decodePlainInt64(data, numValues: numValues);
    case PhysicalType.int96:
      return decodePlainInt96(data, numValues: numValues);
    case PhysicalType.float:
      return decodePlainFloat32(data, numValues: numValues);
    case PhysicalType.double:
      return decodePlainFloat64(data, numValues: numValues);
    case PhysicalType.byteArray:
      return decodePlainByteArray(data, numValues: numValues);
    case PhysicalType.fixedLenByteArray:
      return decodePlainFixedLenByteArray(
        data,
        numValues: numValues,
        typeLength: _requireTypeLength(typeLength, physicalType),
      );
  }
}

/// Decodes a BYTE_STREAM_SPLIT value payload for [physicalType].
Object decodeByteStreamSplitValues(
  Bytes data, {
  required PhysicalType physicalType,
  required int numValues,
  int? typeLength,
}) {
  switch (physicalType) {
    case PhysicalType.int32:
      return decodeByteStreamSplitInt32(data, numValues: numValues);
    case PhysicalType.int64:
      return decodeByteStreamSplitInt64(data, numValues: numValues);
    case PhysicalType.float:
      return decodeByteStreamSplitFloat32(data, numValues: numValues);
    case PhysicalType.double:
      return decodeByteStreamSplitFloat64(data, numValues: numValues);
    case PhysicalType.fixedLenByteArray:
      return decodeByteStreamSplitFixedLenByteArray(
        data,
        numValues: numValues,
        typeLength: _requireTypeLength(typeLength, physicalType),
      );
    default:
      throw ParquetUnsupportedException(
        'BYTE_STREAM_SPLIT is not defined for $physicalType',
      );
  }
}

/// Decodes a value payload for the DELTA_BINARY_PACKED encoding.
Object decodeDeltaBinaryPackedValues(
  Bytes data, {
  required PhysicalType physicalType,
  int? expectedCount,
}) {
  switch (physicalType) {
    case PhysicalType.int32:
      return decodeDeltaBinaryPackedInt32(data, expectedCount: expectedCount);
    case PhysicalType.int64:
      return decodeDeltaBinaryPackedInt64(data, expectedCount: expectedCount);
    default:
      throw ParquetUnsupportedException(
        'DELTA_BINARY_PACKED is not defined for $physicalType',
      );
  }
}

/// Decodes a DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY payload.
ByteArrayData decodeDeltaByteArrayValues(
  Bytes data, {
  required Encoding encoding,
  int? expectedCount,
}) {
  switch (encoding) {
    case Encoding.deltaLengthByteArray:
      return decodeDeltaLengthByteArray(data, expectedCount: expectedCount);
    case Encoding.deltaByteArray:
      return decodeDeltaByteArray(data, expectedCount: expectedCount);
    default:
      throw ParquetUnsupportedException(
        '$encoding is not a byte-array delta encoding',
      );
  }
}

int _requireTypeLength(int? typeLength, PhysicalType physicalType) {
  if (typeLength == null) {
    throw ParquetFormatException(
      '$physicalType requires a type_length in the schema',
    );
  }
  if (typeLength <= 0) {
    throw ParquetFormatException(
      'invalid type_length $typeLength for $physicalType',
    );
  }
  return typeLength;
}

/// Convenience: decodes dictionary indices for a dictionary-encoded page.
Int32List decodeDictionaryIndexValues(Bytes data, {required int numValues}) {
  return decodeDictionaryIndices(data, numValues: numValues);
}
