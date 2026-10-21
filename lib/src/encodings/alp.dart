/// ALP (Adaptive Lossless floating-Point) encoding, Parquet encoding 10.
///
/// This is an implementation of the page layout and the normative decoding
/// procedure specified in `AlpEncoding.md` of `apache/parquet-format`
/// (retrieved from the master branch on 2026-10-21):
///
/// * page header: 7 bytes - `compression_mode` (0 = ALP), `integer_encoding`
///   (0 = FOR + bit-packing), `log_vector_size` (3..15) and an int32
///   `num_elements`;
/// * offset array: one little-endian uint32 per vector, relative to the start
///   of the offset array itself;
/// * vectors: `[AlpInfo][ForInfo][PackedValues][ExceptionPositions]
///   [ExceptionValues]`, each vector holding `min(vector_size, remaining)`
///   values.
///
/// The decimal decode is normative and uses two separate multiplications with
/// the correctly rounded IEEE 754 constants `10^f` and `10^-e`:
///
/// ```text
/// value = (encoded * 10^factor) * 10^-exponent
/// ```
///
/// For FLOAT all arithmetic in that formula is binary32; for DOUBLE it is
/// binary64. The power-of-ten tables below are written as decimal literals, so
/// they are the correctly rounded IEEE values required by the format (the
/// FLOAT tables are additionally narrowed to binary32 once).
///
/// The encode direction (parameter selection and the "fast round" technique)
/// is informative in the specification. This encoder follows the reference
/// strategy of sampling a few `(exponent, factor)` candidates per page and
/// always verifies every value against the normative decode, storing any value
/// that does not round-trip bit-exactly in the exception section. The resulting
/// page is therefore always spec-conforming; it is not required to be
/// byte-identical to another writer's output.
///
/// Web-safety: FLOAT vectors use 32-bit integer arithmetic only, while the
/// 64-bit FOR arithmetic of DOUBLE vectors uses [BigInt]; no `Int64List` or
/// `ByteData.getUint64` is used. NaN payload bits are preserved whenever the
/// platform's float containers preserve them (the VM does; some JS engines
/// canonicalize NaN when a value passes through a JS number).
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../format/enums.dart' show PhysicalType;
import '../util/errors.dart';
import 'bitpacking.dart' show readBitsLSB, readBitsLSBLimbs;

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// Thrift value of `Encoding.ALP` in the Parquet format.
const int alpEncodingValue = 10;

/// Size of the fixed ALP page header in bytes.
const int alpHeaderSize = 7;

/// Number of bytes in one `AlpInfo` structure.
const int alpInfoSize = 4;

/// Spec default `log2(vector_size)`: 1024 values per vector.
const int alpDefaultLogVectorSize = 10;

/// Inclusive allowed range for `log2(vector_size)`.
const int alpMinLogVectorSize = 3;
const int alpMaxLogVectorSize = 15;

/// Highest exponent allowed by the specification for each physical type.
const int alpMaxExponentFloat = 10;
const int alpMaxExponentDouble = 18;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Decodes an ALP-encoded value payload.
///
/// [type] must be [PhysicalType.float] or [PhysicalType.double]; the returned
/// object is then a [Float32List] or a [Float64List] respectively.
///
/// [numValues] is the number of non-null values expected by the caller (the
/// page's definition-level count). It is treated as an upper bound: the ALP
/// header carries the authoritative count, which must not exceed it. The
/// returned list therefore has `header.num_elements` entries, matching the
/// reference implementation's semantics.
Object decodeAlp(
  Bytes data, {
  required PhysicalType type,
  required int numValues,
}) {
  switch (type) {
    case PhysicalType.float:
      return decodeAlpFloat32(data, numValues: numValues);
    case PhysicalType.double:
      return decodeAlpFloat64(data, numValues: numValues);
    default:
      throw ParquetUnsupportedException(
        'ALP is only defined for FLOAT and DOUBLE, got $type',
      );
  }
}

/// Decodes an ALP FLOAT payload into a [Float32List].
Float32List decodeAlpFloat32(Bytes data, {required int numValues}) =>
    _decodeFloat32Page(data, numValues);

/// Decodes an ALP DOUBLE payload into a [Float64List].
Float64List decodeAlpFloat64(Bytes data, {required int numValues}) =>
    _decodeFloat64Page(data, numValues);

/// Encodes [values] as one ALP page for [type].
///
/// [type] must be [PhysicalType.float] or [PhysicalType.double]. FLOAT input is
/// rounded to binary32 before encoding, because that is the precision the page
/// stores and reproduces.
///
/// [logVectorSize] is `log2(vector_size)` and must be in
/// `[alpMinLogVectorSize, alpMaxLogVectorSize]`; the spec-recommended default
/// is [alpDefaultLogVectorSize] (1024 values per vector).
Uint8List encodeAlp(
  List<num> values, {
  required PhysicalType type,
  int logVectorSize = alpDefaultLogVectorSize,
}) {
  if (type != PhysicalType.float && type != PhysicalType.double) {
    throw ParquetUnsupportedException(
      'ALP is only defined for FLOAT and DOUBLE, got $type',
    );
  }
  if (logVectorSize < alpMinLogVectorSize ||
      logVectorSize > alpMaxLogVectorSize) {
    throw ArgumentError.value(
      logVectorSize,
      'logVectorSize',
      'must be in [$alpMinLogVectorSize, $alpMaxLogVectorSize]',
    );
  }
  if (values.length > 0x7FFFFFFF) {
    throw ParquetRangeException(
      'ALP pages cannot store more than ${0x7FFFFFFF} values',
    );
  }

  final isFloat = type == PhysicalType.float;
  final targets = <double>[
    for (final value in values)
      isFloat ? _toFloat32(value.toDouble()) : value.toDouble(),
  ];

  final vectorSize = 1 << logVectorSize;
  final numVectors = (targets.length + vectorSize - 1) >> logVectorSize;
  final preset = _buildPreset(targets, type, vectorSize);

  final page = <int>[
    0, // compression_mode = ALP
    0, // integer_encoding = FOR + bit-packing
    logVectorSize,
    targets.length & 0xFF,
    (targets.length >> 8) & 0xFF,
    (targets.length >> 16) & 0xFF,
    (targets.length >> 24) & 0xFF,
  ];
  final offsetsStart = page.length;
  page.addAll(List<int>.filled(numVectors * 4, 0));

  var vectorIndex = 0;
  for (var start = 0; start < targets.length; start += vectorSize) {
    final end = start + vectorSize < targets.length
        ? start + vectorSize
        : targets.length;
    final vector = targets.sublist(start, end);

    final plan = _selectPlan(vector, preset, type);
    final offset = page.length - alpHeaderSize;
    if (offset > 0xFFFFFFFF) {
      throw ParquetRangeException(
        'ALP page body exceeds the uint32 offset range',
      );
    }
    _writeUint32At(page, offsetsStart + vectorIndex * 4, offset);
    page.addAll(_serializePlan(plan));
    vectorIndex++;
  }

  return Uint8List.fromList(page);
}

// ---------------------------------------------------------------------------
// Correctly rounded power-of-ten tables
// ---------------------------------------------------------------------------

const double _f32Magic = 12582912.0; // 2^22 + 2^23
const double _f64Magic = 6755399441055744.0; // 2^51 + 2^52

/// `2147483500.0f32` from the reference implementation (2147483520.0).
final double _f32UpperLimit = _toFloat32(2147483500.0);
final double _f32LowerLimit = -_f32UpperLimit;

/// `9223372036854775000.0` (the reference implementation's limit).
const double _f64UpperLimit = 9223372036854775000.0;
const double _f64LowerLimit = -9223372036854775000.0;

final List<double> _pow10Float32 = List<double>.unmodifiable(
  <double>[
    1e0,
    1e1,
    1e2,
    1e3,
    1e4,
    1e5,
    1e6,
    1e7,
    1e8,
    1e9,
    1e10,
  ].map(_toFloat32),
);

final List<double> _negPow10Float32 = List<double>.unmodifiable(
  <double>[
    1e0,
    1e-1,
    1e-2,
    1e-3,
    1e-4,
    1e-5,
    1e-6,
    1e-7,
    1e-8,
    1e-9,
    1e-10,
  ].map(_toFloat32),
);

/// Correctly rounded binary64 values of `10^0 .. 10^18`.
const List<double> _pow10Float64 = <double>[
  1e0,
  1e1,
  1e2,
  1e3,
  1e4,
  1e5,
  1e6,
  1e7,
  1e8,
  1e9,
  1e10,
  1e11,
  1e12,
  1e13,
  1e14,
  1e15,
  1e16,
  1e17,
  1e18,
];

/// Correctly rounded binary64 values of `10^0, 10^-1 .. 10^-18`.
const List<double> _negPow10Float64 = <double>[
  1e0,
  1e-1,
  1e-2,
  1e-3,
  1e-4,
  1e-5,
  1e-6,
  1e-7,
  1e-8,
  1e-9,
  1e-10,
  1e-11,
  1e-12,
  1e-13,
  1e-14,
  1e-15,
  1e-16,
  1e-17,
  1e-18,
];

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

final BigInt _u64Mask = (BigInt.one << 64) - BigInt.one;
final BigInt _i64SignBit = BigInt.one << 63;
final BigInt _u64Modulus = BigInt.one << 64;

final class _PageHeader {
  _PageHeader(this.logVectorSize, this.numElements);

  final int logVectorSize;
  final int numElements;

  int get vectorSize => 1 << logVectorSize;

  int get numVectors => (numElements + vectorSize - 1) >> logVectorSize;
}

_PageHeader _parsePageHeader(Bytes data, int numValues) {
  if (data.length < alpHeaderSize) {
    throw ParquetEofException(
      'ALP page header truncated: need $alpHeaderSize bytes, only '
      '${data.length} available',
    );
  }
  final compressionMode = data.getUint8Unchecked(0);
  if (compressionMode != 0) {
    throw ParquetFormatException(
      'unsupported ALP compression mode $compressionMode',
    );
  }
  final integerEncoding = data.getUint8Unchecked(1);
  if (integerEncoding != 0) {
    throw ParquetFormatException(
      'unsupported ALP integer encoding $integerEncoding',
    );
  }
  final logVectorSize = data.getUint8Unchecked(2);
  if (logVectorSize < alpMinLogVectorSize ||
      logVectorSize > alpMaxLogVectorSize) {
    throw ParquetFormatException(
      'invalid ALP log_vector_size $logVectorSize '
      '(expected $alpMinLogVectorSize..$alpMaxLogVectorSize)',
    );
  }
  final rawCount = data.getUint32(3, endian: Endian.little);
  if (rawCount > 0x7FFFFFFF) {
    throw ParquetFormatException(
      'ALP num_elements $rawCount must be a non-negative int32',
    );
  }
  if (rawCount > numValues) {
    throw ParquetFormatException(
      'ALP page stores $rawCount values but the page declares $numValues',
    );
  }
  return _PageHeader(logVectorSize, rawCount);
}

int _asInt32(int unsigned) =>
    unsigned >= 0x80000000 ? unsigned - 0x100000000 : unsigned;

BigInt _asInt64(BigInt unsigned) =>
    unsigned >= _i64SignBit ? unsigned - _u64Modulus : unsigned;

BigInt _readUint64Bits(Bytes data, int offset) {
  final lo = data.getUint32(offset, endian: Endian.little);
  final hi = data.getUint32(offset + 4, endian: Endian.little);
  return (BigInt.from(hi) << 32) | BigInt.from(lo);
}

BigInt _float64Bits(double value) {
  final byteData = ByteData(8)..setFloat64(0, value, Endian.little);
  final lo = byteData.getUint32(0, Endian.little);
  final hi = byteData.getUint32(4, Endian.little);
  return (BigInt.from(hi) << 32) | BigInt.from(lo);
}

int _float32Bits(double value) {
  final byteData = ByteData(4)..setFloat32(0, value, Endian.little);
  return byteData.getUint32(0, Endian.little);
}

Float32List _decodeFloat32Page(Bytes data, int numValues) {
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  // A page with no non-null values may have no encoded value payload at all.
  if (numValues == 0 && data.isEmpty) return Float32List(0);
  final header = _parsePageHeader(data, numValues);
  final count = header.numElements;
  final out = Float32List(count);
  if (count == 0) return out;

  final numVectors = header.numVectors;
  final offsetsBytes = numVectors * 4;
  if (data.length - alpHeaderSize < offsetsBytes) {
    throw ParquetEofException(
      'ALP page needs $offsetsBytes bytes for its offsets array but only '
      '${data.length - alpHeaderSize} are available',
    );
  }

  var expectedOffset = offsetsBytes;
  var outIndex = 0;
  for (var vectorIndex = 0; vectorIndex < numVectors; vectorIndex++) {
    final offset = data.getUint32(
      alpHeaderSize + vectorIndex * 4,
      endian: Endian.little,
    );
    if (offset != expectedOffset) {
      throw ParquetFormatException(
        'ALP vector $vectorIndex starts at offset $offset, expected '
        '$expectedOffset',
      );
    }
    final nextOffset = vectorIndex + 1 < numVectors
        ? data.getUint32(
            alpHeaderSize + (vectorIndex + 1) * 4,
            endian: Endian.little,
          )
        : data.length - alpHeaderSize;
    if (nextOffset < offset || nextOffset > data.length - alpHeaderSize) {
      throw ParquetFormatException(
        'ALP vector $vectorIndex offset $nextOffset is out of bounds',
      );
    }
    final vectorStart = alpHeaderSize + offset;
    final vectorEnd = alpHeaderSize + nextOffset;
    final vectorCount = _vectorLength(header, vectorIndex);
    _decodeFloat32Vector(
      data,
      vectorStart,
      vectorEnd,
      vectorCount,
      out,
      outIndex,
    );
    outIndex += vectorCount;
    expectedOffset = nextOffset;
  }
  return out;
}

Float64List _decodeFloat64Page(Bytes data, int numValues) {
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  // A page with no non-null values may have no encoded value payload at all.
  if (numValues == 0 && data.isEmpty) return Float64List(0);
  final header = _parsePageHeader(data, numValues);
  final count = header.numElements;
  final out = Float64List(count);
  if (count == 0) return out;

  final numVectors = header.numVectors;
  final offsetsBytes = numVectors * 4;
  if (data.length - alpHeaderSize < offsetsBytes) {
    throw ParquetEofException(
      'ALP page needs $offsetsBytes bytes for its offsets array but only '
      '${data.length - alpHeaderSize} are available',
    );
  }

  var expectedOffset = offsetsBytes;
  var outIndex = 0;
  for (var vectorIndex = 0; vectorIndex < numVectors; vectorIndex++) {
    final offset = data.getUint32(
      alpHeaderSize + vectorIndex * 4,
      endian: Endian.little,
    );
    if (offset != expectedOffset) {
      throw ParquetFormatException(
        'ALP vector $vectorIndex starts at offset $offset, expected '
        '$expectedOffset',
      );
    }
    final nextOffset = vectorIndex + 1 < numVectors
        ? data.getUint32(
            alpHeaderSize + (vectorIndex + 1) * 4,
            endian: Endian.little,
          )
        : data.length - alpHeaderSize;
    if (nextOffset < offset || nextOffset > data.length - alpHeaderSize) {
      throw ParquetFormatException(
        'ALP vector $vectorIndex offset $nextOffset is out of bounds',
      );
    }
    final vectorStart = alpHeaderSize + offset;
    final vectorEnd = alpHeaderSize + nextOffset;
    final vectorCount = _vectorLength(header, vectorIndex);
    _decodeFloat64Vector(
      data,
      vectorStart,
      vectorEnd,
      vectorCount,
      out,
      outIndex,
    );
    outIndex += vectorCount;
    expectedOffset = nextOffset;
  }
  return out;
}

int _vectorLength(_PageHeader header, int vectorIndex) {
  final remaining = header.numElements - vectorIndex * header.vectorSize;
  return remaining < header.vectorSize ? remaining : header.vectorSize;
}

void _validateVectorHeader(
  int exponent,
  int factor,
  int numExceptions,
  int vectorCount,
  int maxExponent,
  int maxBitWidth,
  int bitWidth,
) {
  if (exponent > maxExponent) {
    throw ParquetFormatException('ALP exponent $exponent exceeds $maxExponent');
  }
  if (factor > exponent) {
    throw ParquetFormatException(
      'ALP factor $factor exceeds exponent $exponent',
    );
  }
  if (numExceptions > vectorCount) {
    throw ParquetFormatException(
      'ALP vector has $numExceptions exceptions but only $vectorCount values',
    );
  }
  if (bitWidth > maxBitWidth) {
    throw ParquetFormatException(
      'ALP bit width $bitWidth exceeds $maxBitWidth',
    );
  }
}

void _decodeFloat32Vector(
  Bytes data,
  int start,
  int end,
  int count,
  Float32List out,
  int outOffset,
) {
  const metadataSize = alpInfoSize + 5;
  if (end - start < metadataSize) {
    throw ParquetEofException(
      'ALP vector metadata truncated: need $metadataSize bytes, '
      '${end - start} available',
    );
  }
  final exponent = data.getUint8Unchecked(start);
  final factor = data.getUint8Unchecked(start + 1);
  final numExceptions = data.getUint16(start + 2, endian: Endian.little);
  final frame = data.getUint32(start + 4, endian: Endian.little);
  final bitWidth = data.getUint8Unchecked(start + 8);
  _validateVectorHeader(
    exponent,
    factor,
    numExceptions,
    count,
    alpMaxExponentFloat,
    32,
    bitWidth,
  );

  final packedSize = (count * bitWidth + 7) >> 3;
  final positionsSize = numExceptions * 2;
  final valuesSize = numExceptions * 4;
  final expectedSize = metadataSize + packedSize + positionsSize + valuesSize;
  if (end - start < expectedSize) {
    throw ParquetEofException(
      'ALP vector truncated: need $expectedSize bytes, ${end - start} '
      'available',
    );
  }
  if (end - start > expectedSize) {
    throw ParquetFormatException(
      'ALP vector contains ${end - start - expectedSize} unclaimed bytes',
    );
  }

  final packedStart = start + metadataSize;
  final positionsStart = packedStart + packedSize;
  final valuesStart = positionsStart + positionsSize;
  final powFactor = _pow10Float32[factor];
  final negPowExponent = _negPow10Float32[exponent];

  if (bitWidth == 0) {
    final value = _decodeFloat32(_asInt32(frame), powFactor, negPowExponent);
    out.fillRange(outOffset, outOffset + count, value);
  } else {
    final baseBit = packedStart * 8;
    for (var i = 0; i < count; i++) {
      final delta = readBitsLSB(data, baseBit + i * bitWidth, bitWidth);
      final encoded = (frame + delta) & 0xFFFFFFFF;
      out[outOffset + i] = _decodeFloat32(
        _asInt32(encoded),
        powFactor,
        negPowExponent,
      );
    }
  }

  if (numExceptions > 0) {
    final byteData = data.asByteData();
    for (var i = 0; i < numExceptions; i++) {
      final position = data.getUint16(
        positionsStart + i * 2,
        endian: Endian.little,
      );
      if (position >= count) {
        throw ParquetFormatException(
          'ALP exception position $position exceeds vector length $count',
        );
      }
      out[outOffset + position] = byteData.getFloat32(
        valuesStart + i * 4,
        Endian.little,
      );
    }
  }
}

void _decodeFloat64Vector(
  Bytes data,
  int start,
  int end,
  int count,
  Float64List out,
  int outOffset,
) {
  const metadataSize = alpInfoSize + 9;
  if (end - start < metadataSize) {
    throw ParquetEofException(
      'ALP vector metadata truncated: need $metadataSize bytes, '
      '${end - start} available',
    );
  }
  final exponent = data.getUint8Unchecked(start);
  final factor = data.getUint8Unchecked(start + 1);
  final numExceptions = data.getUint16(start + 2, endian: Endian.little);
  final frame = _readUint64Bits(data, start + 4);
  final bitWidth = data.getUint8Unchecked(start + 12);
  _validateVectorHeader(
    exponent,
    factor,
    numExceptions,
    count,
    alpMaxExponentDouble,
    64,
    bitWidth,
  );

  final packedSize = (count * bitWidth + 7) >> 3;
  final positionsSize = numExceptions * 2;
  final valuesSize = numExceptions * 8;
  final expectedSize = metadataSize + packedSize + positionsSize + valuesSize;
  if (end - start < expectedSize) {
    throw ParquetEofException(
      'ALP vector truncated: need $expectedSize bytes, ${end - start} '
      'available',
    );
  }
  if (end - start > expectedSize) {
    throw ParquetFormatException(
      'ALP vector contains ${end - start - expectedSize} unclaimed bytes',
    );
  }

  final packedStart = start + metadataSize;
  final positionsStart = packedStart + packedSize;
  final valuesStart = positionsStart + positionsSize;
  final powFactor = _pow10Float64[factor];
  final negPowExponent = _negPow10Float64[exponent];
  final frameSigned = _asInt64(frame);

  if (bitWidth == 0) {
    final value = _decodeFloat64(frameSigned, powFactor, negPowExponent);
    out.fillRange(outOffset, outOffset + count, value);
  } else {
    final baseBit = packedStart * 8;
    for (var i = 0; i < count; i++) {
      final (lo, hi) = readBitsLSBLimbs(data, baseBit + i * bitWidth, bitWidth);
      final delta = (BigInt.from(hi) << 32) | BigInt.from(lo);
      final encodedBits = (frame + delta) & _u64Mask;
      out[outOffset + i] = _decodeFloat64(
        _asInt64(encodedBits),
        powFactor,
        negPowExponent,
      );
    }
  }

  if (numExceptions > 0) {
    final byteData = data.asByteData();
    for (var i = 0; i < numExceptions; i++) {
      final position = data.getUint16(
        positionsStart + i * 2,
        endian: Endian.little,
      );
      if (position >= count) {
        throw ParquetFormatException(
          'ALP exception position $position exceeds vector length $count',
        );
      }
      out[outOffset + position] = byteData.getFloat64(
        valuesStart + i * 8,
        Endian.little,
      );
    }
  }
}

/// Normative ALP decimal decode for binary32.
double _decodeFloat32(int encoded, double powFactor, double negPowExponent) {
  final exact = _toFloat32(encoded.toDouble());
  return _toFloat32(_toFloat32(exact * powFactor) * negPowExponent);
}

/// Normative ALP decimal decode for binary64.
double _decodeFloat64(BigInt encoded, double powFactor, double negPowExponent) {
  final exact = encoded.toDouble();
  return (exact * powFactor) * negPowExponent;
}

// ---------------------------------------------------------------------------
// Encoding: parameter selection
// ---------------------------------------------------------------------------

final class _Params {
  const _Params(this.exponent, this.factor);

  final int exponent;
  final int factor;

  @override
  bool operator ==(Object other) =>
      other is _Params && other.exponent == exponent && other.factor == factor;

  @override
  int get hashCode => Object.hash(exponent, factor);

  @override
  String toString() => '(_Params e=$exponent f=$factor)';
}

final List<_Params> _floatCandidates = List<_Params>.unmodifiable(<_Params>[
  for (var exponent = 0; exponent <= alpMaxExponentFloat; exponent++)
    for (var factor = 0; factor <= exponent; factor++)
      _Params(exponent, factor),
]);

final List<_Params> _doubleCandidates = List<_Params>.unmodifiable(<_Params>[
  for (var exponent = 0; exponent <= alpMaxExponentDouble; exponent++)
    for (var factor = 0; factor <= exponent; factor++)
      _Params(exponent, factor),
]);

List<_Params> _candidatesFor(PhysicalType type) =>
    type == PhysicalType.float ? _floatCandidates : _doubleCandidates;

/// Builds the per-page set of up to five candidate `(exponent, factor)` pairs.
///
/// Up to eight vectors are sampled (256 evenly spaced values each) and every
/// valid pair is scored with [_estimateBits]. The most frequent winners are
/// carried forward, so per-vector selection only evaluates a handful of
/// candidates - the same two-level sampling strategy used by the reference
/// implementation.
List<_Params> _buildPreset(
  List<double> values,
  PhysicalType type,
  int vectorSize,
) {
  if (values.isEmpty) return const <_Params>[_Params(0, 0)];

  final candidates = _candidatesFor(type);
  final numVectors = (values.length + vectorSize - 1) ~/ vectorSize;
  final vectorStride = ((numVectors + 7) ~/ 8) < 1
      ? 1
      : ((numVectors + 7) ~/ 8);

  final tally = <_Params, int>{};
  final sample = <double>[];
  for (
    var vectorIndex = 0;
    vectorIndex < numVectors;
    vectorIndex += vectorStride
  ) {
    final start = vectorIndex * vectorSize;
    final end = start + vectorSize < values.length
        ? start + vectorSize
        : values.length;
    _sampleVector(values, start, end, sample);

    _Params? best;
    int? bestBits;
    for (final params in candidates) {
      final bits = _estimateBits(sample, params, type, penalize: true);
      if (bits == null) continue;
      if (best == null || bits < bestBits!) {
        best = params;
        bestBits = bits;
      }
    }
    if (best != null) {
      tally.update(best, (count) => count + 1, ifAbsent: () => 1);
    }
  }

  if (tally.isEmpty) return const <_Params>[_Params(0, 0)];

  final ranked = tally.entries.toList()
    ..sort((a, b) {
      final byCount = b.value.compareTo(a.value);
      if (byCount != 0) return byCount;
      final byExponent = b.key.exponent.compareTo(a.key.exponent);
      if (byExponent != 0) return byExponent;
      return b.key.factor.compareTo(a.key.factor);
    });
  return <_Params>[for (final entry in ranked.take(5)) entry.key];
}

void _sampleVector(List<double> values, int start, int end, List<double> out) {
  out.clear();
  final length = end - start;
  if (length == 0) return;
  final stride = (length + 255) ~/ 256;
  for (var i = start; i < end; i += stride) {
    out.add(values[i]);
  }
}

/// Estimated size in bits of encoding [sample] with [params].
///
/// Returns `null` when [penalize] is set and fewer than two values round-trip:
/// such a candidate would turn almost the whole vector into exceptions.
int? _estimateBits(
  List<double> sample,
  _Params params,
  PhysicalType type, {
  required bool penalize,
}) {
  var numExceptions = 0;
  BigInt? min;
  BigInt? max;
  for (final value in sample) {
    final encoded = _encodeValue(value, params, type);
    if (encoded == null) {
      numExceptions++;
      continue;
    }
    if (min == null || encoded < min) min = encoded;
    if (max == null || encoded > max) max = encoded;
  }
  final numValues = sample.length;
  final numEncoded = numValues - numExceptions;
  if (penalize && numEncoded < 2) return null;
  final exceptionBits = (type == PhysicalType.float ? 32 : 64) + 16;
  if (min == null) return numValues * exceptionBits;
  final bitWidth = _rangeWidth(min, max!, type);
  return numValues * bitWidth + numExceptions * exceptionBits;
}

/// Bits required for the unsigned FOR range between [min] and [max].
int _rangeWidth(BigInt min, BigInt max, PhysicalType type) {
  if (type == PhysicalType.float) {
    final minBits = min.toInt() & 0xFFFFFFFF;
    final maxBits = max.toInt() & 0xFFFFFFFF;
    final range = (maxBits - minBits) & 0xFFFFFFFF;
    return _bitsNeededUint32(range);
  }
  final range = ((max & _u64Mask) - (min & _u64Mask)) & _u64Mask;
  return range.bitLength;
}

int _bitsNeededUint32(int value) {
  if (value == 0) return 0;
  var bits = 0;
  var remaining = value;
  while (remaining != 0) {
    bits++;
    remaining >>>= 1;
  }
  return bits;
}

// ---------------------------------------------------------------------------
// Encoding: value encoding
// ---------------------------------------------------------------------------

/// Applies the informative ALP encode formula and verifies it against the
/// normative decode. Returns the signed encoded integer, or `null` when the
/// value must be stored as an exception.
BigInt? _encodeValue(double value, _Params params, PhysicalType type) =>
    type == PhysicalType.float
    ? _encodeFloat32Value(value, params)
    : _encodeFloat64Value(value, params);

BigInt? _encodeFloat32Value(double value, _Params params) {
  final scaled = _toFloat32(
    _toFloat32(value * _pow10Float32[params.exponent]) *
        _negPow10Float32[params.factor],
  );
  if (scaled.isNaN) return null;
  if (scaled < _f32LowerLimit || scaled > _f32UpperLimit) return null;
  if (scaled == 0.0 && scaled.isNegative) return null;

  final rounded = _toFloat32(_toFloat32(scaled + _f32Magic) - _f32Magic);
  if (!rounded.isFinite) return null;
  final encoded = rounded.toInt();
  if (encoded < -0x80000000 || encoded > 0x7FFFFFFF) return null;

  final decoded = _decodeFloat32(
    encoded,
    _pow10Float32[params.factor],
    _negPow10Float32[params.exponent],
  );
  if (_float32Bits(decoded) != _float32Bits(value)) return null;
  return BigInt.from(encoded);
}

BigInt? _encodeFloat64Value(double value, _Params params) {
  final scaled =
      (value * _pow10Float64[params.exponent]) *
      _negPow10Float64[params.factor];
  if (scaled.isNaN) return null;
  if (scaled < _f64LowerLimit || scaled > _f64UpperLimit) return null;
  if (scaled == 0.0 && scaled.isNegative) return null;

  final rounded = (scaled + _f64Magic) - _f64Magic;
  if (!rounded.isFinite) return null;
  if (rounded >= 9223372036854775808.0 || rounded < -9223372036854775808.0) {
    return null;
  }
  final encodedBig = BigInt.from(rounded.truncate());
  if (encodedBig < _i64Min || encodedBig > _i64Max) return null;

  final decoded = _decodeFloat64(
    encodedBig,
    _pow10Float64[params.factor],
    _negPow10Float64[params.exponent],
  );
  if (_float64Bits(decoded) != _float64Bits(value)) return null;
  return encodedBig;
}

final BigInt _i64Min = -_i64SignBit;
final BigInt _i64Max = _i64SignBit - BigInt.one;

// ---------------------------------------------------------------------------
// Encoding: vector plans and serialization
// ---------------------------------------------------------------------------

final class _VectorPlan {
  _VectorPlan({
    required this.type,
    required this.exponent,
    required this.factor,
    required this.frame,
    required this.bitWidth,
    required this.deltas,
    required this.exceptionPositions,
    required this.exceptionValues,
  });

  final PhysicalType type;
  final int exponent;
  final int factor;
  final BigInt frame;
  final int bitWidth;
  final List<BigInt> deltas;
  final List<int> exceptionPositions;
  final List<double> exceptionValues;

  int get storedSize {
    final forInfoSize = type == PhysicalType.float ? 5 : 9;
    final valueSize = type == PhysicalType.float ? 4 : 8;
    return alpInfoSize +
        forInfoSize +
        ((deltas.length * bitWidth + 7) >> 3) +
        exceptionPositions.length * (2 + valueSize);
  }
}

_VectorPlan _selectPlan(
  List<double> values,
  List<_Params> preset,
  PhysicalType type,
) {
  _VectorPlan? best;
  var bestSize = 0;
  for (final params in preset) {
    final plan = _planVector(values, params, type);
    final size = plan.storedSize;
    if (best == null ||
        size < bestSize ||
        (size == bestSize &&
            plan.exceptionPositions.length < best.exceptionPositions.length)) {
      best = plan;
      bestSize = size;
    }
  }
  return best!;
}

_VectorPlan _planVector(
  List<double> values,
  _Params params,
  PhysicalType type,
) {
  final count = values.length;
  final encoded = List<BigInt?>.filled(count, null);
  final exceptionPositions = <int>[];
  for (var i = 0; i < count; i++) {
    final value = _encodeValue(values[i], params, type);
    if (value == null) {
      exceptionPositions.add(i);
    } else {
      encoded[i] = value;
    }
  }

  // Exceptions are replaced by the first successfully encoded value (or zero
  // when every value is an exception) so they do not widen the FOR range.
  var placeholder = BigInt.zero;
  for (var i = 0; i < count; i++) {
    final value = encoded[i];
    if (value != null) {
      placeholder = value;
      break;
    }
  }
  for (final position in exceptionPositions) {
    encoded[position] = placeholder;
  }

  var frame = encoded[0]!;
  var maximum = frame;
  for (var i = 1; i < count; i++) {
    final value = encoded[i]!;
    if (value < frame) frame = value;
    if (value > maximum) maximum = value;
  }

  final int bitWidth;
  final List<BigInt> deltas;
  if (type == PhysicalType.float) {
    final frameBits = frame.toInt() & 0xFFFFFFFF;
    final maximumBits = maximum.toInt() & 0xFFFFFFFF;
    bitWidth = _bitsNeededUint32((maximumBits - frameBits) & 0xFFFFFFFF);
    deltas = <BigInt>[
      for (final value in encoded)
        BigInt.from(((value!.toInt() & 0xFFFFFFFF) - frameBits) & 0xFFFFFFFF),
    ];
  } else {
    final frameBits = frame & _u64Mask;
    final maximumBits = maximum & _u64Mask;
    bitWidth = ((maximumBits - frameBits) & _u64Mask).bitLength;
    deltas = <BigInt>[for (final value in encoded) (value! - frame) & _u64Mask];
  }

  return _VectorPlan(
    type: type,
    exponent: params.exponent,
    factor: params.factor,
    frame: frame,
    bitWidth: bitWidth,
    deltas: deltas,
    exceptionPositions: List<int>.unmodifiable(exceptionPositions),
    exceptionValues: List<double>.unmodifiable(<double>[
      for (final position in exceptionPositions) values[position],
    ]),
  );
}

List<int> _serializePlan(_VectorPlan plan) {
  final out = <int>[];
  out
    ..add(plan.exponent)
    ..add(plan.factor);
  _writeUint16(out, plan.exceptionPositions.length);
  if (plan.type == PhysicalType.float) {
    _writeUint32(out, plan.frame.toInt() & 0xFFFFFFFF);
  } else {
    _writeUint64(out, plan.frame & _u64Mask);
  }
  out.add(plan.bitWidth);
  out.addAll(_packDeltas(plan.deltas, plan.bitWidth));
  for (final position in plan.exceptionPositions) {
    _writeUint16(out, position);
  }
  if (plan.type == PhysicalType.float) {
    for (final value in plan.exceptionValues) {
      _writeUint32(out, _float32Bits(value));
    }
  } else {
    for (final value in plan.exceptionValues) {
      _writeUint64(out, _float64Bits(value));
    }
  }
  return out;
}

/// Packs [deltas] LSB-first at [bitWidth] bits per value, padding the final
/// byte with zero bits.
List<int> _packDeltas(List<BigInt> deltas, int bitWidth) {
  if (bitWidth == 0) return const <int>[];
  final out = <int>[];
  var currentByte = 0;
  var bitsInByte = 0;
  for (final delta in deltas) {
    var remaining = bitWidth;
    var shift = 0;
    while (remaining > 0) {
      final free = 8 - bitsInByte;
      final take = free < remaining ? free : remaining;
      final chunkMask = (BigInt.one << take) - BigInt.one;
      final chunk = ((delta >> shift) & chunkMask).toInt();
      currentByte |= chunk << bitsInByte;
      bitsInByte += take;
      shift += take;
      remaining -= take;
      if (bitsInByte == 8) {
        out.add(currentByte);
        currentByte = 0;
        bitsInByte = 0;
      }
    }
  }
  if (bitsInByte > 0) out.add(currentByte);
  return out;
}

// ---------------------------------------------------------------------------
// Small binary helpers
// ---------------------------------------------------------------------------

double _toFloat32(double value) {
  final buffer = Float32List(1);
  buffer[0] = value;
  return buffer[0];
}

void _writeUint16(List<int> out, int value) {
  out
    ..add(value & 0xFF)
    ..add((value >> 8) & 0xFF);
}

void _writeUint32(List<int> out, int value) {
  out
    ..add(value & 0xFF)
    ..add((value >> 8) & 0xFF)
    ..add((value >> 16) & 0xFF)
    ..add((value >> 24) & 0xFF);
}

void _writeUint64(List<int> out, BigInt value) {
  var remaining = value & _u64Mask;
  final byteMask = BigInt.from(0xFF);
  for (var i = 0; i < 8; i++) {
    out.add((remaining & byteMask).toInt());
    remaining >>= 8;
  }
}

void _writeUint32At(List<int> out, int index, int value) {
  out[index] = value & 0xFF;
  out[index + 1] = (value >> 8) & 0xFF;
  out[index + 2] = (value >> 16) & 0xFF;
  out[index + 3] = (value >> 24) & 0xFF;
}
