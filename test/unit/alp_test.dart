import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:parquet/src/encodings/alp.dart';
import 'package:parquet/src/format/enums.dart' show PhysicalType;
import 'package:parquet/src/util/errors.dart';
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

List<int> _hexList(String hex) {
  if (hex.isEmpty) return const <int>[];
  final out = <int>[];
  for (var i = 0; i < hex.length; i += 2) {
    out.add(int.parse(hex.substring(i, i + 2), radix: 16));
  }
  return out;
}

Bytes _hexBytes(String hex) => Bytes.fromList(_hexList(hex));

String _toHex(List<int> bytes) =>
    bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

int _bits32(double value) {
  final byteData = ByteData(4)..setFloat32(0, value, Endian.little);
  return byteData.getUint32(0, Endian.little);
}

List<int> _bits32List(double value) {
  final byteData = ByteData(4)..setFloat32(0, value, Endian.little);
  return List<int>.generate(4, (i) => byteData.getUint8(i));
}

List<int> _bits64List(double value) {
  final byteData = ByteData(8)..setFloat64(0, value, Endian.little);
  return List<int>.generate(8, (i) => byteData.getUint8(i));
}

double _doubleFromBits(List<int> bits) {
  final byteData = ByteData(8);
  for (var i = 0; i < 8; i++) {
    byteData.setUint8(i, bits[i]);
  }
  return byteData.getFloat64(0, Endian.little);
}

/// The NaN stored by the specification's worked example (sign bit clear).
final double _specNan = _doubleFromBits(<int>[0, 0, 0, 0, 0, 0, 0xF8, 0x7F]);

double _f32(double value) => (Float32List(1)..[0] = value)[0];

void _expectFloat32Bits(List<double> actual, List<double> expected) {
  expect(actual.length, expected.length);
  for (var i = 0; i < expected.length; i++) {
    expect(
      _bits32List(actual[i]),
      _bits32List(_f32(expected[i])),
      reason:
          'float32 bits at index $i (actual=${actual[i]}, '
          'expected=${expected[i]})',
    );
  }
}

void _expectFloat64Bits(List<double> actual, List<double> expected) {
  expect(actual.length, expected.length);
  for (var i = 0; i < expected.length; i++) {
    expect(
      _bits64List(actual[i]),
      _bits64List(expected[i]),
      reason:
          'float64 bits at index $i (actual=${actual[i]}, '
          'expected=${expected[i]})',
    );
  }
}

/// Independent LSB-first bit packer used to handcraft decoder input.
List<int> _packBits(List<BigInt> values, int bitWidth) {
  if (bitWidth == 0) return const <int>[];
  final out = <int>[];
  var currentByte = 0;
  var bitsInByte = 0;
  for (final value in values) {
    for (var bit = 0; bit < bitWidth; bit++) {
      final one = ((value >> bit) & BigInt.one) == BigInt.one;
      if (one) currentByte |= 1 << bitsInByte;
      bitsInByte++;
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

void _writeU16(List<int> out, int value) {
  out
    ..add(value & 0xFF)
    ..add((value >> 8) & 0xFF);
}

void _writeU32(List<int> out, int value) {
  out
    ..add(value & 0xFF)
    ..add((value >> 8) & 0xFF)
    ..add((value >> 16) & 0xFF)
    ..add((value >> 24) & 0xFF);
}

void _writeU64(List<int> out, BigInt value) {
  var remaining = value & ((BigInt.one << 64) - BigInt.one);
  final byteMask = BigInt.from(0xFF);
  for (var i = 0; i < 8; i++) {
    out.add((remaining & byteMask).toInt());
    remaining >>= 8;
  }
}

Uint8List _f32Vector({
  required int exponent,
  required int factor,
  required int frame,
  required int bitWidth,
  required List<BigInt> deltas,
  List<int> exceptionPositions = const <int>[],
  List<int> exceptionBits = const <int>[],
}) {
  final out = <int>[exponent, factor];
  _writeU16(out, exceptionPositions.length);
  _writeU32(out, frame);
  out.add(bitWidth);
  out.addAll(_packBits(deltas, bitWidth));
  for (final position in exceptionPositions) {
    _writeU16(out, position);
  }
  for (final bits in exceptionBits) {
    _writeU32(out, bits);
  }
  return Uint8List.fromList(out);
}

Uint8List _f64Vector({
  required int exponent,
  required int factor,
  required BigInt frame,
  required int bitWidth,
  required List<BigInt> deltas,
  List<int> exceptionPositions = const <int>[],
  List<List<int>> exceptionBits = const <List<int>>[],
}) {
  final out = <int>[exponent, factor];
  _writeU16(out, exceptionPositions.length);
  _writeU64(out, frame);
  out.add(bitWidth);
  out.addAll(_packBits(deltas, bitWidth));
  for (final position in exceptionPositions) {
    _writeU16(out, position);
  }
  for (final bits in exceptionBits) {
    out.addAll(bits);
  }
  return Uint8List.fromList(out);
}

Uint8List _page({
  required int logVectorSize,
  required int numElements,
  required List<List<int>> vectors,
}) {
  final out = <int>[
    0,
    0,
    logVectorSize,
    numElements & 0xFF,
    (numElements >> 8) & 0xFF,
    (numElements >> 16) & 0xFF,
    (numElements >> 24) & 0xFF,
  ];
  var bodyLength = vectors.length * 4;
  for (final vector in vectors) {
    _writeU32(out, bodyLength);
    bodyLength += vector.length;
  }
  for (final vector in vectors) {
    out.addAll(vector);
  }
  return Uint8List.fromList(out);
}

/// Structural view of an encoded page, parsed independently of the codec.
final class _ParsedPage {
  _ParsedPage({
    required this.logVectorSize,
    required this.numElements,
    required this.offsets,
    required this.vectors,
    required this.bodyLength,
  });

  final int logVectorSize;
  final int numElements;
  final List<int> offsets;
  final List<_ParsedVector> vectors;
  final int bodyLength;

  int get vectorSize => 1 << logVectorSize;

  static _ParsedPage parse(Uint8List page, PhysicalType type) {
    expect(page.length, greaterThanOrEqualTo(alpHeaderSize));
    expect(page[0], 0);
    expect(page[1], 0);
    final logVectorSize = page[2];
    final numElements =
        page[3] | (page[4] << 8) | (page[5] << 16) | (page[6] << 24);
    final vectorSize = 1 << logVectorSize;
    final numVectors = numElements == 0
        ? 0
        : (numElements + vectorSize - 1) >> logVectorSize;
    final offsets = <int>[];
    for (var i = 0; i < numVectors; i++) {
      offsets.add(_u32(page, alpHeaderSize + i * 4));
    }
    final bodyLength = page.length - alpHeaderSize;
    final vectors = <_ParsedVector>[];
    for (var i = 0; i < numVectors; i++) {
      final start = alpHeaderSize + offsets[i];
      final end = i + 1 < numVectors
          ? alpHeaderSize + offsets[i + 1]
          : alpHeaderSize + bodyLength;
      final count = <int>[
        vectorSize,
        numElements - i * vectorSize,
      ].reduce((a, b) => a < b ? a : b);
      vectors.add(_ParsedVector.parse(page, start, end, count, type));
    }
    return _ParsedPage(
      logVectorSize: logVectorSize,
      numElements: numElements,
      offsets: offsets,
      vectors: vectors,
      bodyLength: bodyLength,
    );
  }

  static int _u32(Uint8List page, int offset) =>
      page[offset] |
      (page[offset + 1] << 8) |
      (page[offset + 2] << 16) |
      (page[offset + 3] << 24);
}

final class _ParsedVector {
  _ParsedVector({
    required this.exponent,
    required this.factor,
    required this.numExceptions,
    required this.frame,
    required this.bitWidth,
    required this.storedSize,
    required this.count,
  });

  final int exponent;
  final int factor;
  final int numExceptions;
  final BigInt frame;
  final int bitWidth;
  final int storedSize;
  final int count;

  static _ParsedVector parse(
    Uint8List page,
    int start,
    int end,
    int count,
    PhysicalType type,
  ) {
    expect(end, greaterThanOrEqualTo(start));
    final exponent = page[start];
    final factor = page[start + 1];
    final numExceptions = page[start + 2] | (page[start + 3] << 8);
    final isFloat = type == PhysicalType.float;
    final metaSize = alpInfoSize + (isFloat ? 5 : 9);
    final frame = isFloat
        ? BigInt.from(_ParsedPage._u32(page, start + 4))
        : (BigInt.from(_ParsedPage._u32(page, start + 8)) << 32) |
              BigInt.from(_ParsedPage._u32(page, start + 4));
    final bitWidth = page[start + (isFloat ? 8 : 12)];
    final valueSize = isFloat ? 4 : 8;
    final packedSize = (count * bitWidth + 7) >> 3;
    final storedSize = metaSize + packedSize + numExceptions * (2 + valueSize);
    expect(end - start, storedSize, reason: 'vector stored size');
    return _ParsedVector(
      exponent: exponent,
      factor: factor,
      numExceptions: numExceptions,
      frame: frame,
      bitWidth: bitWidth,
      storedSize: storedSize,
      count: count,
    );
  }
}

// Specification worked example (AlpEncoding.md): doubles
// { 1500.0, NaN, 2500.0, 333.5 }, exponent=4, factor=3, one exception at
// position 1. The bytes below were produced independently from the layout
// rules (and cross-checked with a separate decoder).
const String _specExamplePageHex =
    '00000a040000000400000004030100070d0000000000000f91adc85628150000'
    '0100000000000000f87f';

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('ALP page layout (decoder)', () {
    test('decodes the specification worked example', () {
      final decoded = decodeAlp(
        _hexBytes(_specExamplePageHex),
        type: PhysicalType.double,
        numValues: 4,
      );
      expect(decoded, isA<Float64List>());
      final values = decoded as Float64List;
      expect(values.length, 4);
      _expectFloat64Bits(values, <double>[1500.0, _specNan, 2500.0, 333.5]);
      // The exception NaN must keep its stored payload bits.
      expect(
        _bits64List(values[1]),
        _bits64List(
          (ByteData(8)..setUint32(4, 0x7FF80000, Endian.little)).getFloat64(
            0,
            Endian.little,
          ),
        ),
      );
    });

    test('handcrafted page equals the independently computed layout', () {
      // Build the same worked example with the test helpers and compare.
      final frame = BigInt.from(3335);
      final vector = _f64Vector(
        exponent: 4,
        factor: 3,
        frame: frame,
        bitWidth: 15,
        deltas: <BigInt>[
          BigInt.from(11665),
          BigInt.from(11665),
          BigInt.from(21665),
          BigInt.zero,
        ],
        exceptionPositions: <int>[1],
        exceptionBits: <List<int>>[
          <int>[0, 0, 0, 0, 0, 0, 0xF8, 0x7F],
        ],
      );
      final page = _page(
        logVectorSize: 10,
        numElements: 4,
        vectors: <List<int>>[vector],
      );
      expect(_toHex(page), _specExamplePageHex);
      final decoded = decodeAlpFloat64(Bytes.fromList(page), numValues: 4);
      _expectFloat64Bits(decoded, <double>[1500.0, _specNan, 2500.0, 333.5]);
    });

    test('decodes a handcrafted float page without exceptions', () {
      // e=0, f=0: encoded 1/2/3 map directly to 1.0/2.0/3.0.
      final page = _page(
        logVectorSize: 10,
        numElements: 3,
        vectors: <List<int>>[
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: 1,
            bitWidth: 2,
            deltas: <BigInt>[BigInt.zero, BigInt.one, BigInt.two],
          ),
        ],
      );
      final decoded = decodeAlpFloat32(Bytes.fromList(page), numValues: 3);
      _expectFloat32Bits(decoded, <double>[1.0, 2.0, 3.0]);
    });

    test('decodes zero-bit-width vectors (all values equal the frame)', () {
      final f32Page = _page(
        logVectorSize: 10,
        numElements: 3,
        vectors: <List<int>>[
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: 42,
            bitWidth: 0,
            deltas: const <BigInt>[],
          ),
        ],
      );
      _expectFloat32Bits(
        decodeAlpFloat32(Bytes.fromList(f32Page), numValues: 3),
        <double>[42.0, 42.0, 42.0],
      );

      final frame = (BigInt.one << 63) - BigInt.one;
      final f64Page = _page(
        logVectorSize: 10,
        numElements: 2,
        vectors: <List<int>>[
          _f64Vector(
            exponent: 0,
            factor: 0,
            frame: frame,
            bitWidth: 0,
            deltas: const <BigInt>[],
          ),
        ],
      );
      final decoded = decodeAlpFloat64(Bytes.fromList(f64Page), numValues: 2);
      expect(decoded.length, 2);
      // int64 max converts to binary64 by rounding to 2^63.
      _expectFloat64Bits(decoded, <double>[frame.toDouble(), frame.toDouble()]);
      expect(decoded[0], 9223372036854775808.0);
    });

    test('decodes a negative frame with unsigned FOR deltas', () {
      // frame = -2 (0xFFFFFFFE), e=0/f=0, encoded [-2, 1, 100].
      final page = _page(
        logVectorSize: 10,
        numElements: 3,
        vectors: <List<int>>[
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: 0xFFFFFFFE,
            bitWidth: 7,
            deltas: <BigInt>[BigInt.zero, BigInt.from(3), BigInt.from(102)],
          ),
        ],
      );
      _expectFloat32Bits(
        decodeAlpFloat32(Bytes.fromList(page), numValues: 3),
        <double>[-2.0, 1.0, 100.0],
      );
    });

    test('rounds int32 encoded values to binary32', () {
      // 2^24 + 1 is not representable in binary32; the cast rounds to 2^24.
      final page = _page(
        logVectorSize: 10,
        numElements: 1,
        vectors: <List<int>>[
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: 16777217,
            bitWidth: 0,
            deltas: const <BigInt>[],
          ),
        ],
      );
      final decoded = decodeAlpFloat32(Bytes.fromList(page), numValues: 1);
      expect(_bits32(decoded[0]), _bits32(16777216.0));
    });

    test('decodes multiple vectors with a short trailing vector', () {
      // vector_size 8, 20 values -> vectors of 8, 8 and 4 values.
      final vectors = <List<int>>[
        for (var vectorIndex = 0; vectorIndex < 3; vectorIndex++)
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: vectorIndex * 100,
            bitWidth: 0,
            deltas: const <BigInt>[],
          ),
      ];
      final page = _page(logVectorSize: 3, numElements: 20, vectors: vectors);
      final parsed = _ParsedPage.parse(page, PhysicalType.float);
      expect(parsed.offsets, <int>[12, 21, 30]);
      final decoded = decodeAlpFloat32(Bytes.fromList(page), numValues: 20);
      expect(decoded.length, 20);
      expect(decoded[0], 0.0);
      expect(decoded[7], 0.0);
      expect(decoded[8], 100.0);
      expect(decoded[16], 200.0);
      expect(decoded[19], 200.0);
    });

    test('patches exceptions with exact IEEE bit patterns', () {
      final f32Bits = <int>[
        0x7FC00001, // NaN with a payload
        0x80000000, // -0.0
        0x7F800000, // +inf
      ];
      final f32Page = _page(
        logVectorSize: 10,
        numElements: 6,
        vectors: <List<int>>[
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: 0,
            bitWidth: 0,
            deltas: const <BigInt>[],
            exceptionPositions: <int>[0, 2, 5],
            exceptionBits: f32Bits,
          ),
        ],
      );
      final f32Decoded = decodeAlpFloat32(
        Bytes.fromList(f32Page),
        numValues: 6,
      );
      expect(_bits32(f32Decoded[0]), f32Bits[0]);
      expect(_bits32(f32Decoded[2]), f32Bits[1]);
      expect(_bits32(f32Decoded[5]), f32Bits[2]);
      expect(f32Decoded[1], 0.0);

      final f64Bits = <List<int>>[
        <int>[1, 0, 0, 0, 0, 0, 0xF8, 0x7F],
        <int>[0, 0, 0, 0, 0, 0, 0, 0x80],
        <int>[0, 0, 0, 0, 0, 0, 0xF0, 0x7F],
      ];
      final f64Page = _page(
        logVectorSize: 10,
        numElements: 6,
        vectors: <List<int>>[
          _f64Vector(
            exponent: 0,
            factor: 0,
            frame: BigInt.zero,
            bitWidth: 0,
            deltas: const <BigInt>[],
            exceptionPositions: <int>[1, 3, 4],
            exceptionBits: f64Bits,
          ),
        ],
      );
      final f64Decoded = decodeAlpFloat64(
        Bytes.fromList(f64Page),
        numValues: 6,
      );
      expect(_bits64List(f64Decoded[1]), f64Bits[0]);
      expect(_bits64List(f64Decoded[3]), f64Bits[1]);
      expect(_bits64List(f64Decoded[4]), f64Bits[2]);
    });

    test('decodes empty pages', () {
      final f32Empty = encodeAlp(<double>[], type: PhysicalType.float);
      final f64Empty = encodeAlp(<double>[], type: PhysicalType.double);
      expect(f32Empty.length, alpHeaderSize);
      expect(f64Empty.length, alpHeaderSize);
      expect(decodeAlpFloat32(Bytes.fromList(f32Empty), numValues: 0), isEmpty);
      expect(decodeAlpFloat64(Bytes.fromList(f64Empty), numValues: 0), isEmpty);
      // numValues is an upper bound: a zero-element header still yields [].
      expect(
        decodeAlpFloat64(Bytes.fromList(f64Empty), numValues: 10),
        isEmpty,
      );
      // All-null pages may omit the encoded value payload entirely.
      expect(decodeAlpFloat32(Bytes.empty, numValues: 0), isEmpty);
      expect(decodeAlpFloat64(Bytes.empty, numValues: 0), isEmpty);
      expect(
        () => decodeAlpFloat32(Bytes.empty, numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('numValues is an upper bound, header count is authoritative', () {
      final page = _page(
        logVectorSize: 10,
        numElements: 2,
        vectors: <List<int>>[
          _f32Vector(
            exponent: 0,
            factor: 0,
            frame: 5,
            bitWidth: 0,
            deltas: const <BigInt>[],
          ),
        ],
      );
      final decoded = decodeAlpFloat32(Bytes.fromList(page), numValues: 100);
      expect(decoded.length, 2);
      expect(decoded[0], 5.0);
    });
  });

  group('ALP decoder validation', () {
    Bytes pageWithFirstVector(Uint8List vector, {int log = 10, int count = 1}) {
      return Bytes.fromList(
        _page(
          logVectorSize: log,
          numElements: count,
          vectors: <List<int>>[vector],
        ),
      );
    }

    test('rejects a truncated page header', () {
      expect(
        () => decodeAlp(
          _hexBytes('000000000000'),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('rejects unsupported compression mode and integer encoding', () {
      final badMode = Uint8List.fromList(
        encodeAlp(<double>[1.0], type: PhysicalType.float),
      );
      badMode[0] = 1;
      expect(
        () => decodeAlp(
          Bytes.fromList(badMode),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
      final badEncoding = Uint8List.fromList(
        encodeAlp(<double>[1.0], type: PhysicalType.float),
      );
      badEncoding[1] = 1;
      expect(
        () => decodeAlp(
          Bytes.fromList(badEncoding),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects invalid log_vector_size', () {
      for (final log in <int>[2, 16, 255]) {
        final page = Uint8List.fromList(<int>[0, 0, log, 0, 0, 0, 0]);
        expect(
          () => decodeAlp(
            Bytes.fromList(page),
            type: PhysicalType.float,
            numValues: 0,
          ),
          throwsA(isA<ParquetFormatException>()),
          reason: 'log $log',
        );
      }
    });

    test('rejects negative num_elements', () {
      final page = Uint8List.fromList(<int>[0, 0, 10, 0xFF, 0xFF, 0xFF, 0xFF]);
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 0,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects header count greater than numValues', () {
      final page = encodeAlp(<double>[1.0, 2.0], type: PhysicalType.float);
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects a truncated offsets array', () {
      // Two values at vector_size 1024 -> one offset, but the page ends after
      // the header.
      final page = Uint8List.fromList(<int>[0, 0, 10, 2, 0, 0, 0]);
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 2,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('rejects a wrong first offset', () {
      final vector = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 0,
        bitWidth: 0,
        deltas: const <BigInt>[],
      );
      final page = Uint8List.fromList(
        _page(logVectorSize: 10, numElements: 1, vectors: <List<int>>[vector]),
      );
      page[alpHeaderSize] = 8; // expected 4
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects non-contiguous vector offsets', () {
      final vectorA = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 1,
        bitWidth: 0,
        deltas: const <BigInt>[],
      );
      final vectorB = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 2,
        bitWidth: 0,
        deltas: const <BigInt>[],
      );
      final page = Uint8List.fromList(
        _page(
          logVectorSize: 3,
          numElements: 16,
          vectors: <List<int>>[vectorA, vectorB],
        ),
      );
      // Second offset is at header + 4 bytes.
      final second = alpHeaderSize + 4;
      final original =
          page[second] |
          (page[second + 1] << 8) |
          (page[second + 2] << 16) |
          (page[second + 3] << 24);
      final shifted = original + 1;
      page[second] = shifted & 0xFF;
      page[second + 1] = (shifted >> 8) & 0xFF;
      page[second + 2] = (shifted >> 16) & 0xFF;
      page[second + 3] = (shifted >> 24) & 0xFF;
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 16,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects an offset beyond the page body', () {
      final page = Uint8List.fromList(<int>[
        0, 0, 10, 1, 0, 0, 0, // header
        8, 0, 0, 0, // offset 8 > body length 4
      ]);
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects truncated vector metadata', () {
      final page = _page(
        logVectorSize: 10,
        numElements: 1,
        vectors: <List<int>>[
          <int>[0, 0, 0, 0],
        ],
      );
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('rejects invalid exponent and factor', () {
      final good = _f32Vector(
        exponent: 1,
        factor: 1,
        frame: 0,
        bitWidth: 0,
        deltas: const <BigInt>[],
      );
      final badExponent = Uint8List.fromList(good)..[0] = 11;
      expect(
        () => decodeAlp(
          pageWithFirstVector(badExponent),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
      final badFactor = Uint8List.fromList(good)..[1] = 2;
      expect(
        () => decodeAlp(
          pageWithFirstVector(badFactor),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects excessive exception counts and bit widths', () {
      final tooManyExceptions = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 0,
        bitWidth: 0,
        deltas: const <BigInt>[],
      );
      // num_exceptions = 2 for a single-value vector.
      tooManyExceptions[2] = 2;
      tooManyExceptions[3] = 0;
      expect(
        () => decodeAlp(
          pageWithFirstVector(tooManyExceptions),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );

      final wideBits = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 0,
        bitWidth: 33,
        deltas: <BigInt>[BigInt.zero],
      );
      expect(
        () => decodeAlp(
          pageWithFirstVector(wideBits),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );

      final wideBits64 = _f64Vector(
        exponent: 0,
        factor: 0,
        frame: BigInt.zero,
        bitWidth: 65,
        deltas: <BigInt>[BigInt.zero],
      );
      expect(
        () => decodeAlp(
          pageWithFirstVector(wideBits64),
          type: PhysicalType.double,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects truncated packed values and exception sections', () {
      // bit width 8 for one value needs one byte, but the vector ends after
      // the metadata.
      final vector = <int>[0, 0, 0, 0, 0, 0, 0, 0, 8];
      final page = _page(
        logVectorSize: 10,
        numElements: 1,
        vectors: <List<int>>[vector],
      );
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetEofException>()),
      );

      final exceptionVector = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 0,
        bitWidth: 0,
        deltas: const <BigInt>[],
        exceptionPositions: <int>[0],
        exceptionBits: <int>[0x3F800000],
      );
      final truncated = Uint8List.fromList(
        _page(
          logVectorSize: 10,
          numElements: 1,
          vectors: <List<int>>[
            exceptionVector.sublist(0, exceptionVector.length - 1),
          ],
        ),
      );
      expect(
        () => decodeAlp(
          Bytes.fromList(truncated),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('rejects unclaimed trailing bytes inside a vector', () {
      final vector = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 7,
        bitWidth: 0,
        deltas: const <BigInt>[],
      );
      final padded = <int>[...vector, 0xAA];
      final page = _page(
        logVectorSize: 10,
        numElements: 1,
        vectors: <List<int>>[padded],
      );
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects an exception position outside the vector', () {
      final vector = _f32Vector(
        exponent: 0,
        factor: 0,
        frame: 0,
        bitWidth: 0,
        deltas: const <BigInt>[],
        exceptionPositions: <int>[1],
        exceptionBits: <int>[0x3F800000],
      );
      final page = _page(
        logVectorSize: 10,
        numElements: 1,
        vectors: <List<int>>[vector],
      );
      expect(
        () => decodeAlp(
          Bytes.fromList(page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects unsupported physical types', () {
      expect(
        () => decodeAlp(Bytes.empty, type: PhysicalType.int32, numValues: 0),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => encodeAlp(<num>[1], type: PhysicalType.int32),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => encodeAlp(<num>[1], type: PhysicalType.byteArray),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('rejects invalid vector size parameters on encode', () {
      for (final log in <int>[2, 16]) {
        expect(
          () => encodeAlp(
            <double>[1.0],
            type: PhysicalType.float,
            logVectorSize: log,
          ),
          throwsArgumentError,
          reason: 'log $log',
        );
      }
    });
  });

  group('ALP encode/decode round-trip', () {
    test('float64 decimal values round-trip bit-exactly', () {
      final values = <double>[
        0.0,
        1.0,
        -1.0,
        0.1,
        0.2,
        0.3,
        -0.5,
        12.34,
        -56.78,
        1500.0,
        2500.0,
        333.5,
        123456.789,
        -987.654321,
        1e18,
        -1e18,
        1e-18,
        1e-9,
        1234567890123456789.0,
        1.0 / 3.0,
        double.nan,
        double.infinity,
        double.negativeInfinity,
        -0.0,
      ];
      final page = encodeAlp(values, type: PhysicalType.double);
      final decoded = decodeAlpFloat64(
        Bytes.fromList(page),
        numValues: values.length,
      );
      _expectFloat64Bits(decoded, values);
    });

    test('float32 decimal values round-trip bit-exactly', () {
      final values = <double>[
        0.0,
        1.0,
        -1.0,
        0.1,
        0.2,
        0.3,
        -0.5,
        12.34,
        1500.0,
        2500.0,
        333.5,
        1e30,
        -1e30,
        1e-30,
        1.0 / 3.0,
        double.nan,
        double.infinity,
        double.negativeInfinity,
        -0.0,
      ];
      final page = encodeAlp(values, type: PhysicalType.float);
      final decoded = decodeAlpFloat32(
        Bytes.fromList(page),
        numValues: values.length,
      );
      _expectFloat32Bits(decoded, values);
    });

    test('round-trips multiple vectors (float64)', () {
      final values = <double>[
        for (var i = 0; i < 3000; i++) ((i * 37) % 1000) / 10.0 - 50.0,
      ];
      for (final log in <int>[3, 10, 15]) {
        final page = encodeAlp(
          values,
          type: PhysicalType.double,
          logVectorSize: log,
        );
        final decoded = decodeAlpFloat64(
          Bytes.fromList(page),
          numValues: values.length,
        );
        _expectFloat64Bits(decoded, values);
      }
    });

    test('round-trips multiple vectors (float32)', () {
      final values = <double>[
        for (var i = 0; i < 1500; i++) ((i * 13) % 512) / 8.0 - 32.0,
      ];
      for (final log in <int>[3, 10]) {
        final page = encodeAlp(
          values,
          type: PhysicalType.float,
          logVectorSize: log,
        );
        final decoded = decodeAlpFloat32(
          Bytes.fromList(page),
          numValues: values.length,
        );
        _expectFloat32Bits(decoded, values);
      }
    });

    test('all-identical vectors use a zero bit width and no exceptions', () {
      for (final type in <PhysicalType>[
        PhysicalType.float,
        PhysicalType.double,
      ]) {
        final values = <double>[for (var i = 0; i < 100; i++) 7.25];
        final page = encodeAlp(values, type: type);
        final parsed = _ParsedPage.parse(page, type);
        expect(parsed.vectors.length, 1);
        expect(parsed.vectors[0].bitWidth, 0, reason: '$type');
        expect(parsed.vectors[0].numExceptions, 0, reason: '$type');
        if (type == PhysicalType.float) {
          _expectFloat32Bits(
            decodeAlpFloat32(Bytes.fromList(page), numValues: values.length),
            values,
          );
        } else {
          _expectFloat64Bits(
            decodeAlpFloat64(Bytes.fromList(page), numValues: values.length),
            values,
          );
        }
      }
    });

    test('all-exception vectors round-trip their IEEE bits', () {
      for (final type in <PhysicalType>[
        PhysicalType.float,
        PhysicalType.double,
      ]) {
        final values = <double>[
          double.nan,
          double.infinity,
          double.negativeInfinity,
          -0.0,
        ];
        final page = encodeAlp(values, type: type);
        final parsed = _ParsedPage.parse(page, type);
        expect(parsed.vectors[0].numExceptions, values.length);
        expect(parsed.vectors[0].bitWidth, 0);
        if (type == PhysicalType.float) {
          _expectFloat32Bits(
            decodeAlpFloat32(Bytes.fromList(page), numValues: values.length),
            values,
          );
        } else {
          _expectFloat64Bits(
            decodeAlpFloat64(Bytes.fromList(page), numValues: values.length),
            values,
          );
        }
      }
    });

    test('non-round-tripping values become exceptions and stay exact', () {
      // f64 max scaled by any power of ten is outside the int64 FOR range;
      // the smallest subnormal rounds to zero under any decimal scale.
      final values = <double>[
        double.maxFinite,
        -double.maxFinite,
        5e-324,
        -5e-324,
      ];
      final page = encodeAlp(values, type: PhysicalType.double);
      final parsed = _ParsedPage.parse(page, PhysicalType.double);
      final exceptions = parsed.vectors.fold<int>(
        0,
        (sum, v) => sum + v.numExceptions,
      );
      expect(exceptions, values.length);
      _expectFloat64Bits(
        decodeAlpFloat64(Bytes.fromList(page), numValues: values.length),
        values,
      );
    });

    test('decimal hundredths use a non-zero factor and compress well', () {
      final values = <double>[for (var i = 0; i < 1024; i++) i / 100.0];
      final page = encodeAlp(values, type: PhysicalType.double);
      final parsed = _ParsedPage.parse(page, PhysicalType.double);
      expect(parsed.vectors.length, 1);
      expect(parsed.vectors[0].factor, greaterThan(0));
      expect(parsed.vectors[0].numExceptions, 0);
      expect(parsed.vectors[0].bitWidth, lessThanOrEqualTo(10));
      expect(page.length, lessThan(4096)); // far below 1024 * 8 bytes
      _expectFloat64Bits(
        decodeAlpFloat64(Bytes.fromList(page), numValues: values.length),
        values,
      );
    });

    test('empty input produces a header-only page', () {
      for (final type in <PhysicalType>[
        PhysicalType.float,
        PhysicalType.double,
      ]) {
        final page = encodeAlp(<double>[], type: type);
        expect(page.length, alpHeaderSize);
        expect(page.sublist(0, 3), <int>[0, 0, alpDefaultLogVectorSize]);
        expect(page.sublist(3, 7), <int>[0, 0, 0, 0]);
      }
    });

    test('single and boundary-sized inputs', () {
      for (final log in <int>[3, 10]) {
        final vectorSize = 1 << log;
        for (final count in <int>[1, vectorSize, vectorSize + 1]) {
          final values = <double>[for (var i = 0; i < count; i++) i * 0.25];
          final page = encodeAlp(
            values,
            type: PhysicalType.double,
            logVectorSize: log,
          );
          final decoded = decodeAlpFloat64(
            Bytes.fromList(page),
            numValues: count,
          );
          _expectFloat64Bits(decoded, values);
          final parsed = _ParsedPage.parse(page, PhysicalType.double);
          expect(
            parsed.vectors.length,
            (count + vectorSize - 1) ~/ vectorSize,
            reason: 'log=$log count=$count',
          );
        }
      }
    });

    test('encoding is deterministic', () {
      final values = <double>[for (var i = 0; i < 500; i++) (i % 97) / 4.0];
      final first = encodeAlp(values, type: PhysicalType.double);
      final second = encodeAlp(values, type: PhysicalType.double);
      expect(first, second);
    });

    test('values near the int64 FOR limit round-trip', () {
      final values = <double>[9e18, -9e18, 9.1e18, 1.0, 0.0, -1.0];
      final page = encodeAlp(values, type: PhysicalType.double);
      _expectFloat64Bits(
        decodeAlpFloat64(Bytes.fromList(page), numValues: values.length),
        values,
      );
    });
  });

  group('ALP encoder structure', () {
    test('header and offsets follow the specification', () {
      final values = <double>[for (var i = 0; i < 20; i++) i * 1.5];
      final page = encodeAlp(
        values,
        type: PhysicalType.double,
        logVectorSize: 3,
      );
      final parsed = _ParsedPage.parse(page, PhysicalType.double);
      expect(parsed.logVectorSize, 3);
      expect(parsed.numElements, 20);
      expect(parsed.vectorSize, 8);
      expect(parsed.vectors.length, 3);
      expect(parsed.offsets.first, parsed.vectors.length * 4);
      for (var i = 1; i < parsed.offsets.length; i++) {
        expect(
          parsed.offsets[i],
          parsed.offsets[i - 1] + parsed.vectors[i - 1].storedSize,
        );
      }
      expect(
        parsed.offsets.last + parsed.vectors.last.storedSize,
        parsed.bodyLength,
      );
      // The last vector only carries the four remaining values.
      expect(parsed.vectors.last.count, 4);
      expect(parsed.vectors.sublist(0, 2).map((v) => v.count), <int>[8, 8]);
    });

    test('float and double vectors have the specified header sizes', () {
      final f32Page = encodeAlp(<double>[1.5, 2.5], type: PhysicalType.float);
      final f64Page = encodeAlp(<double>[1.5, 2.5], type: PhysicalType.double);
      final f32Vector = _ParsedPage.parse(
        f32Page,
        PhysicalType.float,
      ).vectors[0];
      final f64Vector = _ParsedPage.parse(
        f64Page,
        PhysicalType.double,
      ).vectors[0];
      // AlpInfo (4) + ForInfo (5) + packed bytes.
      expect(
        f32Vector.storedSize,
        alpInfoSize + 5 + ((2 * f32Vector.bitWidth + 7) >> 3),
      );
      expect(
        f64Vector.storedSize,
        alpInfoSize + 9 + ((2 * f64Vector.bitWidth + 7) >> 3),
      );
    });

    test('supports every allowed log_vector_size', () {
      final values = <double>[for (var i = 0; i < 20; i++) i * 0.5];
      for (var log = alpMinLogVectorSize; log <= alpMaxLogVectorSize; log++) {
        final page = encodeAlp(
          values,
          type: PhysicalType.double,
          logVectorSize: log,
        );
        expect(page[2], log);
        final decoded = decodeAlpFloat64(
          Bytes.fromList(page),
          numValues: values.length,
        );
        _expectFloat64Bits(decoded, values);
      }
    });

    test('dispatched decoder returns the requested typed list', () {
      final f32Page = encodeAlp(<double>[1.0], type: PhysicalType.float);
      final f64Page = encodeAlp(<double>[1.0], type: PhysicalType.double);
      expect(
        decodeAlp(
          Bytes.fromList(f32Page),
          type: PhysicalType.float,
          numValues: 1,
        ),
        isA<Float32List>(),
      );
      expect(
        decodeAlp(
          Bytes.fromList(f64Page),
          type: PhysicalType.double,
          numValues: 1,
        ),
        isA<Float64List>(),
      );
    });
  });
}
