import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:parquet/src/encodings/bitpacking.dart';
import 'package:parquet/src/encodings/byte_stream_split.dart';
import 'package:parquet/src/encodings/delta.dart';
import 'package:parquet/src/encodings/dictionary.dart';
import 'package:parquet/src/encodings/encoding.dart';
import 'package:parquet/src/encodings/levels.dart';
import 'package:parquet/src/encodings/plain.dart';
import 'package:parquet/src/encodings/rle.dart';
import 'package:parquet/src/format/enums.dart';
import 'package:parquet/src/util/errors.dart';
import 'package:parquet/src/util/int64.dart';
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

Bytes _b(List<int> bytes) => Bytes.fromList(bytes);

Bytes _hex(String hex) => _b(_hexList(hex));

Bytes _u8Bytes(Uint8List bytes) => Bytes.fromUint8List(bytes);

/// LSB-first bit writer mirroring the Parquet packing order.
class _BitWriter {
  final List<int> _bytes = <int>[];
  int _bitPosition = 0;

  void put(int value, int width) {
    for (var i = 0; i < width; i++) {
      final byteIndex = _bitPosition >> 3;
      if (byteIndex == _bytes.length) _bytes.add(0);
      if (((value >> i) & 1) != 0) {
        _bytes[byteIndex] |= 1 << (_bitPosition & 7);
      }
      _bitPosition++;
    }
  }

  Uint8List take() => Uint8List.fromList(_bytes);
}

/// LSB-first bit writer for values wider than 63 bits (test-only).
class _BigBitWriter {
  final List<int> _bytes = <int>[];
  int _bitPosition = 0;

  void put(BigInt value, int width) {
    for (var i = 0; i < width; i++) {
      final byteIndex = _bitPosition >> 3;
      if (byteIndex == _bytes.length) _bytes.add(0);
      if (((value >> i) & BigInt.one) != BigInt.zero) {
        _bytes[byteIndex] |= 1 << (_bitPosition & 7);
      }
      _bitPosition++;
    }
  }

  Uint8List take() => Uint8List.fromList(_bytes);
}

/// MSB-first bit writer used by the deprecated BIT_PACKED level encoding.
class _MsbBitWriter {
  final List<int> _bytes = <int>[];
  int _bitPosition = 0;

  void put(int value, int width) {
    for (var i = width - 1; i >= 0; i--) {
      final byteIndex = _bitPosition >> 3;
      if (byteIndex == _bytes.length) _bytes.add(0);
      if (((value >> i) & 1) != 0) {
        _bytes[byteIndex] |= 1 << (7 - (_bitPosition & 7));
      }
      _bitPosition++;
    }
  }

  Uint8List take() => Uint8List.fromList(_bytes);
}

void _writeVarint32(List<int> out, int value) {
  var x = value & 0xFFFFFFFF;
  while (x >= 0x80) {
    out.add((x & 0x7F) | 0x80);
    x >>>= 7;
  }
  out.add(x);
}

void _writeZigZag64(List<int> out, BigInt value) {
  final mask64 = (BigInt.one << 64) - BigInt.one;
  var x = ((value << 1) ^ (value >> 63)) & mask64;
  while (x >= BigInt.from(0x80)) {
    out.add(((x & BigInt.from(0x7F)).toInt()) | 0x80);
    x >>= 7;
  }
  out.add(x.toInt());
}

BigInt _wrapSigned(BigInt value, int bits) {
  final modulus = BigInt.one << bits;
  final unsigned = value & (modulus - BigInt.one);
  final signBit = BigInt.one << (bits - 1);
  return unsigned >= signBit ? unsigned - modulus : unsigned;
}

BigInt _wrapUnsigned(BigInt value, int bits) =>
    value & ((BigInt.one << bits) - BigInt.one);

int _requiredBitsBig(BigInt value) {
  var bits = 0;
  var v = value;
  while (v != BigInt.zero) {
    bits++;
    v >>= 1;
  }
  return bits;
}

/// Test-only RLE / bit-packed hybrid encoder.
class _RleEncoder {
  _RleEncoder(this.bitWidth);

  final int bitWidth;
  final List<int> _bytes = <int>[];

  void rle(int value, int count) {
    _writeVarint32(_bytes, count << 1);
    final valueBytes = (bitWidth + 7) >> 3;
    for (var i = 0; i < valueBytes; i++) {
      _bytes.add((value >> (8 * i)) & 0xFF);
    }
  }

  void bitPacked(List<int> values) {
    final padded = [...values];
    while (padded.length % 8 != 0) {
      padded.add(0);
    }
    _writeVarint32(_bytes, ((padded.length ~/ 8) << 1) | 1);
    final writer = _BitWriter();
    for (final value in padded) {
      writer.put(value, bitWidth);
    }
    _bytes.addAll(writer.take());
  }

  Uint8List take() => Uint8List.fromList(_bytes);
}

/// Test-only DELTA_BINARY_PACKED encoder (pads used miniblocks like the spec).
class _DeltaEncoder {
  _DeltaEncoder({this.blockSize = 128, this.miniBlocks = 4});

  final int blockSize;
  final int miniBlocks;

  Uint8List encodeInt32(List<int> values) =>
      _encode([for (final v in values) BigInt.from(v)], 32);

  Uint8List encodeInt64(List<BigInt> values) => _encode(values, 64);

  Uint8List _encode(List<BigInt> values, int bits) {
    final out = <int>[];
    _writeVarint32(out, blockSize);
    _writeVarint32(out, miniBlocks);
    _writeVarint32(out, values.length);
    if (values.isEmpty) {
      _writeZigZag64(out, BigInt.zero);
      return Uint8List.fromList(out);
    }
    _writeZigZag64(out, values.first);

    final deltas = <BigInt>[
      for (var i = 1; i < values.length; i++)
        _wrapSigned(values[i] - values[i - 1], bits),
    ];
    final miniSize = blockSize ~/ miniBlocks;
    for (
      var blockStart = 0;
      blockStart < deltas.length;
      blockStart += blockSize
    ) {
      final blockEnd = blockStart + blockSize < deltas.length
          ? blockStart + blockSize
          : deltas.length;
      final block = deltas.sublist(blockStart, blockEnd);
      var minDelta = block.first;
      for (final delta in block) {
        if (delta < minDelta) minDelta = delta;
      }
      _writeZigZag64(out, minDelta);

      final residuals = <BigInt>[
        for (final delta in block) _wrapUnsigned(delta - minDelta, bits),
      ];
      final widths = List<int>.filled(miniBlocks, 0);
      for (var m = 0; m < miniBlocks; m++) {
        final from = m * miniSize;
        final to = from + miniSize < residuals.length
            ? from + miniSize
            : residuals.length;
        if (from >= to) continue;
        var maxResidual = BigInt.zero;
        for (var i = from; i < to; i++) {
          if (residuals[i] > maxResidual) maxResidual = residuals[i];
        }
        widths[m] = _requiredBitsBig(maxResidual);
      }
      out.addAll(widths);

      for (var m = 0; m < miniBlocks; m++) {
        final from = m * miniSize;
        final to = from + miniSize < residuals.length
            ? from + miniSize
            : residuals.length;
        if (from >= to) continue;
        final writer = _BigBitWriter();
        for (var i = from; i < to; i++) {
          writer.put(residuals[i], widths[m]);
        }
        // The final used miniblock is padded to its full value count.
        for (var i = to - from; i < miniSize; i++) {
          writer.put(BigInt.zero, widths[m]);
        }
        out.addAll(writer.take());
      }
    }
    return Uint8List.fromList(out);
  }
}

Uint8List _plainInt32Bytes(List<int> values) {
  final byteData = ByteData(values.length * 4);
  for (var i = 0; i < values.length; i++) {
    byteData.setInt32(i * 4, values[i], Endian.little);
  }
  return byteData.buffer.asUint8List(
    byteData.offsetInBytes,
    byteData.lengthInBytes,
  );
}

Uint8List _plainInt64Bytes(List<BigInt> values) {
  final out = Uint8List(values.length * 8);
  final mask64 = (BigInt.one << 64) - BigInt.one;
  for (var i = 0; i < values.length; i++) {
    final unsigned = values[i] & mask64;
    for (var j = 0; j < 8; j++) {
      out[i * 8 + j] = ((unsigned >> (8 * j)) & BigInt.from(0xFF)).toInt();
    }
  }
  return out;
}

Uint8List _plainFloat32Bytes(List<double> values) {
  final byteData = ByteData(values.length * 4);
  for (var i = 0; i < values.length; i++) {
    byteData.setFloat32(i * 4, values[i], Endian.little);
  }
  return byteData.buffer.asUint8List(
    byteData.offsetInBytes,
    byteData.lengthInBytes,
  );
}

Uint8List _plainFloat64Bytes(List<double> values) {
  final byteData = ByteData(values.length * 8);
  for (var i = 0; i < values.length; i++) {
    byteData.setFloat64(i * 8, values[i], Endian.little);
  }
  return byteData.buffer.asUint8List(
    byteData.offsetInBytes,
    byteData.lengthInBytes,
  );
}

Uint8List _plainByteArrayBytes(List<List<int>> values) {
  final out = <int>[];
  for (final value in values) {
    out
      ..add(value.length & 0xFF)
      ..add((value.length >> 8) & 0xFF)
      ..add((value.length >> 16) & 0xFF)
      ..add((value.length >> 24) & 0xFF)
      ..addAll(value);
  }
  return Uint8List.fromList(out);
}

Uint8List _plainFixedBytes(List<List<int>> values, int width) {
  final out = <int>[];
  for (final value in values) {
    if (value.length != width) {
      throw ArgumentError('value must be $width bytes');
    }
    out.addAll(value);
  }
  return Uint8List.fromList(out);
}

Uint8List _plainBooleanBytes(List<bool> values) {
  final out = Uint8List((values.length + 7) >> 3);
  for (var i = 0; i < values.length; i++) {
    if (values[i]) out[i >> 3] |= 1 << (i & 7);
  }
  return out;
}

Uint8List _bssFloat32Bytes(List<double> values) {
  final raw = Float32List.fromList(values);
  final source = raw.buffer.asUint8List();
  final out = Uint8List(source.length);
  for (var j = 0; j < 4; j++) {
    for (var i = 0; i < values.length; i++) {
      out[j * values.length + i] = source[i * 4 + j];
    }
  }
  return out;
}

Uint8List _bssFloat64Bytes(List<double> values) {
  final raw = Float64List.fromList(values);
  final source = raw.buffer.asUint8List();
  final out = Uint8List(source.length);
  for (var j = 0; j < 8; j++) {
    for (var i = 0; i < values.length; i++) {
      out[j * values.length + i] = source[i * 8 + j];
    }
  }
  return out;
}

Uint8List _bssInt32Bytes(List<int> values) {
  final raw = _plainInt32Bytes(values);
  final out = Uint8List(raw.length);
  for (var j = 0; j < 4; j++) {
    for (var i = 0; i < values.length; i++) {
      out[j * values.length + i] = raw[i * 4 + j];
    }
  }
  return out;
}

Uint8List _bssInt64Bytes(List<BigInt> values) {
  final raw = _plainInt64Bytes(values);
  final out = Uint8List(raw.length);
  for (var j = 0; j < 8; j++) {
    for (var i = 0; i < values.length; i++) {
      out[j * values.length + i] = raw[i * 8 + j];
    }
  }
  return out;
}

Uint8List _bssFixedBytes(List<List<int>> values, int width) {
  final out = Uint8List(values.length * width);
  for (var j = 0; j < width; j++) {
    for (var i = 0; i < values.length; i++) {
      out[j * values.length + i] = values[i][j];
    }
  }
  return out;
}

(int lo, int hi) _limbsOf(BigInt value) {
  final unsigned = value & ((BigInt.one << 64) - BigInt.one);
  final lo = (unsigned & BigInt.from(0xFFFFFFFF)).toInt();
  final hi = ((unsigned >> 32) & BigInt.from(0xFFFFFFFF)).toInt();
  return (lo, hi);
}

void _expectFloats32(List<double> actual, List<double> expected) {
  expect(actual.length, expected.length);
  for (var i = 0; i < expected.length; i++) {
    final e = expected[i];
    final a = actual[i];
    if (e.isNaN) {
      expect(a.isNaN, isTrue, reason: 'index $i should be NaN');
    } else {
      expect(a, e, reason: 'index $i');
      if (e == 0) {
        expect(a.isNegative, e.isNegative, reason: 'signed zero at $i');
      }
    }
  }
}

void _expectFloats64(List<double> actual, List<double> expected) {
  expect(actual.length, expected.length);
  for (var i = 0; i < expected.length; i++) {
    final e = expected[i];
    final a = actual[i];
    if (e.isNaN) {
      expect(a.isNaN, isTrue, reason: 'index $i should be NaN');
    } else {
      expect(a, e, reason: 'index $i');
      if (e == 0) {
        expect(a.isNegative, e.isNegative, reason: 'signed zero at $i');
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Real vectors produced by Apache Arrow C++ (pyarrow 26.0.0), data page v1,
// uncompressed, exactly as emitted by a mainstream writer.
// ---------------------------------------------------------------------------

// i32 body (142 bytes)
const String _i32Hex =
    '800104140efdffffff0f20000000f5ffff7fea03008017fcfffffeffff7f0000000029000080abffff7f8c000080b1cc5b87d565487119cd5b87ffffff7fffffff7f00000080000000800000008000000080000000800000008000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';
const List<int> _i32Expected = <int>[
  7,
  -3,
  1000,
  -2147483648,
  2147483647,
  0,
  42,
  -42,
  99,
  123456789,
  -123456789,
  5,
  5,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
];

// i64 body (539 bytes)
const String _i64Hex =
    '80020414f68180808080808020adffffffffffffffff0140000000deffffffffffff7f4effffffffffff2fdeffffffffffffbfd6ffffffffffff7fd9ffffffffffff7fdbffffffffffffbfd2ffffffffffff2fd8ffffffffff1f90d5ffffffffffbf7f020000000000208083ffffffffffff7f0000000000000000d8ffffffffffff7fec80e97df4102211adfd2c0417debb5dec80e97df4102291d7ffffffffffff7fdeffffffffffff7fc9ffffffffffff7f000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';
final List<BigInt> _i64Expected = <BigInt>[
  BigInt.parse('1152921504606847099'),
  BigInt.parse('1152921504606847106'),
  BigInt.parse('-4611686018427387911'),
  BigInt.parse('0'),
  BigInt.parse('-1'),
  BigInt.parse('1'),
  BigInt.parse('4611686018427387909'),
  BigInt.parse('-1152921504606846976'),
  BigInt.parse('9007199254740993'),
  BigInt.parse('-9007199254740993'),
  BigInt.parse('42'),
  BigInt.parse('-42'),
  BigInt.parse('9223372036854775807'),
  BigInt.parse('-9223372036854775808'),
  BigInt.parse('1234567890123456789'),
  BigInt.parse('-1234567890123456789'),
  BigInt.parse('0'),
  BigInt.parse('0'),
  BigInt.parse('7'),
  BigInt.parse('-7'),
];

// DELTA_LENGTH_BYTE_ARRAY body (139 bytes).
const String _sHex =
    '80010414082706000000157531575549d3645640555555a5010000000000000000007a6574617a65746121616c706861616c70686162657461626361626364616263646578797a7879786c6f6e6765722d737472696e672d76616c75656c6f6e6765722d737472696e672d76616c7565217171717171717171717168656c6c6f68656c6c6f20776f726c64';
const List<String> _sExpected = <String>[
  'zeta',
  'zeta!',
  'alpha',
  'alphabet',
  '',
  'abc',
  'abcd',
  'abcde',
  'xyz',
  'xy',
  'x',
  'longer-string-value',
  'longer-string-value!',
  '',
  'q',
  'qq',
  'qqq',
  'qqqq',
  'hello',
  'hello world',
];

// DELTA_BYTE_ARRAY body (124 bytes).
const String _dsHex =
    '80010414002506000000d7833993453d952499c04451148401000000000000000000800104140823060000008f053d1524518f5402d124499235010000000000000000007a65746121616c706861626574616263646578797a6c6f6e6765722d737472696e672d76616c7565217171717168656c6c6f20776f726c64';
const List<String> _dsExpected = <String>[
  'zeta',
  'zeta!',
  'alpha',
  'alphabet',
  '',
  'abc',
  'abcd',
  'abcde',
  'xyz',
  'xy',
  'x',
  'longer-string-value',
  'longer-string-value!',
  '',
  'q',
  'qq',
  'qqq',
  'qqqq',
  'hello',
  'hello world',
];

// BYTE_STREAM_SPLIT FLOAT body (80 bytes).
const String _f32Hex =
    '00000000000000ca60000000000000000000000000000000000000f2420000000000000080402010c0008080c050e449a200209008840281008000803f807fff7f40c0718d0040404141424243434444';
final List<double> _f32Expected = <double>[
  1.5,
  -0.0,
  double.infinity,
  double.negativeInfinity,
  double.nan,
  3.25,
  -7.125,
  1.0000000150474662e+30,
  -1.0000000031710769e-30,
  0.0,
  2.5,
  4.5,
  8.5,
  16.5,
  32.5,
  64.5,
  128.5,
  256.5,
  512.5,
  1024.5,
];

// BYTE_STREAM_SPLIT DOUBLE body (160 bytes).
const String _f64Hex =
    '189c000000001869599c9a9ad6cf0000000000002d75000000002d57f375999919670000000000004400000000004414f8009999dfc0000000000000548800000000548bc2889999e3e6000000000000fb3c00000000fb0a1f3c9999878700000000000021e40000000021bf6ee49999d6d60000000000000937f0f0f8000905a537b9b932624545f0f0e0e040fe7fff7f804040017e3fbf41c140c03fbf3fbf';
final List<double> _f64Expected = <double>[
  3.141592653589793,
  -1e+300,
  double.infinity,
  double.negativeInfinity,
  double.nan,
  -0.0,
  3.141592653589793,
  2.718281828459045,
  1e-300,
  1e+300,
  0.1,
  -0.1,
  1234567.890123,
  -9876543.210987,
  42.0,
  -42.0,
  1.0,
  -1.0,
  0.5,
  -0.5,
];

// PLAIN BYTE_ARRAY body (135 bytes).
const String _binHex =
    '0100000000000000000200000001020500000068656c6c6f05000000776f726c640300000062696e0300000061727902000000fffe0100000078020000007979030000007a7a7a150000006c6f6e6765722062696e617279207061796c6f6164000000000100000061010000006201000000630100000064010000006501000000660100000067';
final List<List<int>> _binExpected = <List<int>>[
  _hexList('00'),
  _hexList(''),
  _hexList('0102'),
  _hexList('68656c6c6f'),
  _hexList('776f726c64'),
  _hexList('62696e'),
  _hexList('617279'),
  _hexList('fffe'),
  _hexList('78'),
  _hexList('7979'),
  _hexList('7a7a7a'),
  _hexList('6c6f6e6765722062696e617279207061796c6f6164'),
  _hexList(''),
  _hexList('61'),
  _hexList('62'),
  _hexList('63'),
  _hexList('64'),
  _hexList('65'),
  _hexList('66'),
  _hexList('67'),
];

// PLAIN FIXED_LEN_BYTE_ARRAY(type_length = 3) body (60 bytes).
const String _fixedHex =
    '01020361626300000058595a313233343536373839ff00807172737475767778797a6162636465666768696a6b6c6d6e6f707172737475767778797a';
final List<List<int>> _fixedExpected = <List<int>>[
  _hexList('010203'),
  _hexList('616263'),
  _hexList('000000'),
  _hexList('58595a'),
  _hexList('313233'),
  _hexList('343536'),
  _hexList('373839'),
  _hexList('ff0080'),
  _hexList('717273'),
  _hexList('747576'),
  _hexList('777879'),
  _hexList('7a6162'),
  _hexList('636465'),
  _hexList('666768'),
  _hexList('696a6b'),
  _hexList('6c6d6e'),
  _hexList('6f7071'),
  _hexList('727374'),
  _hexList('757677'),
  _hexList('78797a'),
];

// Nullable string page: RLE definition levels followed by PLAIN values.
const String _nstrHex =
    '0400000007b5f70f010000006102000000626203000000636363040000006464646401000000650200000066660300000067676701000000680100000069020000006a6a030000006b6b6b010000006c020000006d6d010000006e020000006f6f03000000707070';
const List<String> _nstrExpected = <String>[
  'a',
  'bb',
  'ccc',
  'dddd',
  'e',
  'ff',
  'ggg',
  'h',
  'i',
  'jj',
  'kkk',
  'l',
  'mm',
  'n',
  'oo',
  'ppp',
];
const List<int> _nstrDefLevels = <int>[
  1,
  0,
  1,
  0,
  1,
  1,
  0,
  1,
  1,
  1,
  1,
  0,
  1,
  1,
  1,
  1,
  1,
  1,
  1,
  1,
];

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('bitpacking', () {
    test('requiredBits', () {
      expect(requiredBits(0), 0);
      expect(requiredBits(1), 1);
      expect(requiredBits(2), 2);
      expect(requiredBits(3), 2);
      expect(requiredBits(255), 8);
      expect(requiredBits(256), 9);
      expect(requiredBits(0xFFFFFFFF), 32);
      expect(() => requiredBits(-1), throwsArgumentError);
    });

    test('packedByteLength', () {
      expect(packedByteLength(0, 100), 0);
      expect(packedByteLength(1, 8), 1);
      expect(packedByteLength(1, 9), 2);
      expect(packedByteLength(3, 8), 3);
      expect(packedByteLength(32, 8), 32);
      expect(packedByteLength(32, 0), 0);
    });

    test('round-trips every width 1..32', () {
      for (var width = 1; width <= 32; width++) {
        final mask = width == 32 ? 0xFFFFFFFF : (1 << width) - 1;
        final values = <int>[
          for (var i = 0; i < 17; i++) (i * 7919 + width * 13) & mask,
        ];
        final writer = _BitWriter();
        for (final value in values) {
          writer.put(value, width);
        }
        final out = Uint32List(values.length);
        unpackBitsLSB(
          _u8Bytes(writer.take()),
          bitWidth: width,
          numValues: values.length,
          out: out,
        );
        expect(out, values, reason: 'width $width');
        for (var i = 0; i < values.length; i++) {
          expect(
            readBitsLSB(_u8Bytes(writer.take()), i * width, width),
            values[i],
            reason: 'width $width index $i',
          );
        }
      }
    });

    test('known little-endian vector', () {
      // [1, 2, 3] at bit width 2 LSB-first: 01 10 11 -> 0x39.
      final data = _b(<int>[0x39]);
      final out = Uint32List(3);
      unpackBitsLSB(data, bitWidth: 2, numValues: 3, out: out);
      expect(out, <int>[1, 2, 3]);
    });

    test('bit width 0 fills zeros without consuming bytes', () {
      final out = Uint32List(5)..fillRange(0, 5, 0xAB);
      unpackBitsLSB(Bytes.empty, bitWidth: 0, numValues: 5, out: out);
      expect(out, <int>[0, 0, 0, 0, 0]);
    });

    test('stops at numValues inside a partial group', () {
      final writer = _BitWriter();
      for (var i = 0; i < 8; i++) {
        writer.put(i & 1, 1);
      }
      final out = Uint32List(3);
      unpackBitsLSB(
        _u8Bytes(writer.take()),
        bitWidth: 1,
        numValues: 3,
        out: out,
      );
      expect(out, <int>[0, 1, 0]);
    });

    test('legacy MSB-first known vector', () {
      // [2, 1] at 3 bits MSB-first: 010 001 -> 0x44.
      final data = _b(<int>[0x44]);
      final out = Uint32List(2);
      unpackBitsMSB(data, bitWidth: 3, numValues: 2, out: out);
      expect(out, <int>[2, 1]);
      expect(readBitsMSB(data, 0, 3), 2);
      expect(readBitsMSB(data, 3, 3), 1);
    });

    test('64-bit limbs', () {
      final writer = _BigBitWriter();
      final values = <BigInt>[
        BigInt.parse('1152921504606847099'),
        BigInt.parse('18446744073709551615'),
        BigInt.zero,
        BigInt.parse('9223372036854775808'),
      ];
      for (final value in values) {
        writer.put(value, 64);
      }
      final data = _u8Bytes(writer.take());
      for (var i = 0; i < values.length; i++) {
        final (lo, hi) = readBitsLSBLimbs(data, i * 64, 64);
        final combined = (BigInt.from(hi) << 32) | BigInt.from(lo);
        expect(combined, values[i], reason: 'index $i');
        expect((lo, hi), _limbsOf(values[i]), reason: 'limbs at $i');
      }
    });

    test('truncated input throws instead of reading out of bounds', () {
      expect(() {
        final out = Uint32List(2);
        unpackBitsLSB(_b(<int>[0xFF]), bitWidth: 8, numValues: 2, out: out);
      }, throwsA(isA<ParquetEofException>()));
      expect(
        () => readBitsLSB(_b(<int>[0x00]), 0, 16),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('rejects invalid widths and output ranges', () {
      expect(() => packedByteLength(-1, 1), throwsArgumentError);
      expect(
        () => unpackBitsLSB(
          Bytes.empty,
          bitWidth: 33,
          numValues: 1,
          out: Uint32List(1),
        ),
        throwsA(isA<ParquetFormatException>()),
      );
      expect(
        () => unpackBitsLSB(
          _b(<int>[0]),
          bitWidth: 1,
          numValues: 9,
          out: Uint32List(8),
        ),
        throwsRangeError,
      );
    });
  });

  group('RLE / bit-packed hybrid', () {
    test('decodes an RLE run', () {
      final encoded = _RleEncoder(3)..rle(5, 10);
      final out = decodeRleBitPacked(
        _u8Bytes(encoded.take()),
        bitWidth: 3,
        numValues: 10,
      );
      expect(out, List<int>.filled(10, 5));
    });

    test('stops in the middle of an RLE run', () {
      final encoded = _RleEncoder(3)..rle(5, 10);
      final out = decodeRleBitPacked(
        _u8Bytes(encoded.take()),
        bitWidth: 3,
        numValues: 7,
      );
      expect(out, List<int>.filled(7, 5));
    });

    test('decodes a bit-packed run', () {
      final values = <int>[0, 1, 2, 3, 0, 1, 2, 3];
      final encoded = _RleEncoder(2)..bitPacked(values);
      final out = decodeRleBitPacked(
        _u8Bytes(encoded.take()),
        bitWidth: 2,
        numValues: 8,
      );
      expect(out, values);
    });

    test('stops in the middle of a bit-packed run', () {
      final values = <int>[0, 1, 2, 3, 0, 1, 2, 3];
      final encoded = _RleEncoder(2)..bitPacked(values);
      final out = decodeRleBitPacked(
        _u8Bytes(encoded.take()),
        bitWidth: 2,
        numValues: 5,
      );
      expect(out, <int>[0, 1, 2, 3, 0]);
    });

    test('decodes mixed runs', () {
      final encoded = _RleEncoder(3)
        ..rle(1, 3)
        ..bitPacked(<int>[0, 1, 2, 3, 4, 5, 6, 7])
        ..rle(0, 2);
      final out = decodeRleBitPacked(
        _u8Bytes(encoded.take()),
        bitWidth: 3,
        numValues: 13,
      );
      expect(out, <int>[1, 1, 1, 0, 1, 2, 3, 4, 5, 6, 7, 0, 0]);
    });

    test('bit width 0 decodes zeros without payload bytes', () {
      // RLE run of 5 zeros: header 10 (0x0A), no value bytes.
      final rleData = _b(<int>[10]);
      expect(decodeRleBitPacked(rleData, bitWidth: 0, numValues: 5), <int>[
        0,
        0,
        0,
        0,
        0,
      ]);
      // Bit-packed run of 8 zeros: header (1 << 1) | 1 = 3, no payload.
      final packedData = _b(<int>[3]);
      expect(decodeRleBitPacked(packedData, bitWidth: 0, numValues: 8), <int>[
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
      ]);
    });

    test('32-bit values (RLE and bit-packed runs)', () {
      final encoded = _RleEncoder(32)
        ..rle(0xFFFFFFFF, 2)
        ..bitPacked(<int>[0x80000000, 0xFFFFFFFF, 0, 1, 2, 3, 4, 5]);
      final out = decodeRleBitPacked(
        _u8Bytes(encoded.take()),
        bitWidth: 32,
        numValues: 10,
      );
      expect(out, <int>[
        0xFFFFFFFF,
        0xFFFFFFFF,
        0x80000000,
        0xFFFFFFFF,
        0,
        1,
        2,
        3,
        4,
        5,
      ]);
    });

    test('length-prefixed streams', () {
      final encoded = _RleEncoder(2)..bitPacked(<int>[0, 1, 2, 3]);
      final payload = encoded.take();
      final lengthBytes = _b(<int>[
        payload.length & 0xFF,
        (payload.length >> 8) & 0xFF,
        (payload.length >> 16) & 0xFF,
        (payload.length >> 24) & 0xFF,
        ...payload,
      ]);
      final out = decodeRleBitPackedLengthPrefixed(
        lengthBytes,
        bitWidth: 2,
        numValues: 4,
      );
      expect(out, <int>[0, 1, 2, 3]);
      expect(readRleBitPackedLength(lengthBytes), payload.length);

      expect(
        () => decodeRleBitPackedLengthPrefixed(
          _b(<int>[0xFF, 0x00, 0x00, 0x00, 1, 2]),
          bitWidth: 2,
          numValues: 4,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('empty / zero-length requests succeed without reading', () {
      expect(
        decodeRleBitPacked(Bytes.empty, bitWidth: 5, numValues: 0),
        isEmpty,
      );
    });

    test('truncated or malformed streams throw', () {
      expect(
        () => decodeRleBitPacked(Bytes.empty, bitWidth: 1, numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      // Zero header cannot encode values.
      expect(
        () => decodeRleBitPacked(_b(<int>[0]), bitWidth: 1, numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      // Bit-packed run announces bytes that are not there.
      expect(
        () => decodeRleBitPacked(_b(<int>[3, 0x01]), bitWidth: 8, numValues: 8),
        throwsA(isA<ParquetEofException>()),
      );
      // Invalid bit width.
      expect(
        () => decodeRleBitPacked(_b(<int>[2]), bitWidth: 33, numValues: 1),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });

  group('levels', () {
    test('max level 0 yields zeros without consuming data', () {
      final levels = decodeLevels(
        Bytes.empty,
        maxLevel: 0,
        numValues: 4,
        encoding: Encoding.rle,
        lengthPrefix: true,
      );
      expect(levels, <int>[0, 0, 0, 0]);
    });

    test('RLE levels with a v1 length prefix', () {
      final encoded = _RleEncoder(2)..bitPacked(<int>[0, 1, 2, 3, 1]);
      final payload = encoded.take();
      final data = _b(<int>[
        payload.length & 0xFF,
        (payload.length >> 8) & 0xFF,
        (payload.length >> 16) & 0xFF,
        (payload.length >> 24) & 0xFF,
        ...payload,
      ]);
      final levels = decodeLevels(
        data,
        maxLevel: 3,
        numValues: 5,
        encoding: Encoding.rle,
        lengthPrefix: true,
      );
      expect(levels, <int>[0, 1, 2, 3, 1]);
    });

    test('RLE levels without a prefix (data page v2)', () {
      final data = _u8Bytes(
        (_RleEncoder(2)..bitPacked(<int>[1, 1, 0, 1])).take(),
      );
      final levels = decodeLevels(
        data,
        maxLevel: 3,
        numValues: 4,
        encoding: Encoding.rle,
      );
      expect(levels, <int>[1, 1, 0, 1]);
    });

    test('legacy BIT_PACKED levels are MSB-first', () {
      final writer = _MsbBitWriter();
      for (final value in <int>[0, 1, 3, 2, 1]) {
        writer.put(value, 2);
      }
      final levels = decodeLevels(
        _u8Bytes(writer.take()),
        maxLevel: 3,
        numValues: 5,
        encoding: Encoding.bitPacked,
      );
      expect(levels, <int>[0, 1, 3, 2, 1]);
    });

    test('values above the maximum level are rejected', () {
      final encoded = _RleEncoder(2)..bitPacked(<int>[3, 3]);
      expect(
        () => decodeLevels(
          _u8Bytes(encoded.take()),
          maxLevel: 2,
          numValues: 2,
          encoding: Encoding.rle,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('invalid level encodings are rejected', () {
      expect(
        () => decodeLevels(
          Bytes.empty,
          maxLevel: 1,
          numValues: 1,
          encoding: Encoding.plain,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('truncated BIT_PACKED levels throw', () {
      expect(
        () => decodeLevels(
          _b(<int>[0x00]),
          maxLevel: 3,
          numValues: 5,
          encoding: Encoding.bitPacked,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('levels deeper than 255 use the wide decoder', () {
      expect(
        () => decodeLevels(
          Bytes.empty,
          maxLevel: 300,
          numValues: 1,
          encoding: Encoding.rle,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );

      final encoded = _RleEncoder(9)..bitPacked(<int>[0, 300, 255, 1]);
      final data = _b(<int>[...encoded.take()]);
      final levels = decodeLevelsWide(
        data,
        maxLevel: 300,
        numValues: 4,
        encoding: Encoding.rle,
      );
      expect(levels, <int>[0, 300, 255, 1]);
    });

    test('real Arrow definition levels (nullable string page)', () {
      final body = _hex(_nstrHex);
      final levels = decodeLevels(
        body,
        maxLevel: 1,
        numValues: 20,
        encoding: Encoding.rle,
        lengthPrefix: true,
      );
      expect(levels, _nstrDefLevels);
    });
  });

  group('PLAIN', () {
    test('boolean values are LSB-first bit-packed', () {
      final values = <bool>[
        true,
        false,
        true,
        true,
        false,
        false,
        false,
        true,
        true,
      ];
      final raw = _plainBooleanBytes(values);
      final data = _u8Bytes(raw);
      final out = decodePlainBoolean(data, numValues: values.length);
      expect(out, <int>[1, 0, 1, 1, 0, 0, 0, 1, 1]);
      // Last byte is padded.
      expect(raw, <int>[0x8D, 0x01]);
      expect(decodePlainBoolean(data, numValues: 0), isEmpty);
    });

    test('int32 values', () {
      final values = <int>[-1, 0, 1, -2147483648, 2147483647, 123456, -123456];
      final out = decodePlainInt32(
        _u8Bytes(_plainInt32Bytes(values)),
        numValues: values.length,
      );
      expect(out, values);
    });

    test('int64 values beyond 2^53 are exact', () {
      final values = <BigInt>[
        BigInt.parse('1152921504606847099'),
        BigInt.parse('-4611686018427387911'),
        BigInt.parse('9223372036854775807'),
        BigInt.parse('-9223372036854775808'),
        BigInt.zero,
        BigInt.parse('9007199254740993'),
      ];
      final buffer = decodePlainInt64(
        _u8Bytes(_plainInt64Bytes(values)),
        numValues: values.length,
      );
      expect(buffer.length, values.length);
      expect(buffer.isNative, Int64Buffer(1).isNative);
      for (var i = 0; i < values.length; i++) {
        expect(buffer.getBigInt(i), values[i], reason: 'value $i');
        expect(buffer.getLimbs(i), _limbsOf(values[i]), reason: 'limbs $i');
      }
    });

    test('float32 values', () {
      final values = <double>[
        1.5,
        -0.0,
        double.infinity,
        double.negativeInfinity,
        double.nan,
        3.25,
        -7.125,
      ];
      final out = decodePlainFloat32(
        _u8Bytes(_plainFloat32Bytes(values)),
        numValues: values.length,
      );
      _expectFloats32(out, values);
    });

    test('float64 values', () {
      final values = <double>[
        3.141592653589793,
        -1e300,
        double.infinity,
        double.negativeInfinity,
        double.nan,
        -0.0,
        0.1,
      ];
      final out = decodePlainFloat64(
        _u8Bytes(_plainFloat64Bytes(values)),
        numValues: values.length,
      );
      _expectFloats64(out, values);
    });

    test('BYTE_ARRAY values with empty and non-ASCII entries', () {
      final values = <List<int>>[
        <int>[],
        'abc'.codeUnits,
        <int>[],
        'héllo'.codeUnits,
        <int>[0, 255, 128],
      ];
      final out = decodePlainByteArray(
        _u8Bytes(_plainByteArrayBytes(values)),
        numValues: values.length,
      );
      expect(out.numValues, values.length);
      expect(out.offsets.length, values.length + 1);
      for (var i = 0; i < values.length; i++) {
        expect(out.valueAt(i), values[i], reason: 'value $i');
      }
      expect(out.offsets.first, 0);
      expect(out.offsets.last, out.data.length);
    });

    test('real PLAIN BYTE_ARRAY values', () {
      final out = decodePlainByteArray(_hex(_binHex), numValues: 20);
      expect(out.numValues, _binExpected.length);
      for (var i = 0; i < _binExpected.length; i++) {
        expect(out.valueAt(i), _binExpected[i], reason: 'value $i');
      }
    });

    test('nullable page: PLAIN values after real definition levels', () {
      final body = _hex(_nstrHex);
      final plain = decodePlainByteArray(body.slice(8), numValues: 16);
      expect(plain.numValues, _nstrExpected.length);
      for (var i = 0; i < _nstrExpected.length; i++) {
        expect(
          String.fromCharCodes(plain.valueAt(i)),
          _nstrExpected[i],
          reason: 'value $i',
        );
      }
    });

    test('FIXED_LEN_BYTE_ARRAY values', () {
      final values = <List<int>>[
        <int>[1, 2, 3],
        <int>[4, 5, 6],
        <int>[0, 0, 0],
      ];
      final out = decodePlainFixedLenByteArray(
        _u8Bytes(_plainFixedBytes(values, 3)),
        numValues: values.length,
        typeLength: 3,
      );
      expect(out.numValues, values.length);
      expect(out.width, 3);
      for (var i = 0; i < values.length; i++) {
        expect(out.valueAt(i), values[i], reason: 'value $i');
      }
      final asByteArray = out.toByteArrayData();
      expect(asByteArray.numValues, values.length);
      expect(asByteArray.offsets, <int>[0, 3, 6, 9]);
      expect(
        () => decodePlainFixedLenByteArray(
          Bytes.empty,
          numValues: 0,
          typeLength: 0,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('real PLAIN FIXED_LEN_BYTE_ARRAY values', () {
      final out = decodePlainFixedLenByteArray(
        _hex(_fixedHex),
        numValues: 20,
        typeLength: 3,
      );
      expect(out.numValues, _fixedExpected.length);
      for (var i = 0; i < _fixedExpected.length; i++) {
        expect(out.valueAt(i), _fixedExpected[i], reason: 'value $i');
      }
    });

    test('INT96 values are 12-byte fixed-length values', () {
      final raw = <int>[for (var i = 0; i < 24; i++) i & 0xFF];
      final out = decodePlainInt96(_b(raw), numValues: 2);
      expect(out.width, 12);
      expect(out.numValues, 2);
      expect(out.valueAt(0), <int>[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
      expect(out.valueAt(1), <int>[
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
      ]);
    });

    test('empty inputs produce empty results', () {
      expect(decodePlainBoolean(Bytes.empty, numValues: 0), isEmpty);
      expect(decodePlainInt32(Bytes.empty, numValues: 0), isEmpty);
      expect(decodePlainInt64(Bytes.empty, numValues: 0).length, 0);
      expect(decodePlainFloat32(Bytes.empty, numValues: 0), isEmpty);
      expect(decodePlainFloat64(Bytes.empty, numValues: 0), isEmpty);
      expect(decodePlainByteArray(Bytes.empty, numValues: 0).numValues, 0);
      expect(
        decodePlainFixedLenByteArray(
          Bytes.empty,
          numValues: 0,
          typeLength: 4,
        ).numValues,
        0,
      );
    });

    test('truncated inputs throw', () {
      expect(
        () => decodePlainInt32(_b(<int>[1, 0, 0]), numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodePlainInt64(_b(<int>[1, 2, 3, 4]), numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodePlainBoolean(Bytes.empty, numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodePlainByteArray(_b(<int>[5, 0, 0, 0, 1, 2]), numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodePlainByteArray(_b(<int>[5, 0]), numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodePlainFixedLenByteArray(
          _b(<int>[1, 2]),
          numValues: 1,
          typeLength: 3,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });
  });

  group('dictionary indices', () {
    test('reads the bit width prefix and decodes a bit-packed run', () {
      final encoded = _RleEncoder(2)..bitPacked(<int>[0, 1, 2, 3, 0, 0]);
      final data = _b(<int>[2, ...encoded.take()]);
      final indices = decodeDictionaryIndices(data, numValues: 6);
      expect(indices, <int>[0, 1, 2, 3, 0, 0]);
    });

    test('decodes an RLE run', () {
      final encoded = _RleEncoder(3)..rle(5, 10);
      final data = _b(<int>[3, ...encoded.take()]);
      expect(
        decodeDictionaryIndices(data, numValues: 10),
        List<int>.filled(10, 5),
      );
    });

    test('bit width 0 yields all-zero indices', () {
      final data = _b(<int>[0, 20]); // width 0, RLE run of 10
      expect(
        decodeDictionaryIndices(data, numValues: 10),
        List<int>.filled(10, 0),
      );
    });

    test('rejects malformed streams', () {
      expect(
        () => decodeDictionaryIndices(Bytes.empty, numValues: 3),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodeDictionaryIndices(_b(<int>[33]), numValues: 1),
        throwsA(isA<ParquetFormatException>()),
      );
      expect(decodeDictionaryIndices(Bytes.empty, numValues: 0), isEmpty);
      expect(decodeDictionaryIndices(_b(<int>[8]), numValues: 0), isEmpty);
    });
  });

  group('DELTA_BINARY_PACKED INT32', () {
    test('empty stream', () {
      final data = _u8Bytes(_DeltaEncoder().encodeInt32(<int>[]));
      expect(decodeDeltaBinaryPackedInt32(data), isEmpty);
    });

    test('single value', () {
      final data = _u8Bytes(_DeltaEncoder().encodeInt32(<int>[-5]));
      expect(decodeDeltaBinaryPackedInt32(data), <int>[-5]);
    });

    test('real Arrow vector (partial miniblock padded to 32 values)', () {
      final values = decodeDeltaBinaryPackedInt32(
        _hex(_i32Hex),
        expectedCount: 20,
      );
      expect(values, _i32Expected);
      expect(
        () => decodeDeltaBinaryPackedInt32(_hex(_i32Hex), expectedCount: 19),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('handcrafted vector from the reference implementation', () {
      // block size 128, 4 miniblocks, 3 values, first value 29, min delta 14,
      // miniblock width 6 -> values [29, 43, 89].
      final data = _b(<int>[
        128,
        1,
        4,
        3,
        58,
        28,
        6,
        0,
        0,
        0,
        0,
        8,
        for (var i = 0; i < 22; i++) 0,
      ]);
      expect(data.length, 34);
      expect(decodeDeltaBinaryPackedInt32(data), <int>[29, 43, 89]);
    });

    test('round-trips multiple blocks and negative deltas', () {
      final values = <int>[for (var i = 0; i < 300; i++) (i * 37) % 211 - 105];
      final data = _u8Bytes(
        _DeltaEncoder(blockSize: 128, miniBlocks: 4).encodeInt32(values),
      );
      expect(decodeDeltaBinaryPackedInt32(data), values);

      final values2 = <int>[
        for (var i = 0; i < 50; i++) i.isEven ? i * 3 : -i * 7,
      ];
      final data2 = _u8Bytes(
        _DeltaEncoder(blockSize: 256, miniBlocks: 4).encodeInt32(values2),
      );
      expect(decodeDeltaBinaryPackedInt32(data2), values2);
    });

    test('uniform step (all miniblock widths are zero)', () {
      final values = <int>[for (var i = 0; i < 500; i++) i * 7 - 3000];
      final data = _u8Bytes(_DeltaEncoder().encodeInt32(values));
      expect(decodeDeltaBinaryPackedInt32(data), values);
    });

    test('extreme int32 values (wrapping deltas)', () {
      final values = <int>[
        -2147483648,
        -2147483648,
        2147483647,
        -2147483648,
        2147483647,
        -1,
        0,
        1,
        -2147483648,
        2147483647,
      ];
      final data = _u8Bytes(
        _DeltaEncoder(blockSize: 128, miniBlocks: 4).encodeInt32(values),
      );
      expect(decodeDeltaBinaryPackedInt32(data), values);
    });

    test('rejects malformed headers and truncated payloads', () {
      // block size not a multiple of 128
      expect(
        () => decodeDeltaBinaryPackedInt32(_b(<int>[100, 4, 1, 0])),
        throwsA(isA<ParquetFormatException>()),
      );
      // zero miniblocks
      expect(
        () => decodeDeltaBinaryPackedInt32(_b(<int>[128, 1, 0, 1, 0])),
        throwsA(isA<ParquetFormatException>()),
      );
      // truncated header
      expect(
        () => decodeDeltaBinaryPackedInt32(_b(<int>[128, 1])),
        throwsA(isA<ParquetEofException>()),
      );
      // truncated payload: width 8, 32-value miniblock needs 32 bytes.
      final truncated = <int>[128, 1, 4, 2, 0]; // 2 values, first 0
      truncated.add(0); // min delta 0
      truncated.addAll(<int>[8, 0, 0, 0]); // widths
      truncated.addAll(<int>[1, 2, 3]); // only 3 of 32 bytes
      expect(
        () => decodeDeltaBinaryPackedInt32(_b(truncated)),
        throwsA(isA<ParquetEofException>()),
      );
    });
  });

  group('DELTA_BINARY_PACKED INT64', () {
    test('empty stream', () {
      final data = _u8Bytes(_DeltaEncoder().encodeInt64(<BigInt>[]));
      expect(decodeDeltaBinaryPackedInt64(data).length, 0);
    });

    test('single value beyond 2^53', () {
      final value = BigInt.parse('1152921504606847099');
      final data = _u8Bytes(_DeltaEncoder().encodeInt64(<BigInt>[value]));
      final out = decodeDeltaBinaryPackedInt64(data);
      expect(out.length, 1);
      expect(out.getBigInt(0), value);
    });

    test('real Arrow vector is exact (64-bit wide miniblock)', () {
      final out = decodeDeltaBinaryPackedInt64(
        _hex(_i64Hex),
        expectedCount: 20,
      );
      expect(out.length, _i64Expected.length);
      for (var i = 0; i < _i64Expected.length; i++) {
        expect(out.getBigInt(i), _i64Expected[i], reason: 'value $i');
        expect(out.getLimbs(i), _limbsOf(_i64Expected[i]), reason: 'limbs $i');
      }
      if (out.isNative) {
        expect(out.int64List, isNotNull);
        expect(out.getInt(0), _i64Expected[0].toInt());
      }
    });

    test('round-trips huge positive and negative values', () {
      final values = <BigInt>[
        for (var i = 0; i < 200; i++)
          BigInt.from(i * i * 7919) * (i.isEven ? BigInt.one : -BigInt.one) +
              (BigInt.one << 60),
      ];
      final data = _u8Bytes(
        _DeltaEncoder(blockSize: 128, miniBlocks: 4).encodeInt64(values),
      );
      final out = decodeDeltaBinaryPackedInt64(data);
      expect(out.length, values.length);
      for (var i = 0; i < values.length; i++) {
        expect(out.getBigInt(i), values[i], reason: 'value $i');
      }
    });

    test('int64 boundaries and wrapping deltas', () {
      final values = <BigInt>[
        BigInt.parse('-9223372036854775808'),
        BigInt.parse('-9223372036854775808'),
        BigInt.parse('9223372036854775807'),
        BigInt.parse('-9223372036854775808'),
        BigInt.parse('9223372036854775807'),
        BigInt.zero,
        BigInt.parse('-1'),
        BigInt.one,
      ];
      final data = _u8Bytes(
        _DeltaEncoder(blockSize: 128, miniBlocks: 4).encodeInt64(values),
      );
      final out = decodeDeltaBinaryPackedInt64(data);
      for (var i = 0; i < values.length; i++) {
        expect(out.getBigInt(i), values[i], reason: 'value $i');
        expect(out.getLimbs(i), _limbsOf(values[i]), reason: 'limbs $i');
      }
    });

    test('matches expectedCount', () {
      final data = _u8Bytes(
        _DeltaEncoder().encodeInt64(<BigInt>[BigInt.one, BigInt.two]),
      );
      expect(
        () => decodeDeltaBinaryPackedInt64(data, expectedCount: 3),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects malformed headers', () {
      expect(
        () => decodeDeltaBinaryPackedInt64(_b(<int>[128, 1, 4, 0])),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodeDeltaBinaryPackedInt64(_b(<int>[128, 1, 0, 0, 0])),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });

  group('DELTA_LENGTH_BYTE_ARRAY', () {
    test('real Arrow vector', () {
      final out = decodeDeltaLengthByteArray(_hex(_sHex), expectedCount: 20);
      expect(out.numValues, _sExpected.length);
      for (var i = 0; i < _sExpected.length; i++) {
        expect(
          String.fromCharCodes(out.valueAt(i)),
          _sExpected[i],
          reason: 'value $i',
        );
      }
      expect(
        () => decodeDeltaLengthByteArray(_hex(_sHex), expectedCount: 19),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('round-trips empty and varying-length values', () {
      final values = <String>[
        for (var i = 0; i < 100; i++) List<String>.filled(i % 17, 'x').join(),
      ];
      final lengthStream = _DeltaEncoder(
        blockSize: 128,
        miniBlocks: 4,
      ).encodeInt32(<int>[for (final value in values) value.length]);
      final payload = <int>[
        ...lengthStream,
        for (final value in values) ...value.codeUnits,
      ];
      final out = decodeDeltaLengthByteArray(
        _u8Bytes(Uint8List.fromList(payload)),
      );
      expect(out.numValues, values.length);
      for (var i = 0; i < values.length; i++) {
        expect(
          String.fromCharCodes(out.valueAt(i)),
          values[i],
          reason: 'value $i',
        );
      }
    });

    test('negative lengths and truncated bytes are rejected', () {
      final negative = _DeltaEncoder().encodeInt32(<int>[-1]);
      expect(
        () => decodeDeltaLengthByteArray(_u8Bytes(negative)),
        throwsA(isA<ParquetFormatException>()),
      );

      final lengths = _DeltaEncoder().encodeInt32(<int>[5, 5]);
      final truncated = <int>[...lengths, 1, 2, 3];
      expect(
        () =>
            decodeDeltaLengthByteArray(_u8Bytes(Uint8List.fromList(truncated))),
        throwsA(isA<ParquetEofException>()),
      );
    });

    test('empty stream', () {
      final data = _u8Bytes(_DeltaEncoder().encodeInt32(<int>[]));
      final out = decodeDeltaLengthByteArray(data);
      expect(out.numValues, 0);
      expect(out.data, isEmpty);
      expect(out.offsets, <int>[0]);
    });
  });

  group('DELTA_BYTE_ARRAY', () {
    test('real Arrow vector', () {
      final out = decodeDeltaByteArray(_hex(_dsHex), expectedCount: 20);
      expect(out.numValues, _dsExpected.length);
      for (var i = 0; i < _dsExpected.length; i++) {
        expect(
          String.fromCharCodes(out.valueAt(i)),
          _dsExpected[i],
          reason: 'value $i',
        );
      }
    });

    test('round-trips shared-prefix strings', () {
      final values = <String>[
        'alpha',
        'alphabet',
        'alphanumeric',
        'alpine',
        'beta',
        '',
        'bet',
        'zeta',
        'zeta!',
        'z',
      ];
      final prefixes = <int>[0];
      final suffixes = <String>[values.first];
      for (var i = 1; i < values.length; i++) {
        final previous = values[i - 1];
        var common = 0;
        while (common < previous.length &&
            common < values[i].length &&
            previous.codeUnitAt(common) == values[i].codeUnitAt(common)) {
          common++;
        }
        prefixes.add(common);
        suffixes.add(values[i].substring(common));
      }
      final prefixStream = _DeltaEncoder().encodeInt32(prefixes);
      final suffixLengths = _DeltaEncoder().encodeInt32(<int>[
        for (final suffix in suffixes) suffix.length,
      ]);
      final payload = <int>[
        ...prefixStream,
        ...suffixLengths,
        for (final suffix in suffixes) ...suffix.codeUnits,
      ];
      final out = decodeDeltaByteArray(_u8Bytes(Uint8List.fromList(payload)));
      expect(out.numValues, values.length);
      for (var i = 0; i < values.length; i++) {
        expect(
          String.fromCharCodes(out.valueAt(i)),
          values[i],
          reason: 'value $i',
        );
      }
    });

    test('empty stream', () {
      final prefixStream = _DeltaEncoder().encodeInt32(<int>[]);
      final suffixStream = _DeltaEncoder().encodeInt32(<int>[]);
      final out = decodeDeltaByteArray(
        _u8Bytes(Uint8List.fromList(<int>[...prefixStream, ...suffixStream])),
      );
      expect(out.numValues, 0);
      expect(out.offsets, <int>[0]);
    });

    test('prefix longer than the previous value is rejected', () {
      final prefixStream = _DeltaEncoder().encodeInt32(<int>[0, 5, 1]);
      final suffixStream = _DeltaEncoder().encodeInt32(<int>[2, 1, 1]);
      final payload = <int>[
        ...prefixStream,
        ...suffixStream,
        ...'ab'.codeUnits,
        ...'c'.codeUnits,
        ...'d'.codeUnits,
      ];
      expect(
        () => decodeDeltaByteArray(_u8Bytes(Uint8List.fromList(payload))),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('mismatched prefix/suffix counts are rejected', () {
      final prefixStream = _DeltaEncoder().encodeInt32(<int>[0, 0]);
      final suffixStream = _DeltaEncoder().encodeInt32(<int>[1, 1, 1]);
      final payload = <int>[
        ...prefixStream,
        ...suffixStream,
        ...'abc'.codeUnits,
      ];
      expect(
        () => decodeDeltaByteArray(_u8Bytes(Uint8List.fromList(payload))),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('truncated suffix bytes are rejected', () {
      final prefixStream = _DeltaEncoder().encodeInt32(<int>[0, 1]);
      final suffixStream = _DeltaEncoder().encodeInt32(<int>[3, 3]);
      final payload = <int>[
        ...prefixStream,
        ...suffixStream,
        ...'ab'.codeUnits,
      ];
      expect(
        () => decodeDeltaByteArray(_u8Bytes(Uint8List.fromList(payload))),
        throwsA(isA<ParquetEofException>()),
      );
    });
  });

  group('BYTE_STREAM_SPLIT', () {
    test('float32 round trip', () {
      final values = <double>[
        1.5,
        -0.0,
        double.infinity,
        double.negativeInfinity,
        double.nan,
        3.25,
        -7.125,
        1024.5,
      ];
      final out = decodeByteStreamSplitFloat32(
        _u8Bytes(_bssFloat32Bytes(values)),
        numValues: values.length,
      );
      _expectFloats32(out, values);
    });

    test('float64 round trip', () {
      final values = <double>[
        3.141592653589793,
        -1e300,
        double.infinity,
        double.negativeInfinity,
        double.nan,
        -0.0,
        0.1,
      ];
      final out = decodeByteStreamSplitFloat64(
        _u8Bytes(_bssFloat64Bytes(values)),
        numValues: values.length,
      );
      _expectFloats64(out, values);
    });

    test('real Arrow FLOAT and DOUBLE vectors', () {
      _expectFloats32(
        decodeByteStreamSplitFloat32(_hex(_f32Hex), numValues: 20),
        _f32Expected,
      );
      _expectFloats64(
        decodeByteStreamSplitFloat64(_hex(_f64Hex), numValues: 20),
        _f64Expected,
      );
    });

    test('int32 round trip (Arrow extension)', () {
      final values = <int>[
        -2147483648,
        -1,
        0,
        1,
        2147483647,
        123456789,
        -123456789,
        42,
      ];
      final out = decodeByteStreamSplitInt32(
        _u8Bytes(_bssInt32Bytes(values)),
        numValues: values.length,
      );
      expect(out, values);
    });

    test('int64 round trip is exact beyond 2^53 (Arrow extension)', () {
      final values = <BigInt>[
        BigInt.parse('1152921504606847099'),
        BigInt.parse('-4611686018427387911'),
        BigInt.parse('9223372036854775807'),
        BigInt.parse('-9223372036854775808'),
        BigInt.zero,
        BigInt.parse('9007199254740993'),
        BigInt.parse('-7'),
      ];
      final out = decodeByteStreamSplitInt64(
        _u8Bytes(_bssInt64Bytes(values)),
        numValues: values.length,
      );
      expect(out.length, values.length);
      for (var i = 0; i < values.length; i++) {
        expect(out.getBigInt(i), values[i], reason: 'value $i');
        expect(out.getLimbs(i), _limbsOf(values[i]), reason: 'limbs $i');
      }
    });

    test('FIXED_LEN_BYTE_ARRAY transpose', () {
      final values = <List<int>>[
        <int>[1, 2, 3],
        <int>[4, 5, 6],
        <int>[7, 8, 9],
        <int>[10, 11, 12],
      ];
      final out = decodeByteStreamSplitFixedLenByteArray(
        _u8Bytes(_bssFixedBytes(values, 3)),
        numValues: values.length,
        typeLength: 3,
      );
      expect(out.numValues, values.length);
      expect(out.offsets, <int>[0, 3, 6, 9, 12]);
      for (var i = 0; i < values.length; i++) {
        expect(out.valueAt(i), values[i], reason: 'value $i');
      }
    });

    test('empty inputs', () {
      expect(decodeByteStreamSplitFloat32(Bytes.empty, numValues: 0), isEmpty);
      expect(decodeByteStreamSplitFloat64(Bytes.empty, numValues: 0), isEmpty);
      final fixed = decodeByteStreamSplitFixedLenByteArray(
        Bytes.empty,
        numValues: 0,
        typeLength: 4,
      );
      expect(fixed.numValues, 0);
      expect(fixed.offsets, <int>[0]);
    });

    test('truncated data and invalid lengths are rejected', () {
      expect(
        () => decodeByteStreamSplitFloat32(_b(<int>[0, 0, 0]), numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodeByteStreamSplitFloat64(_b(<int>[0, 0, 0, 0]), numValues: 1),
        throwsA(isA<ParquetEofException>()),
      );
      expect(
        () => decodeByteStreamSplitFixedLenByteArray(
          Bytes.empty,
          numValues: 1,
          typeLength: 0,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
      expect(
        () => decodeByteStreamSplitFixedLenByteArray(
          _b(<int>[1, 2]),
          numValues: 1,
          typeLength: 3,
        ),
        throwsA(isA<ParquetEofException>()),
      );
    });
  });

  group('encoding dispatcher', () {
    test('encoding classification', () {
      expect(isDictionaryEncoding(Encoding.plainDictionary), isTrue);
      expect(isDictionaryEncoding(Encoding.rleDictionary), isTrue);
      expect(isDictionaryEncoding(Encoding.plain), isFalse);
      expect(isByteArrayDeltaEncoding(Encoding.deltaLengthByteArray), isTrue);
      expect(isByteArrayDeltaEncoding(Encoding.deltaByteArray), isTrue);
      expect(isByteArrayDeltaEncoding(Encoding.deltaBinaryPacked), isFalse);
      for (final encoding in Encoding.values) {
        expect(
          isSupportedEncoding(encoding),
          encoding != Encoding.alp,
          reason: '$encoding',
        );
      }
    });

    test('validateEncodingForPhysicalType', () {
      expect(
        () =>
            validateEncodingForPhysicalType(Encoding.plain, PhysicalType.int32),
        returnsNormally,
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.deltaBinaryPacked,
          PhysicalType.int32,
        ),
        returnsNormally,
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.deltaBinaryPacked,
          PhysicalType.byteArray,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.rleDictionary,
          PhysicalType.boolean,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.byteStreamSplit,
          PhysicalType.int32,
        ),
        returnsNormally,
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.byteStreamSplit,
          PhysicalType.int64,
        ),
        returnsNormally,
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.byteStreamSplit,
          PhysicalType.boolean,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => validateEncodingForPhysicalType(
          Encoding.byteStreamSplit,
          PhysicalType.fixedLenByteArray,
        ),
        returnsNormally,
      );
      expect(
        () => validateEncodingForPhysicalType(Encoding.rle, PhysicalType.int32),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('decodePlainValues dispatches by physical type', () {
      final ints = _plainInt32Bytes(<int>[1, -2, 3]);
      expect(
        decodePlainValues(
          _u8Bytes(ints),
          physicalType: PhysicalType.int32,
          numValues: 3,
        ),
        isA<Int32List>(),
      );
      expect(
        decodePlainValues(
          _u8Bytes(_plainBooleanBytes(<bool>[true, false])),
          physicalType: PhysicalType.boolean,
          numValues: 2,
        ),
        <int>[1, 0],
      );
      expect(
        decodePlainValues(
          _u8Bytes(_plainByteArrayBytes(<List<int>>['ab'.codeUnits, <int>[]])),
          physicalType: PhysicalType.byteArray,
          numValues: 2,
        ),
        isA<ByteArrayData>(),
      );
      expect(
        () => decodePlainValues(
          Bytes.empty,
          physicalType: PhysicalType.fixedLenByteArray,
          numValues: 0,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('decodeByteStreamSplitValues dispatches by physical type', () {
      expect(
        decodeByteStreamSplitValues(
          _u8Bytes(_bssFloat32Bytes(<double>[1.5])),
          physicalType: PhysicalType.float,
          numValues: 1,
        ),
        isA<Float32List>(),
      );
      expect(
        decodeByteStreamSplitValues(
          _u8Bytes(_bssFloat64Bytes(<double>[1.5])),
          physicalType: PhysicalType.double,
          numValues: 1,
        ),
        isA<Float64List>(),
      );
      expect(
        decodeByteStreamSplitValues(
          _u8Bytes(_bssInt32Bytes(<int>[-1, 2])),
          physicalType: PhysicalType.int32,
          numValues: 2,
        ),
        isA<Int32List>(),
      );
      expect(
        decodeByteStreamSplitValues(
          _u8Bytes(_bssInt64Bytes(<BigInt>[BigInt.one, BigInt.two])),
          physicalType: PhysicalType.int64,
          numValues: 2,
        ),
        isA<Int64Buffer>(),
      );
      expect(
        () => decodeByteStreamSplitValues(
          Bytes.empty,
          physicalType: PhysicalType.boolean,
          numValues: 0,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('delta dispatchers', () {
      expect(
        decodeDeltaBinaryPackedValues(
          _u8Bytes(_DeltaEncoder().encodeInt32(<int>[1, 2, 3])),
          physicalType: PhysicalType.int32,
        ),
        <int>[1, 2, 3],
      );
      expect(
        decodeDeltaBinaryPackedValues(
          _u8Bytes(_DeltaEncoder().encodeInt64(<BigInt>[BigInt.one])),
          physicalType: PhysicalType.int64,
        ),
        isA<Int64Buffer>(),
      );
      expect(
        () => decodeDeltaBinaryPackedValues(
          Bytes.empty,
          physicalType: PhysicalType.byteArray,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );

      final byteArray = decodeDeltaByteArrayValues(
        _hex(_sHex),
        encoding: Encoding.deltaLengthByteArray,
        expectedCount: 20,
      );
      expect(byteArray.numValues, 20);
      expect(
        () => decodeDeltaByteArrayValues(Bytes.empty, encoding: Encoding.plain),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('dictionary index dispatch', () {
      final encoded = _RleEncoder(1)..bitPacked(<int>[0, 1, 1, 0]);
      final data = _b(<int>[1, ...encoded.take()]);
      expect(decodeDictionaryIndexValues(data, numValues: 4), <int>[
        0,
        1,
        1,
        0,
      ]);
    });
  });
}
