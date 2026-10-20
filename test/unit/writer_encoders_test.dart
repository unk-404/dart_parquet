@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/encodings/dictionary.dart';
import 'package:parquet/src/encodings/plain.dart';
import 'package:parquet/src/encodings/rle.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:parquet/src/writer/encoders.dart';
import 'package:test/test.dart';

void main() {
  group('RLE / bit-packed hybrid encoder', () {
    test('round-trips every bit width with mixed runs', () {
      final patterns = <List<int>>[
        <int>[0],
        <int>[],
        <int>[1, 1, 1, 1, 1, 1, 1, 1],
        List<int>.generate(100, (i) => i % 2),
        List<int>.generate(257, (i) => i % 5),
        List<int>.generate(64, (i) => i ~/ 9),
        <int>[0, 1, 2, 3, 2, 1, 0, 1, 2, 3],
      ];
      for (var bitWidth = 1; bitWidth <= 8; bitWidth++) {
        final max = (1 << bitWidth) - 1;
        for (final raw in patterns) {
          final values = [for (final value in raw) value & max];
          final encoded = encodeRleBitPacked(values, bitWidth);
          final decoded = decodeRleBitPacked(
            Bytes.fromUint8List(encoded),
            bitWidth: bitWidth,
            numValues: values.length,
          );
          expect(decoded, values, reason: 'bits=$bitWidth values=$values');
        }
      }
    });

    test('handles zero bit width (single-entry dictionary)', () {
      final encoded = encodeRleBitPacked(List<int>.filled(37, 0), 0);
      final decoded = decodeRleBitPacked(
        Bytes.fromUint8List(encoded),
        bitWidth: 0,
        numValues: 37,
      );
      expect(decoded, List<int>.filled(37, 0));
    });

    test('emits a valid length-prefixed stream for data page v1 levels', () {
      final values = List<int>.generate(123, (i) => i % 3);
      final payload = encodeRleBitPacked(values, 2);
      final prefixed = ByteWriter()
        ..writeUint32LE(payload.length)
        ..writeUint8List(payload);
      final decoded = decodeRleBitPackedLengthPrefixed(
        prefixed.bytes,
        bitWidth: 2,
        numValues: values.length,
      );
      expect(decoded, values);
    });

    test('rejects invalid bit widths', () {
      expect(() => encodeRleBitPacked(<int>[0], -1), throwsArgumentError);
      expect(() => encodeRleBitPacked(<int>[0], 33), throwsArgumentError);
    });
  });

  group('PLAIN encoder', () {
    test('boolean', () {
      final values = <bool>[
        true,
        false,
        true,
        true,
        false,
        false,
        true,
        false,
        true,
      ];
      final encoded = encodePlainValues(values, PhysicalType.boolean);
      expect(encoded.length, 2);
      final decoded = decodePlainBoolean(
        Bytes.fromUint8List(encoded),
        numValues: values.length,
      );
      expect(decoded, [for (final value in values) value ? 1 : 0]);
    });

    test('int32 including extremes', () {
      final values = <int>[0, 1, -1, 2147483647, -2147483648, 42];
      final encoded = encodePlainValues(values, PhysicalType.int32);
      final decoded = decodePlainInt32(
        Bytes.fromUint8List(encoded),
        numValues: values.length,
      );
      expect(decoded, values);
    });

    test('int64 limbs including extremes', () {
      final values = <Int64Value>[
        const Int64Value(0, 0),
        const Int64Value(1, 0),
        const Int64Value(0xFFFFFFFF, 0xFFFFFFFF), // -1
        const Int64Value(0xFFFFFFFF, 0x7FFFFFFF), // max
        const Int64Value(0, 0x80000000), // min
        const Int64Value(0x89ABCDEF, 0x01234567),
      ];
      final encoded = encodePlainValues(values, PhysicalType.int64);
      final decoded = decodePlainInt64(
        Bytes.fromUint8List(encoded),
        numValues: values.length,
      );
      expect(decoded.length, values.length);
      for (var i = 0; i < values.length; i++) {
        final limbs = decoded.getLimbs(i);
        expect((limbs.$1, limbs.$2), (values[i].lo, values[i].hi));
      }
    });

    test('float and double', () {
      final floats = <double>[
        0.0,
        -0.0,
        1.5,
        -3.25,
        double.infinity,
        double.negativeInfinity,
      ];
      final f32 = decodePlainFloat32(
        Bytes.fromUint8List(encodePlainValues(floats, PhysicalType.float)),
        numValues: floats.length,
      );
      expect(f32, floats);

      final doubles = <double>[...floats, 1e300, -1e-300];
      final f64 = decodePlainFloat64(
        Bytes.fromUint8List(encodePlainValues(doubles, PhysicalType.double)),
        numValues: doubles.length,
      );
      expect(f64, doubles);
    });

    test('byte array including empty values', () {
      final values = <Uint8List>[
        Uint8List.fromList([]),
        Uint8List.fromList([1, 2, 3]),
        Uint8List.fromList([0, 255]),
        Uint8List.fromList(List<int>.generate(300, (i) => i & 0xFF)),
      ];
      final encoded = encodePlainValues(values, PhysicalType.byteArray);
      final decoded = decodePlainByteArray(
        Bytes.fromUint8List(encoded),
        numValues: values.length,
      );
      expect(decoded.numValues, values.length);
      for (var i = 0; i < values.length; i++) {
        expect(decoded.valueAt(i), values[i]);
      }
    });

    test('fixed length byte array', () {
      final values = <Uint8List>[
        Uint8List.fromList([1, 2, 3, 4]),
        Uint8List.fromList([5, 6, 7, 8]),
      ];
      final encoded = encodePlainValues(
        values,
        PhysicalType.fixedLenByteArray,
        typeLength: 4,
      );
      final decoded = decodePlainFixedLenByteArray(
        Bytes.fromUint8List(encoded),
        numValues: values.length,
        typeLength: 4,
      );
      expect(decoded.numValues, 2);
      for (var i = 0; i < values.length; i++) {
        expect(decoded.valueAt(i), values[i]);
      }
    });

    test('int96', () {
      final values = <Uint8List>[
        Uint8List.fromList(List<int>.generate(12, (i) => i)),
        Uint8List.fromList(List<int>.generate(12, (i) => 255 - i)),
      ];
      final encoded = encodePlainValues(values, PhysicalType.int96);
      final decoded = decodePlainInt96(
        Bytes.fromUint8List(encoded),
        numValues: values.length,
      );
      for (var i = 0; i < values.length; i++) {
        expect(decoded.valueAt(i), values[i]);
      }
    });

    test('fixed length values must match the declared width', () {
      expect(
        () => encodePlainValues(
          <Uint8List>[Uint8List(3)],
          PhysicalType.fixedLenByteArray,
          typeLength: 4,
        ),
        throwsArgumentError,
      );
    });
  });

  group('dictionary indices', () {
    test('round-trip through the reader decoder', () {
      for (final count in <int>[0, 1, 2, 7, 8, 9, 100, 1000]) {
        final indices = <int>[for (var i = 0; i < count; i++) (i * 7) % 13];
        final bitWidth = dictionaryBitWidth(12);
        final encoded = encodeDictionaryIndices(indices, bitWidth);
        final decoded = decodeDictionaryIndices(
          Bytes.fromUint8List(encoded),
          numValues: count,
        );
        expect(decoded, indices, reason: 'count=$count');
      }
    });

    test('bit width zero for a single-entry dictionary', () {
      final encoded = encodeDictionaryIndices(List<int>.filled(10, 0), 0);
      final decoded = decodeDictionaryIndices(
        Bytes.fromUint8List(encoded),
        numValues: 10,
      );
      expect(decoded, List<int>.filled(10, 0));
    });
  });

  group('statistics value encoding', () {
    test('raw PLAIN bytes without a byte-array length prefix', () {
      expect(encodeStatisticsValue(0x01020304, PhysicalType.int32), [
        4,
        3,
        2,
        1,
      ]);
      expect(
        encodeStatisticsValue(
          const Int64Value(0x05060708, 0x01020304),
          PhysicalType.int64,
        ),
        [8, 7, 6, 5, 4, 3, 2, 1],
      );
      expect(encodeStatisticsValue(true, PhysicalType.boolean), [1]);
      expect(
        encodeStatisticsValue(
          Uint8List.fromList([9, 8]),
          PhysicalType.byteArray,
        ),
        [9, 8],
      );
    });

    test('INT96 statistics are rejected', () {
      expect(
        () => encodeStatisticsValue(Uint8List(12), PhysicalType.int96),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('plain value size matches the encoded length', () {
      expect(plainValueSize(true, PhysicalType.boolean), 1);
      expect(plainValueSize(0, PhysicalType.int32), 4);
      expect(plainValueSize(const Int64Value(0, 0), PhysicalType.int64), 8);
      expect(plainValueSize(0.0, PhysicalType.double), 8);
      expect(plainValueSize(Uint8List(5), PhysicalType.byteArray), 9);
      expect(plainValueSize(Uint8List(5), PhysicalType.fixedLenByteArray), 5);
    });
  });
}
