import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/src/util/errors.dart';
import 'package:parquet/src/variant/variant.dart';
import 'package:test/test.dart';

Uint8List u8(List<int> values) => Uint8List.fromList(values);

Uint8List le64(int value) {
  final bytes = Uint8List(8);
  final big = BigInt.from(value);
  final modulus = BigInt.one << 64;
  var remaining = big % modulus;
  for (var i = 0; i < 8; i++) {
    bytes[i] = (remaining & BigInt.from(0xFF)).toInt();
    remaining >>= 8;
  }
  return bytes;
}

VariantValue decodeValue(Uint8List value, [VariantMetadata? metadata]) =>
    VariantCodec.decode(value, metadata ?? VariantMetadata.empty());

Set<String> fieldNames(VariantValue value) {
  final names = <String>{};
  value.collectFieldNames(names);
  return names;
}

void expectRoundTrip(VariantValue value) {
  final metadata = VariantMetadata.fromStrings(fieldNames(value));
  final bytes = VariantCodec.encode(value, metadata);
  final decoded = VariantCodec.decode(bytes, metadata);
  expect(decoded, value);
  expect(decoded.toJson(), value.toJson());
  expect(VariantCodec.encode(decoded, metadata), bytes);
}

void main() {
  group('Variant metadata', () {
    test('decodes the canonical empty metadata', () {
      final metadata = VariantMetadata.fromBytes([0x01, 0x00, 0x00]);
      expect(metadata.version, 1);
      expect(metadata.isSorted, isFalse);
      expect(metadata.length, 0);
      expect(metadata.isEmpty, isTrue);
      expect(metadata.toBytes(), [0x01, 0x00, 0x00]);
    });

    test('decodes and re-encodes a sorted dictionary', () {
      final bytes = u8([
        0x11, // version 1, sorted, 1-byte offsets
        3, // dictionary size
        0, 6, 9, 13, // offsets
        97, 99, 116, 105, 118, 101, // active
        97, 103, 101, // age
        110, 97, 109, 101, // name
      ]);
      final metadata = VariantMetadata.fromBytes(bytes);
      expect(metadata.isSorted, isTrue);
      expect(metadata.strings, ['active', 'age', 'name']);
      expect(metadata.idOf('active'), 0);
      expect(metadata.idOf('age'), 1);
      expect(metadata.idOf('name'), 2);
      expect(metadata.idOf('missing'), isNull);
      expect(metadata.toBytes(), bytes);
    });

    test('chooses the smallest offset size', () {
      final small = VariantMetadata.fromStrings(['b', 'a']);
      expect(small.strings, ['a', 'b']);
      expect((small.toBytes()[0] >> 6) & 0x03, 0);

      final mediumNames = <String>[
        for (var i = 0; i < 200; i++) 'field_${i.toString().padLeft(3, '0')}',
      ];
      final medium = VariantMetadata.fromStrings(mediumNames);
      final mediumBytes = medium.toBytes();
      expect((mediumBytes[0] >> 6) & 0x03, 1); // two-byte offsets
      expect(VariantMetadata.fromBytes(mediumBytes).strings, mediumNames);

      final longName = 'x' * 70000;
      final large = VariantMetadata.fromStrings([longName]);
      final largeBytes = large.toBytes();
      expect((largeBytes[0] >> 6) & 0x03, 2); // three-byte offsets
      expect(VariantMetadata.fromBytes(largeBytes).strings, [longName]);
    });

    test('preserves unsorted metadata with duplicate entries', () {
      final metadata = VariantMetadata.fromStrings([
        'b',
        'a',
        'a',
      ], sort: false);
      expect(metadata.isSorted, isFalse);
      expect(metadata.strings, ['b', 'a', 'a']);
      expect(metadata.idOf('a'), 1);
      expect(metadata.idOf('b'), 0);
      expect(metadata.idOf('c'), isNull);
      expect(VariantMetadata.fromBytes(metadata.toBytes()), metadata);
    });

    test('sorts field names by UTF-8 bytes, not UTF-16 code units', () {
      final metadata = VariantMetadata.fromStrings(['😀', '\uE000', 'a', 'A']);
      expect(metadata.strings, ['A', 'a', '\uE000', '😀']);
      expect(compareVariantStrings('\uE000', '😀'), lessThan(0));
      // Dart's String.compareTo puts the surrogate pair first; the spec does
      // not.
      expect('\uE000'.compareTo('😀'), greaterThan(0));
    });

    test('rejects malformed metadata', () {
      for (final bytes in <List<int>>[
        <int>[],
        [1],
        [1, 0],
        [1, 1, 0],
        [1, 1, 0, 2, 0x61],
        [1, 2, 0, 2, 1, 0x61, 0x62],
        [0x11, 2, 0, 1, 2, 0x62, 0x61],
        [1, 1, 1, 1, 0x61],
        [1, 1, 0, 1, 0xFF],
      ]) {
        expect(
          () => VariantMetadata.fromBytes(bytes),
          throwsA(isA<ParquetException>()),
          reason: 'bytes: $bytes',
        );
      }
    });

    test('rejects newer metadata versions', () {
      expect(
        () => VariantMetadata.fromBytes([0x02, 0x00, 0x00]),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => VariantMetadata.fromBytes([0x00, 0x00, 0x00]),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('matches Quoted empty metadata for Variant.fromJson({})', () {
      final variant = Variant.fromJson(<String, Object?>{});
      expect(variant.metadataBytes, [0x01, 0x00, 0x00]);
      expect(variant.valueBytes, [0x02, 0x00, 0x00]);
    });
  });

  group('Variant primitive values', () {
    test('decodes and encodes known wire vectors', () {
      final vectors = <(List<int>, VariantValue)>[
        ([0x00], const VariantNull()),
        ([0x04], const VariantBoolean(true)),
        ([0x08], const VariantBoolean(false)),
        ([0x0C, 0x2A], VariantInt(42, VariantIntWidth.int8)),
        ([0x0C, 0xFF], VariantInt(-1, VariantIntWidth.int8)),
        ([0x10, 0xD2, 0x04], VariantInt(1234, VariantIntWidth.int16)),
        ([0x10, 0xFE, 0xFF], VariantInt(-2, VariantIntWidth.int16)),
        (
          [0x14, 0x40, 0xE2, 0x01, 0x00],
          VariantInt(123456, VariantIntWidth.int32),
        ),
        (
          [0x18, 0x15, 0x81, 0xE9, 0x7D, 0xF4, 0x10, 0x22, 0x11],
          VariantInt(1234567890123456789, VariantIntWidth.int64),
        ),
        (
          [0x1C, 0xC9, 0xE5, 0x87, 0xB4, 0x80, 0x65, 0xD2, 0x41],
          const VariantDouble(1234567890.1234),
        ),
        ([0x38, 0x06, 0x2C, 0x93, 0x4E], const VariantFloat(1234567936.0)),
        (
          [0x20, 0x02, 0xD2, 0x04, 0x00, 0x00],
          VariantDecimal(
            BigInt.from(1234),
            scale: 2,
            width: VariantDecimalWidth.decimal4,
          ),
        ),
        (
          [0x24, 0x02, 0xD2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00],
          VariantDecimal(
            BigInt.from(1234567890),
            scale: 2,
            width: VariantDecimalWidth.decimal8,
          ),
        ),
        (
          [
            0x28,
            0x02,
            0xD2,
            0xB6,
            0x23,
            0xC0,
            0xF4,
            0x10,
            0x22,
            0x11,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
          ],
          VariantDecimal(
            BigInt.parse('1234567891234567890'),
            scale: 2,
            width: VariantDecimalWidth.decimal16,
          ),
        ),
        ([0x2C, 0xE2, 0x4E, 0x00, 0x00], VariantDate(20194)),
        (
          [0x30, 0xE0, 0x52, 0x97, 0xDD, 0xE7, 0x32, 0x06, 0x00],
          VariantTimestamp.micros(1744821296780000),
        ),
        (
          [0x34, 0xE0, 0x52, 0x97, 0xDD, 0xE7, 0x32, 0x06, 0x00],
          VariantTimestamp.micros(1744821296780000, isAdjustedToUtc: false),
        ),
        (
          [0x48, 0x15, 0x41, 0xA2, 0x5A, 0x36, 0xA2, 0x5B, 0x18],
          VariantTimestamp.nanos(1755174834123456789),
        ),
        (
          [0x4C, 0x15, 0x41, 0xA2, 0x5A, 0x36, 0xA2, 0x5B, 0x18],
          VariantTimestamp.nanos(1755174834123456789, isAdjustedToUtc: false),
        ),
        ([0x44, ...le64(45296000004)], VariantTime(45296000004)),
        ([0x15, 0x68, 0x65, 0x6C, 0x6C, 0x6F], const VariantString('hello')),
        (
          [0x3C, 0x04, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF],
          VariantBinary([0xDE, 0xAD, 0xBE, 0xEF]),
        ),
        (
          [
            0x50,
            0xF2,
            0x4F,
            0x9B,
            0x64,
            0x81,
            0xFA,
            0x49,
            0xD1,
            0xB7,
            0x4E,
            0x8C,
            0x09,
            0xA6,
            0xE3,
            0x1C,
            0x56,
          ],
          VariantUuid.fromString('f24f9b64-81fa-49d1-b74e-8c09a6e31c56'),
        ),
      ];

      for (final (bytes, expected) in vectors) {
        final decoded = decodeValue(u8(bytes));
        expect(decoded, expected, reason: 'decoded $bytes');
        expect(VariantCodec.encode(decoded, VariantMetadata.empty()), bytes);
      }
    });

    test('handles short and long strings', () {
      expect(
        VariantCodec.encode(const VariantString(''), VariantMetadata.empty()),
        [0x01],
      );
      expect(
        VariantCodec.encode(
          VariantString('x' * 63),
          VariantMetadata.empty(),
        ).first,
        (63 << 2) | 1,
      );
      final longString = 'x' * 64;
      final longBytes = VariantCodec.encode(
        VariantString(longString),
        VariantMetadata.empty(),
      );
      expect(longBytes.first, 0x40);
      expect(decodeValue(longBytes), VariantString(longString));
    });

    test('JSON conversion of typed values', () {
      expect(VariantDate(20194).toJson(), '2025-04-16');
      expect(VariantTime(45296000004).toJson(), '12:34:56.000004');
      expect(
        VariantTimestamp.micros(1744821296780000).toJson(),
        '2025-04-16T16:34:56.780000Z',
      );
      expect(
        VariantTimestamp.micros(
          1744821296780000,
          isAdjustedToUtc: false,
        ).toJson(),
        '2025-04-16T16:34:56.780000',
      );
      expect(
        VariantTimestamp.nanos(1755174834123456789).toJson(),
        '2025-08-14T12:33:54.123456789Z',
      );
      expect(
        VariantTimestamp.nanos(-1).toJson(),
        '1969-12-31T23:59:59.999999999Z',
      );
      expect(VariantDecimal.fromInt(1234, scale: 2).toJson(), '12.34');
      expect(VariantDecimal.fromInt(-1234, scale: 2).toJson(), '-12.34');
      expect(VariantDecimal.fromInt(123, scale: 5).toJson(), '0.00123');
      expect(VariantDecimal.fromInt(0, scale: 2).toJson(), '0.00');
      expect(VariantBinary([0xDE, 0xAD]).toJson(), base64Encode([0xDE, 0xAD]));
      expect(
        VariantUuid.fromString('f24f9b64-81fa-49d1-b74e-8c09a6e31c56').toJson(),
        'f24f9b64-81fa-49d1-b74e-8c09a6e31c56',
      );
    });

    test('rejects invalid primitive payloads', () {
      final malformed = <List<int>>[
        <int>[],
        [0x0C],
        [0x10, 0x01],
        [0x14, 0x01, 0x02],
        [0x1C, 0, 0, 0, 0],
        [0x15, 0x61],
        [0x40, 5, 0, 0, 0],
        [0x3C, 2, 0, 0, 0, 0x01],
        [0x50, ...List<int>.filled(15, 0)],
        [0x05, 0xFF],
        [0x40, 1, 0, 0, 0, 0xFF],
        [0x20, 10, 0, 0, 0, 0],
        [0x20, 0, 0x00, 0xCA, 0x9A, 0x3B],
        [0x44, ...le64(86400000000)],
        [0x00, 0x00],
        [0x54], // primitive type 21 (unknown)
      ];
      for (final bytes in malformed) {
        expect(
          () => decodeValue(u8(bytes)),
          throwsA(isA<ParquetException>()),
          reason: 'bytes: $bytes',
        );
      }
    });

    test('round-trips all primitive types', () {
      final values = <VariantValue>[
        const VariantNull(),
        const VariantBoolean(true),
        const VariantBoolean(false),
        VariantInt(-128, VariantIntWidth.int8),
        VariantInt(127, VariantIntWidth.int8),
        VariantInt(-32768, VariantIntWidth.int16),
        VariantInt(32767, VariantIntWidth.int16),
        VariantInt(-2147483648, VariantIntWidth.int32),
        VariantInt(2147483647, VariantIntWidth.int32),
        VariantInt(-9223372036854775808, VariantIntWidth.int64),
        VariantInt(9223372036854775807, VariantIntWidth.int64),
        const VariantDouble(1234567890.1234),
        const VariantFloat(1.25),
        const VariantFloat16(1.5),
        VariantDecimal.fromInt(999999999, scale: 0),
        VariantDecimal.fromInt(-1234567890, scale: 4),
        VariantDecimal(
          BigInt.parse('12345678901234567890123456789012345678'),
          scale: 10,
        ),
        VariantDecimal(
          BigInt.parse('-12345678901234567890123456789012345678'),
          scale: 10,
        ),
        VariantDate(0),
        VariantDate(20194),
        VariantDate(-1),
        VariantTime(0),
        VariantTime(86399999999),
        VariantTimestamp.micros(0),
        VariantTimestamp.micros(-1),
        VariantTimestamp.micros(1744821296780000, isAdjustedToUtc: false),
        VariantTimestamp.nanos(1755174834123456789),
        VariantTimestamp.nanos(-1, isAdjustedToUtc: false),
        VariantBinary(const []),
        VariantBinary([0, 1, 254, 255]),
        const VariantString(''),
        const VariantString('hello'),
        VariantString('x' * 64),
        const VariantString('héllo 🎉'),
        VariantUuid.fromString('f24f9b64-81fa-49d1-b74e-8c09a6e31c56'),
      ];

      for (final value in values) {
        if (value is VariantFloat16) {
          final encoded = VariantCodec.encode(value, VariantMetadata.empty());
          expect(encoded.first, 14 << 2); // widened to FLOAT on the wire
          final decoded = VariantCodec.decode(encoded, VariantMetadata.empty());
          expect(decoded, isA<VariantFloat>());
          expect((decoded as VariantFloat).value, value.value);
          continue;
        }
        expectRoundTrip(value);
      }
    });
  });

  group('Variant containers', () {
    final metadataBytes = u8([
      0x11,
      3,
      0,
      6,
      9,
      13,
      97,
      99,
      116,
      105,
      118,
      101,
      97,
      103,
      101,
      110,
      97,
      109,
      101,
    ]);
    final metadata = VariantMetadata.fromBytes(metadataBytes);
    final objectBytes = u8([
      0x02, 3, 0, 1, 2, 0, 1, 3, 9,
      0x04, // true
      0x0C, 42, // int8 42
      0x15, 104, 101, 108, 108, 111, // "hello"
    ]);

    test('decodes and encodes object field-id form', () {
      final decoded = VariantCodec.decode(objectBytes, metadata);
      expect(decoded, isA<VariantObject>());
      final object = decoded as VariantObject;
      expect(object.fields.keys, ['active', 'age', 'name']);
      expect(object['active'], const VariantBoolean(true));
      expect(object['age'], VariantInt(42, VariantIntWidth.int8));
      expect(object['name'], const VariantString('hello'));
      expect(decoded.toJson(), {'active': true, 'age': 42, 'name': 'hello'});
      expect(VariantCodec.encode(decoded, metadata), objectBytes);
    });

    test('encodes objects with fields in UTF-8 name order', () {
      final object = VariantObject({
        'name': const VariantString('hello'),
        'active': const VariantBoolean(true),
        'age': VariantInt(42, VariantIntWidth.int8),
      });
      expect(VariantCodec.encode(object, metadata), objectBytes);
    });

    test('supports unsorted metadata and empty containers', () {
      final unsorted = VariantMetadata.fromStrings(['b', 'a'], sort: false);
      final object = VariantObject({
        'b': const VariantBoolean(true),
        'a': const VariantBoolean(false),
      });
      final bytes = VariantCodec.encode(object, unsorted);
      final decoded = VariantCodec.decode(bytes, unsorted);
      expect(decoded, object);

      expect(
        VariantCodec.encode(VariantObject.empty(), VariantMetadata.empty()),
        [0x02, 0x00, 0x00],
      );
      expect(
        VariantCodec.encode(VariantArray.empty(), VariantMetadata.empty()),
        [0x03, 0x00, 0x00],
      );
    });

    test('decodes objects whose values are stored out of order', () {
      // {"a": null, "b": 1}, with b's value first in the data section.
      final metadata = VariantMetadata.fromStrings(['a', 'b']);
      final bytes = u8([
        0x02, // object
        2, // fields
        0, 1, // field ids a, b
        2, 0, 3, // offsets: a at 2, b at 0, end 3
        0x0C, 0x01, // b = int8 1
        0x00, // a = null
      ]);
      final decoded = VariantCodec.decode(bytes, metadata);
      expect(decoded.toJson(), {'a': null, 'b': 1});
    });

    test('large arrays set is_large', () {
      final array = VariantArray(
        List<VariantValue>.filled(300, const VariantNull()),
      );
      final bytes = VariantCodec.encode(array, VariantMetadata.empty());
      expect((bytes[0] >> 2) & 0x04, 0x04);
      final decoded = VariantCodec.decode(bytes, VariantMetadata.empty());
      expect(decoded, array);
      expect((decoded as VariantArray).length, 300);
    });

    test('large objects set is_large', () {
      final fields = <String, VariantValue>{
        for (var i = 0; i < 300; i++)
          'field_${i.toString().padLeft(3, '0')}': const VariantNull(),
      };
      final object = VariantObject(fields);
      final metadata = VariantMetadata.fromStrings(fields.keys);
      final bytes = VariantCodec.encode(object, metadata);
      expect((bytes[0] >> 2) & 0x10, 0x10);
      expect(VariantCodec.decode(bytes, metadata), object);
    });

    test('uses multi-byte field ids and offsets', () {
      final names = <String>[
        for (var i = 0; i < 300; i++) 'field_${i.toString().padLeft(3, '0')}',
      ];
      final metadata = VariantMetadata.fromStrings(names);
      final object = VariantObject({'field_299': const VariantNull()});
      final bytes = VariantCodec.encode(object, metadata);
      expect(bytes, [
        0x12, // object, 2-byte field ids, 1-byte offsets
        0x01, // one field
        0x2B, 0x01, // field id 299
        0x00, 0x01, // offsets
        0x00, // null
      ]);
      expect(VariantCodec.decode(bytes, metadata), object);
    });

    test('uses multi-byte offsets for large values', () {
      final metadata = VariantMetadata.fromStrings(['a']);
      final object = VariantObject({'a': VariantString('x' * 300)});
      final objectBytes = VariantCodec.encode(object, metadata);
      expect((objectBytes[0] >> 2) & 0x03, 1); // 2-byte offsets
      expect(VariantCodec.decode(objectBytes, metadata), object);

      final array = VariantArray([VariantString('x' * 300)]);
      final arrayBytes = VariantCodec.encode(array, VariantMetadata.empty());
      expect((arrayBytes[0] >> 2) & 0x03, 1); // 2-byte offsets
      expect(VariantCodec.decode(arrayBytes, VariantMetadata.empty()), array);
    });

    test('round-trips nested JSON values', () {
      final json = <String, Object?>{
        'name': 'Ada',
        'age': 36,
        'height': 1.65,
        'active': true,
        'tags': <Object?>['engineer', null, 42],
        'nested': <String, Object?>{
          'a': 1,
          'b': <Object?>[false],
        },
      };
      final variant = Variant.fromJson(json);
      final decoded = Variant.decode(variant.metadataBytes, variant.valueBytes);
      expect(decoded.toJson(), json);
      expect(decoded.value, isA<VariantObject>());
    });

    test('uses binary values opaquely', () {
      final raw = Uint8List.fromList([for (var i = 0; i < 256; i++) 255 - i]);
      final value = VariantBinary(raw);
      final metadata = VariantMetadata.empty();
      final bytes = VariantCodec.encode(value, metadata);
      final decoded = VariantCodec.decode(bytes, metadata);
      expect(decoded, value);
      expect((decoded as VariantBinary).bytes, raw);
      expect(VariantCodec.encode(decoded, metadata), bytes);
    });

    test('rejects malformed containers', () {
      final metadata = VariantMetadata.fromStrings(['a', 'b']);
      final malformed = <(List<int>, VariantMetadata)>[
        ([0x02, 1, 0, 0, 1, 0x00], VariantMetadata.empty()), // bad field id
        ([0x02, 2, 1, 0, 0, 1, 2, 0x00, 0x00], metadata), // unsorted names
        ([0x02, 2, 0, 0, 0, 1, 2, 0x00, 0x00], metadata), // duplicate names
        ([0x02, 1, 0, 0, 2, 0x00], VariantMetadata.empty()), // offset overflow
        ([0x02, 2, 0, 1], VariantMetadata.empty()), // truncated ids
        ([0x03, 2, 0, 1, 1, 0x00, 0x00], VariantMetadata.empty()),
        ([0x03, 1, 0, 5, 0x00], VariantMetadata.empty()),
        ([0x03, 1, 0, 2, 0x00], VariantMetadata.empty()),
        ([0x03, 1, 0, 1], VariantMetadata.empty()),
        ([0x02, 0, 0, 0xFF], VariantMetadata.empty()), // empty object padding
        ([0x03, 0, 0, 0xFF], VariantMetadata.empty()), // empty array padding
      ];
      for (final (bytes, meta) in malformed) {
        expect(
          () => VariantCodec.decode(u8(bytes), meta),
          throwsA(isA<ParquetException>()),
          reason: 'bytes: $bytes',
        );
      }
    });

    test('rejects excessive nesting', () {
      VariantValue value = const VariantNull();
      for (var i = 0; i < 129; i++) {
        value = VariantArray([value]);
      }
      expect(() => Variant.fromValue(value).valueBytes, throwsArgumentError);

      var nestedBytes = u8([0x00]);
      for (var i = 0; i < 129; i++) {
        final offsetSize = nestedBytes.length <= 0xFF
            ? 1
            : nestedBytes.length <= 0xFFFF
            ? 2
            : 3;
        final builder = BytesBuilder(copy: false)
          ..addByte(((offsetSize - 1) << 2) | 3)
          ..addByte(1);
        for (final offset in [0, nestedBytes.length]) {
          var remaining = offset;
          for (var j = 0; j < offsetSize; j++) {
            builder.addByte(remaining & 0xFF);
            remaining ~/= 256;
          }
        }
        builder.add(nestedBytes);
        nestedBytes = builder.takeBytes();
      }
      expect(
        () => VariantCodec.decode(nestedBytes, VariantMetadata.empty()),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });

  group('Variant container API', () {
    test('exposes metadata and value bytes', () {
      final variant = Variant.fromJson({'a': 1});
      expect(variant.metadata.strings, ['a']);
      expect(variant.metadata.isSorted, isTrue);
      expect(
        Variant.decode(variant.metadataBytes, variant.valueBytes).toJson(),
        {'a': 1},
      );
    });

    test('requires object fields to exist in supplied metadata', () {
      final variant = Variant.fromValue(
        VariantObject({'missing': const VariantNull()}),
        metadata: VariantMetadata.empty(),
      );
      expect(() => variant.valueBytes, throwsArgumentError);
    });

    test('rejects non-JSON input', () {
      expect(() => VariantValue.fromJson(Object()), throwsArgumentError);
      expect(() => VariantValue.fromJson({1: 'x'}), throwsArgumentError);
    });

    test('fromValue builds a sorted metadata dictionary', () {
      final variant = Variant.fromValue(
        VariantObject({
          'z': const VariantNull(),
          'a': VariantArray([
            VariantObject({'m': VariantInt(1)}),
          ]),
        }),
      );
      expect(variant.metadata.strings, ['a', 'm', 'z']);
      final decoded = Variant.decode(variant.metadataBytes, variant.valueBytes);
      expect(decoded.toJson(), {
        'a': [
          {'m': 1},
        ],
        'z': null,
      });
    });

    test('reports supported specification version', () {
      expect(variantSpecificationVersion, 1);
      expect(supportsVariantSpecificationVersion(null), isTrue);
      expect(supportsVariantSpecificationVersion(1), isTrue);
      expect(supportsVariantSpecificationVersion(2), isFalse);
    });
  });

  group('Variant model validation', () {
    test('checks integer widths', () {
      expect(() => VariantInt(128, VariantIntWidth.int8), throwsArgumentError);
      expect(
        () => VariantInt(32768, VariantIntWidth.int16),
        throwsArgumentError,
      );
      expect(
        () => VariantInt(2147483648, VariantIntWidth.int32),
        throwsArgumentError,
      );
      expect(VariantInt(42).width, VariantIntWidth.int8);
      expect(VariantInt(1000).width, VariantIntWidth.int16);
      expect(VariantInt(100000).width, VariantIntWidth.int32);
      expect(VariantInt(1 << 40).width, VariantIntWidth.int64);
    });

    test('checks decimal widths and precision', () {
      expect(
        VariantDecimal.fromInt(999999999).width,
        VariantDecimalWidth.decimal4,
      );
      expect(
        VariantDecimal.fromInt(1000000000).width,
        VariantDecimalWidth.decimal8,
      );
      expect(
        VariantDecimal(BigInt.parse('10000000000000000000')).width,
        VariantDecimalWidth.decimal16,
      );
      expect(
        () => VariantDecimal(BigInt.parse('1${'0' * 38}')),
        throwsArgumentError,
      );
      expect(() => VariantDecimal.fromInt(1, scale: 39), throwsArgumentError);
      expect(
        () => VariantDecimal.fromInt(
          1,
          scale: 10,
          width: VariantDecimalWidth.decimal4,
        ),
        throwsArgumentError,
      );
    });

    test('checks time and UUID values', () {
      expect(() => VariantTime(-1), throwsArgumentError);
      expect(() => VariantTime(86400000000), throwsArgumentError);
      expect(() => VariantUuid([1, 2, 3]), throwsArgumentError);
      expect(() => VariantUuid.fromString('not-a-uuid'), throwsArgumentError);
    });
  });
}
