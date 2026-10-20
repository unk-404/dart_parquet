@TestOn('vm')
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/format/thrift.dart';
import 'package:parquet/src/writer/bloom_filter.dart';
import 'package:test/test.dart';

void main() {
  group('XXH64', () {
    test('matches the canonical test vectors', () {
      final vectors = <String, String>{
        '': 'ef46db3751d8e999',
        'a': 'd24ec4f1a98c6e5b',
        'abc': '44bc2cf5ad770999',
        'message digest': '066ed728fceeb3be',
        'abcdefghijklmnopqrstuvwxyz': 'cfe1f278fa89835c',
      };
      for (final entry in vectors.entries) {
        final hash = xxHash64(Uint8List.fromList(utf8.encode(entry.key)));
        expect(
          hash.toRadixString(16).padLeft(16, '0'),
          entry.value,
          reason: 'XXH64("${entry.key}")',
        );
      }
    });

    test('handles long inputs and seeds deterministically', () {
      final bytes = Uint8List.fromList(
        List<int>.generate(300, (i) => (i * 31 + 7) & 0xFF),
      );
      expect(xxHash64(bytes), xxHash64(bytes));
      expect(xxHash64(bytes, seed: 1), isNot(xxHash64(bytes)));
      expect(xxHash64(Uint8List(0)).toRadixString(16), 'ef46db3751d8e999');
    });

    test('rejects negative seeds', () {
      expect(() => xxHash64(Uint8List(0), seed: -1), throwsArgumentError);
    });
  });

  group('SBBF sizing', () {
    test('is a power of two and at least 32 bytes', () {
      expect(bloomFilterByteSize(numValues: 0), 32);
      expect(bloomFilterByteSize(numValues: 1), 32);
      expect(bloomFilterByteSize(numValues: 1000), 2048);
      final filter = ParquetBloomFilter.create(numValues: 1000);
      expect(filter.numBytes, 2048);
      expect(filter.blockCount, 64);
    });

    test('honours the maximum size cap', () {
      final filter = ParquetBloomFilter.create(
        numValues: 1000000,
        fpp: 0.0001,
        maxBytes: 64,
      );
      expect(filter.numBytes, 64);
      expect(bloomFilterByteSize(numValues: 1000000, maxBytes: 100), 64);
      expect(bloomFilterByteSize(numValues: 1000000, maxBytes: 32), 32);
    });

    test('rejects invalid arguments', () {
      expect(
        () => ParquetBloomFilter.create(numValues: 1, fpp: 0),
        throwsArgumentError,
      );
      expect(
        () => ParquetBloomFilter.create(numValues: 1, fpp: 1),
        throwsArgumentError,
      );
      expect(
        () => ParquetBloomFilter.create(numValues: 1, maxBytes: 16),
        throwsArgumentError,
      );
      expect(
        () => ParquetBloomFilter.fromBitset(Uint8List(16)),
        throwsArgumentError,
      );
      expect(
        () => ParquetBloomFilter.fromBitset(Uint8List(48)),
        throwsArgumentError,
      );
    });
  });

  group('SBBF membership', () {
    test('has no false negatives', () {
      final filter = ParquetBloomFilter.create(numValues: 1000);
      final values = <Uint8List>[
        for (var i = 0; i < 1000; i++)
          Uint8List.fromList(utf8.encode('value-$i')),
      ];
      for (final value in values) {
        filter.addBytes(value);
      }
      for (final value in values) {
        expect(filter.mightContainBytes(value), isTrue);
      }
    });

    test('false-positive rate is near the configured fpp', () {
      const fpp = 0.01;
      final filter = ParquetBloomFilter.create(numValues: 1000, fpp: fpp);
      for (var i = 0; i < 1000; i++) {
        filter.addBytes(Uint8List.fromList(utf8.encode('present-$i')));
      }
      var falsePositives = 0;
      const probes = 10000;
      for (var i = 0; i < probes; i++) {
        if (filter.mightContainBytes(
          Uint8List.fromList(utf8.encode('absent-$i')),
        )) {
          falsePositives++;
        }
      }
      // Generous statistical bound: observed fpp should stay below 4%.
      expect(falsePositives / probes, lessThan(0.04));
    });

    test('empty and null-equivalent values', () {
      final filter = ParquetBloomFilter.create(numValues: 4);
      expect(filter.mightContainBytes(Uint8List(0)), isFalse);
      filter.addBytes(Uint8List(0));
      expect(filter.mightContainBytes(Uint8List(0)), isTrue);
      expect(filter.mightContainBytes(Uint8List.fromList(<int>[1])), isFalse);
    });

    test('single-block filters work', () {
      final filter = ParquetBloomFilter.create(numValues: 1, maxBytes: 32);
      expect(filter.blockCount, 1);
      final value = Uint8List.fromList(utf8.encode('only'));
      filter.addBytes(value);
      expect(filter.mightContainBytes(value), isTrue);
    });
  });

  group('serialization', () {
    test('header and bitset round-trip through read()', () {
      final filter = ParquetBloomFilter.create(numValues: 500);
      for (var i = 0; i < 500; i++) {
        filter.addBytes(Uint8List.fromList(utf8.encode('x$i')));
      }
      final bytes = filter.toBytes();
      final parsed = ParquetBloomFilter.read(Bytes.fromUint8List(bytes));
      expect(parsed.numBytes, filter.numBytes);
      expect(parsed.bitset, filter.bitset);
      for (var i = 0; i < 500; i++) {
        expect(
          parsed.mightContainBytes(Uint8List.fromList(utf8.encode('x$i'))),
          isTrue,
        );
      }
    });

    test('header carries the required unions', () {
      final bytes = encodeBloomFilterHeader(2048);
      final fields = ThriftCompactReader.fromBytes(Bytes.fromUint8List(bytes))
          .readStruct();
      expect(fields[1], 2048);
      for (final id in <int>[2, 3, 4]) {
        final union = fields[id];
        expect(union, isA<Map<int, Object?>>());
        final payload = (union! as Map<int, Object?>)[1];
        expect(payload, isA<Map<int, Object?>>());
        expect((payload! as Map<int, Object?>).isEmpty, isTrue);
      }

      final parsed = ParquetBloomFilter.parseHeader(Bytes.fromUint8List(bytes));
      expect(parsed.numBytes, 2048);
      expect(parsed.headerLength, bytes.length);
    });

    test('read rejects truncated or unsupported headers', () {
      final bytes = ParquetBloomFilter.create(numValues: 10).toBytes();
      expect(
        () => ParquetBloomFilter.read(
          Bytes.fromUint8List(bytes.sublist(0, bytes.length - 5)),
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });
}
