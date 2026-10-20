import 'dart:convert';
import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:dart_lz4/dart_lz4.dart' as lz4;
import 'package:lzo_neo/lzo_neo.dart' as lzo;
import 'package:parquet/src/compression/compression.dart';
import 'package:parquet/src/format/enums.dart';
import 'package:parquet/src/util/errors.dart';
import 'package:test/test.dart';

Uint8List fromHex(String hex) {
  final clean = hex.replaceAll(' ', '');
  final out = Uint8List(clean.length ~/ 2);
  for (var i = 0; i < out.length; i++) {
    out[i] = int.parse(clean.substring(i * 2, i * 2 + 2), radix: 16);
  }
  return out;
}

/// Compressible, but not trivially so (defeats RLE-only encoders).
Uint8List periodic(int n) {
  final out = Uint8List(n);
  for (var i = 0; i < n; i++) {
    out[i] = (i * 37 + i ~/ 13) & 0xff;
  }
  return out;
}

/// Incompressible pseudo-random payload with a fixed seed.
Uint8List randomBytes(int n, [int seed = 0x12345678]) {
  final out = Uint8List(n);
  var state = seed & 0xffffffff;
  for (var i = 0; i < n; i++) {
    state = (state * 1664525 + 1013904223) & 0xffffffff;
    out[i] = (state >>> 16) & 0xff;
  }
  return out;
}

int readBe32(Uint8List data, int position) =>
    (data[position] << 24) |
    (data[position + 1] << 16) |
    (data[position + 2] << 8) |
    data[position + 3];

void writeBe32(List<int> out, int value) {
  out
    ..add((value >>> 24) & 0xff)
    ..add((value >>> 16) & 0xff)
    ..add((value >>> 8) & 0xff)
    ..add(value & 0xff);
}

/// Independent Hadoop `BlockCompressorStream` decoder for LZO, used to verify
/// that our writer produces parquet-java/hadoop-lzo compatible bytes.
Uint8List hadoopLzoDecompress(Uint8List framed, int expectedSize) {
  final codec = lzo.Lzo1x();
  final out = Uint8List(expectedSize);
  var position = 0;
  var produced = 0;
  while (position < framed.length) {
    final blockLength = readBe32(framed, position);
    position += 4;
    var blockProduced = 0;
    while (blockProduced < blockLength) {
      final chunkLength = readBe32(framed, position);
      position += 4;
      final chunk = Uint8List.sublistView(
        framed,
        position,
        position + chunkLength,
      );
      position += chunkLength;
      final destination = Uint8List.sublistView(
        out,
        produced,
        produced + blockLength - blockProduced,
      );
      final written = codec.decompressInto(chunk, destination);
      produced += written;
      blockProduced += written;
    }
  }
  if (produced != expectedSize) {
    throw StateError('hadoop-lzo produced $produced != $expectedSize');
  }
  return out;
}

/// Hadoop framing with several compressed chunks per block.
Uint8List hadoopLzoCompress(
  Uint8List raw, {
  int blockSize = 128 * 1024,
  int chunkSize = 64 * 1024,
}) {
  final codec = lzo.Lzo1x();
  final out = <int>[];
  for (var offset = 0; offset < raw.length; offset += blockSize) {
    final blockEnd = offset + blockSize < raw.length
        ? offset + blockSize
        : raw.length;
    writeBe32(out, blockEnd - offset);
    for (
      var chunkOffset = offset;
      chunkOffset < blockEnd;
      chunkOffset += chunkSize
    ) {
      final chunkEnd = chunkOffset + chunkSize < blockEnd
          ? chunkOffset + chunkSize
          : blockEnd;
      final source = Uint8List.sublistView(raw, chunkOffset, chunkEnd);
      final target = Uint8List(lzo.Lzo1x.compressBound(source.length));
      final written = codec.compressInto(source, target);
      writeBe32(out, written);
      out.addAll(Uint8List.sublistView(target, 0, written));
    }
  }
  return Uint8List.fromList(out);
}

/// Hadoop framing for LZ4 with several compressed chunks per block.
Uint8List hadoopLz4Compress(
  Uint8List raw, {
  int blockSize = 20 * 1024,
  int chunkSize = 7000,
}) {
  final out = <int>[];
  for (var offset = 0; offset < raw.length; offset += blockSize) {
    final blockEnd = offset + blockSize < raw.length
        ? offset + blockSize
        : raw.length;
    writeBe32(out, blockEnd - offset);
    for (
      var chunkOffset = offset;
      chunkOffset < blockEnd;
      chunkOffset += chunkSize
    ) {
      final chunkEnd = chunkOffset + chunkSize < blockEnd
          ? chunkOffset + chunkSize
          : blockEnd;
      final chunk = lz4.lz4Compress(
        Uint8List.sublistView(raw, chunkOffset, chunkEnd),
      );
      writeBe32(out, chunk.length);
      out.addAll(chunk);
    }
  }
  return Uint8List.fromList(out);
}

/// External liblzo2/lzokay-produced Hadoop-framed LZO block for
/// `periodic(1 << 17)`; captured in `.agents/probe/test/lzo_hadoop_test.dart`.
const String lzoHadoopReferenceBase64 =
    'AAIAAAAABYCiACVKb5S53gMoTXKXvOIHLFF2m8DlCi9UeZ7E6Q4zWH2ix+wRNluApsvwFTpfhKnO8xg9Yoit0vccQWaLsNX6H0Rqj7TZ/iNIbZK33AEmTHGWu+AFKk90mb7jCC5TeJ3C5wwxVnugxeoQNVp/pMnuEzhdgqfM8hc8YYar0PUaP2SJrtT5HkNojbLX/CFGa5C22ylCApi9KUICep8pQgJcgSlCAj5jKUICIEUpQgICJylCAuQJKUICxuspQgKozSlCAoqvKUICbJEpQgJOcylCAjBVKUICEjcpQgL0GSlCAtb7KUICuN0pQgKavylCAnyhKUICXoMpQgJAZSlCAiJHKUICBCkpQgLmCylCAsjtKUICqs8pQgKMsSlCAm6TKUICUHUpQgIyVylCAhQ5KUIC9hspQgLY/SlCArrfKUICnMEpQgJ+oylCAmCFKUICQmcpQgIkSSlCAgYrKUIC6A0pQgLK7ylCAqzRKUICjrMpQgJwlSlCAlJ3KUICNFkpQgIWOylCAvgdKUIC2v8pQgK84SlCAp7DKUICgKUpQgJihylCAkRpKUICJkspQgIILSlCAuoPKUICzPEpQgKu0ylCApC1KUACSHUoQAJIdShAAkh1KEACSHUoQAJIdShAAkh1KEACSHUoQAJIdShAAkh1KEACSHUoQAJIdShAAoyH4BKMh+ASjIfgEoyH4BKMh+ASjIfgEoyH4BKMh+ASjIfgEoyH4BKMh+AS0JmgEtCZoBLQmaAS0JmgEtCZoBLQmaAS0JmgEtCZoBLQmaAS0JmgEtCZoBIndBVgEid0FWASJ3QVYBIndBVgEid0FWASJ3QVYBIndBVgEid0FWASJ3QVYBIndBVgEid0FWASKboX4QYpuhfD6Cm6F6XKKboXh6wpuhdpjim6F0twKboXLVIpuhcPNCm6F/EWKboX0/gpuhe12iAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAAAAAAAAA5vwZIAAAAAAAAADm/BkgAAAAAAAAAOb8GSAAYPwZEQAA';

void main() {
  group('registry', () {
    test('every CompressionType has a codec with the matching id', () {
      for (final type in CompressionType.values) {
        final codec = codecFor(type);
        expect(codec.codecId, type.value, reason: '$type');
        expect(createCodec(type), same(codec));
      }
      expect(compressionCodecs.length, CompressionType.values.length);
    });

    test('codecForValue maps raw Thrift values', () {
      expect(codecForValue(0), isA<UncompressedCodec>());
      expect(codecForValue(1), isA<SnappyCodec>());
      expect(codecForValue(2), isA<GzipCodec>());
      expect(codecForValue(3), isA<LzoCodec>());
      expect(codecForValue(4), isA<BrotliCodec>());
      expect(codecForValue(5), isA<Lz4LegacyCodec>());
      expect(codecForValue(6), isA<ZstdCodec>());
      expect(codecForValue(7), isA<Lz4RawCodec>());
    });

    test('unknown codec ids are unsupported', () {
      expect(
        () => codecForValue(8),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => codecForValue(-1),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });
  });

  group('roundtrip', () {
    final payloads = <String, Uint8List Function(int)>{
      'periodic': periodic,
      'random': (n) => randomBytes(n),
    };
    const sizes = [0, 1, 7, 64, 1024, 65537];

    for (final type in CompressionType.values) {
      test('$type', () {
        final codec = codecFor(type);
        for (final entry in payloads.entries) {
          for (final size in sizes) {
            final raw = entry.value(size);
            final encoded = codec.encode(Bytes.fromUint8List(raw));
            final decoded = codec.decode(encoded, uncompressedSize: raw.length);
            expect(
              decoded.asUint8List(),
              raw,
              reason:
                  '$type ${entry.key} size=$size '
                  'compressed=${encoded.length}',
            );
          }
        }
      });
    }

    test('uncompressed codec is a pass-through view', () {
      final raw = Bytes.fromUint8List(randomBytes(128));
      const codec = UncompressedCodec();
      expect(codec.encode(raw), same(raw));
      expect(codec.decode(raw, uncompressedSize: 128), same(raw));
    });
  });

  group('known external vectors', () {
    const text = 'hello, parquet!';
    final textBytes = Uint8List.fromList(utf8.encode(text));

    test('snappy block', () {
      final vector = fromHex('0f3868656c6c6f2c207061727175657421');
      final decoded = const SnappyCodec().decode(
        Bytes.fromUint8List(vector),
        uncompressedSize: textBytes.length,
      );
      expect(decoded.asUint8List(), textBytes);
    });

    test('gzip member', () {
      final vector = fromHex(
        '1f8b08000000000000ffcb48cdc9c9d75128482c2a2c4d2d5104'
        '00556cb9e90f000000',
      );
      final decoded = const GzipCodec().decode(
        Bytes.fromUint8List(vector),
        uncompressedSize: textBytes.length,
      );
      expect(decoded.asUint8List(), textBytes);
    });

    test('brotli stream', () {
      final vector = fromHex('0b078068656c6c6f2c20706172717565742103');
      final decoded = const BrotliCodec().decode(
        Bytes.fromUint8List(vector),
        uncompressedSize: textBytes.length,
      );
      expect(decoded.asUint8List(), textBytes);
    });

    test('zstd frame', () {
      final vector = fromHex(
        '28b52ffd200f79000068656c6c6f2c207061727175657421',
      );
      final decoded = const ZstdCodec().decode(
        Bytes.fromUint8List(vector),
        uncompressedSize: textBytes.length,
      );
      expect(decoded.asUint8List(), textBytes);
    });

    test('raw lz4 block', () {
      // token 0xF0 (15 literals, extended length 0) + 15 literal bytes.
      final vector = fromHex('f00068656c6c6f2c207061727175657421');
      final decoded = const Lz4RawCodec().decode(
        Bytes.fromUint8List(vector),
        uncompressedSize: textBytes.length,
      );
      expect(decoded.asUint8List(), textBytes);
    });

    test('Hadoop-framed LZO from liblzo2/lzokay', () {
      final framed = base64.decode(lzoHadoopReferenceBase64);
      final raw = periodic(1 << 17);
      final decoded = LzoCodec().decode(
        Bytes.fromUint8List(framed),
        uncompressedSize: raw.length,
      );
      expect(decoded.asUint8List(), raw);
    });
  });

  group('hadoop block framing', () {
    test('LZO writer produces framing hadoop-lzo can read', () {
      final raw = periodic(300000);
      final encoded = LzoCodec().encode(Bytes.fromUint8List(raw));

      // First block header is a big-endian uncompressed block length, capped at
      // the encoder's 128 KiB block size.
      expect(readBe32(encoded.asUint8List(), 0), LzoCodec.encodeBlockSize);

      final decoded = hadoopLzoDecompress(encoded.asUint8List(), raw.length);
      expect(decoded, raw);
    });

    test('LZO decoder accepts multiple chunks per block', () {
      final raw = periodic(200000);
      final framed = hadoopLzoCompress(raw);
      final decoded = LzoCodec().decode(
        Bytes.fromUint8List(framed),
        uncompressedSize: raw.length,
      );
      expect(decoded.asUint8List(), raw);
    });

    test('LZO decoder falls back to a raw block', () {
      final raw = periodic(4096);
      final packed = lzo.Lzo1x().compress(Bytes.fromUint8List(raw));
      final decoded = LzoCodec().decode(packed, uncompressedSize: raw.length);
      expect(decoded.asUint8List(), raw);
    });

    test('LZ4 legacy decoder accepts Hadoop framing', () {
      final raw = periodic(100000);
      final framed = hadoopLz4Compress(raw);
      final decoded = const Lz4LegacyCodec().decode(
        Bytes.fromUint8List(framed),
        uncompressedSize: raw.length,
      );
      expect(decoded.asUint8List(), raw);
    });

    test('LZ4 legacy decoder falls back to a raw block', () {
      final raw = randomBytes(5000);
      final packed = lz4.lz4Compress(raw);
      final decoded = const Lz4LegacyCodec().decode(
        Bytes.fromUint8List(packed),
        uncompressedSize: raw.length,
      );
      expect(decoded.asUint8List(), raw);
    });

    test('LZ4 legacy encode roundtrips (raw block)', () {
      final raw = periodic(5000);
      final codec = codecFor(CompressionType.lz4);
      final encoded = codec.encode(Bytes.fromUint8List(raw));
      final decoded = codec.decode(encoded, uncompressedSize: raw.length);
      expect(decoded.asUint8List(), raw);
    });
  });

  group('bounds and malformed input', () {
    test('negative uncompressed size is rejected by every codec', () {
      final body = Bytes.fromUint8List(Uint8List(4));
      for (final type in CompressionType.values) {
        expect(
          () => codecFor(type).decode(body, uncompressedSize: -1),
          throwsA(isA<ParquetFormatException>()),
          reason: '$type',
        );
      }
    });

    test('uncompressed size mismatch is rejected', () {
      final body = Bytes.fromUint8List(Uint8List(10));
      expect(
        () => const UncompressedCodec().decode(body, uncompressedSize: 9),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('snappy declared length is checked before decompressing', () {
      // Varint 100000 followed by no body: must fail on the length check and
      // must not allocate a 100000-byte output buffer.
      final body = Bytes.fromUint8List(fromHex('a08d06'));
      expect(
        () => const SnappyCodec().decode(body, uncompressedSize: 10),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('snappy truncated block is a format error', () {
      // Declares 100 bytes, then a literal tag promising more than is present.
      final body = Bytes.fromUint8List(fromHex('6410aabb'));
      expect(
        () => const SnappyCodec().decode(body, uncompressedSize: 100),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    for (final type in const [
      CompressionType.snappy,
      CompressionType.gzip,
      CompressionType.brotli,
      CompressionType.zstd,
      CompressionType.lz4Raw,
    ]) {
      test('$type rejects an output larger than uncompressedSize', () {
        final raw = periodic(4096);
        final codec = codecFor(type);
        final encoded = codec.encode(Bytes.fromUint8List(raw));
        expect(
          () => codec.decode(encoded, uncompressedSize: 100),
          throwsA(isA<ParquetFormatException>()),
          reason: '$type must not produce more than the page header allows',
        );
      });
    }

    test('lzo garbage is a format error', () {
      final body = Bytes.fromUint8List(fromHex('deadbeefdeadbeef'));
      expect(
        () => LzoCodec().decode(body, uncompressedSize: 64),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('lz4 legacy garbage is a format error', () {
      final body = Bytes.fromUint8List(fromHex('deadbeefdeadbeef'));
      expect(
        () => const Lz4LegacyCodec().decode(body, uncompressedSize: 64),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('truncated hadoop lzo framing falls back and fails cleanly', () {
      final raw = periodic(4000);
      final framed = hadoopLzoCompress(raw);
      final truncated = Uint8List.sublistView(framed, 0, framed.length - 3);
      expect(
        () => LzoCodec().decode(
          Bytes.fromUint8List(truncated),
          uncompressedSize: raw.length,
        ),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });
}
