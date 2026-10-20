/// The ZSTD Parquet codec (Zstandard frames).
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:zstd_neo/zstd_neo.dart' as zstd;

import '../format/enums.dart';
import 'codec.dart';
import 'codec_util.dart';

/// Zstandard codec for Parquet pages (codec 6).
///
/// A Parquet page body is a single zstd frame. Decompression is bounded by the
/// page's `uncompressed_page_size` through [zstd.ZstdCodec.maxOutputSize]; the
/// decoder also rejects frames whose declared window is implausibly large.
final class ZstdCodec implements CompressionCodec {
  const ZstdCodec({this.level = 3});

  /// Compression level ([encode] only), 1..19.
  final int level;

  @override
  int get codecId => CompressionType.zstd.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    return guardDecode('zstd', () {
      final codec = zstd.ZstdCodec(maxOutputSize: uncompressedSize);
      final out = codec.decompress(input.asUint8List());
      checkDecodedLength('zstd', out.length, uncompressedSize);
      return ownBytes(out);
    });
  }

  @override
  Bytes encode(Bytes input) => guardEncode('zstd', () {
    RangeError.checkValueInInterval(level, 1, 19, 'level');
    final Uint8List out = zstd.ZstdCodec(level: level)
        .compress(input.asUint8List());
    return ownBytes(out);
  });
}
