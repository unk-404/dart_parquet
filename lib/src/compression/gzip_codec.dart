/// The GZIP Parquet codec (RFC 1952 gzip members).
library;

import 'package:blit/blit.dart' show Bytes;
import 'package:just_deflate/just_deflate.dart' as deflate;

import '../format/enums.dart';
import 'codec.dart';
import 'codec_util.dart';

/// Gzip codec for Parquet pages (codec 2).
///
/// Parquet page bodies are ordinary gzip members; [decode] passes the page's
/// `uncompressed_page_size` as `maxOutputSize`, so the inflater never grows its
/// output buffer beyond the size the page header promises.
final class GzipCodec implements CompressionCodec {
  const GzipCodec({this.level = 6});

  /// Compression level ([encode] only), 0 (stored) to 9 (smallest).
  final int level;

  @override
  int get codecId => CompressionType.gzip.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    return guardDecode('gzip', () {
      final out = const deflate.GzipCodec().decode(
        input.asUint8List(),
        maxOutputSize: uncompressedSize,
      );
      checkDecodedLength('gzip', out.length, uncompressedSize);
      return ownBytes(out);
    });
  }

  @override
  Bytes encode(Bytes input) => guardEncode('gzip', () {
    RangeError.checkValueInInterval(level, 0, 9, 'level');
    final out = deflate.GzipCodec(level: level).encode(input.asUint8List());
    return ownBytes(out);
  });
}
