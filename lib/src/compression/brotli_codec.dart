/// The BROTLI Parquet codec (RFC 7932).
library;

import 'package:blit/blit.dart' show Bytes;
import 'package:brotli_neo/compat.dart' as brotli;

import '../format/enums.dart';
import 'codec.dart';
import 'codec_util.dart';

/// Brotli codec for Parquet pages (codec 4).
///
/// Decompression is bounded by the page's `uncompressed_page_size` through
/// [brotli.BrotliDecodeOptions.maxOutputSize]. Quality 5 is the default for
/// [encode]: the recon notes in `.agents/notes/codec-evaluation.md` measured
/// 239 MiB/s at q5 versus 0.9 MiB/s at q11, and Parquet page sizes are small
/// enough that the ratio difference rarely pays for it.
final class BrotliCodec implements CompressionCodec {
  const BrotliCodec({this.quality = 5});

  /// Brotli encoder quality, 0..9. (10 and 11 are supported by the codec but
  /// deliberately excluded: they are two orders of magnitude slower.)
  final int quality;

  @override
  int get codecId => CompressionType.brotli.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    return guardDecode('brotli', () {
      final out = brotli.brotliDecompress(
        input.asUint8List(),
        options: brotli.BrotliDecodeOptions(maxOutputSize: uncompressedSize),
      );
      checkDecodedLength('brotli', out.length, uncompressedSize);
      return ownBytes(out);
    });
  }

  @override
  Bytes encode(Bytes input) => guardEncode('brotli', () {
    RangeError.checkValueInInterval(quality, 0, 9, 'quality');
    final out = brotli.brotliCompress(
      input.asUint8List(),
      options: brotli.BrotliEncodeOptions(
        quality: quality,
        sizeHint: input.length,
      ),
    );
    return ownBytes(out);
  });
}
