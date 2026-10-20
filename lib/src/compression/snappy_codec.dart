/// The SNAPPY Parquet codec (raw Snappy block format).
library;

import 'package:blit/blit.dart' show Bytes;
import 'package:snappy_neo/snappy_neo.dart' as snappy;

import '../format/enums.dart';
import '../util/errors.dart';
import 'codec.dart';
import 'codec_util.dart';

/// Snappy block codec for Parquet pages (codec 1).
///
/// Parquet stores the raw Snappy block format, not the chunked framing format:
/// a varint uncompressed length followed by literals and back-references. The
/// declared length is checked before decompressing so a corrupt header cannot
/// make Snappy allocate beyond the page's `uncompressed_page_size`.
final class SnappyCodec implements CompressionCodec {
  const SnappyCodec();

  @override
  int get codecId => CompressionType.snappy.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    return guardDecode('snappy', () {
      final declared = snappy.Snappy.uncompressedLength(input.asUint8List());
      if (declared != uncompressedSize) {
        throw ParquetFormatException(
          'snappy block declares $declared uncompressed bytes, '
          'page header declares $uncompressedSize',
        );
      }
      final out = snappy.Snappy.decompress(input.asUint8List());
      checkDecodedLength('snappy', out.length, uncompressedSize);
      return ownBytes(out);
    });
  }

  @override
  Bytes encode(Bytes input) => guardEncode(
    'snappy',
    () => ownBytes(snappy.Snappy.compress(input.asUint8List())),
  );
}
