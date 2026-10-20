/// The UNCOMPRESSED Parquet codec.
library;

import 'package:blit/blit.dart' show Bytes;

import '../format/enums.dart';
import '../util/errors.dart';
import 'codec.dart';
import 'codec_util.dart';

/// Pass-through codec for pages stored without compression (codec 0).
///
/// The page body is the decoded value: no copy is made, so the returned [Bytes]
/// is a view of [input]'s backing buffer.
final class UncompressedCodec implements CompressionCodec {
  const UncompressedCodec();

  @override
  int get codecId => CompressionType.uncompressed.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    if (input.length != uncompressedSize) {
      throw ParquetFormatException(
        'uncompressed page is ${input.length} bytes, '
        'page header declares $uncompressedSize',
      );
    }
    return input;
  }

  @override
  Bytes encode(Bytes input) => input;
}
