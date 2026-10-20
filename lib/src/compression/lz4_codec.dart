/// The LZ4_RAW and legacy LZ4 Parquet codecs.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:dart_lz4/dart_lz4.dart' as lz4;

import '../format/enums.dart';
import '../util/errors.dart';
import 'codec.dart';
import 'codec_util.dart';
import 'hadoop_block_stream.dart';

/// LZ4 block codec for the `LZ4_RAW` Parquet compression code (7).
///
/// The page body is one raw LZ4 block; the block format stores no output size,
/// so the page's `uncompressed_page_size` is required and passed straight to
/// the block decoder, which bounds its output buffer with it.
final class Lz4RawCodec implements CompressionCodec {
  const Lz4RawCodec();

  @override
  int get codecId => CompressionType.lz4Raw.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    return guardDecode('lz4_raw', () {
      final out = lz4.lz4Decompress(
        input.asUint8List(),
        decompressedSize: uncompressedSize,
        maxDecompressedSize: uncompressedSize,
      );
      checkDecodedLength('lz4_raw', out.length, uncompressedSize);
      return ownBytes(out);
    });
  }

  @override
  Bytes encode(Bytes input) => guardEncode(
    'lz4_raw',
    () => ownBytes(lz4.lz4Compress(input.asUint8List())),
  );
}

/// Legacy `LZ4` Parquet codec (code 5).
///
/// Historically this codec meant different things to different writers:
///
///  * parquet-mr wrapped blocks in Hadoop's `BlockCompressorStream` framing;
///  * some writers wrote a bare LZ4 block (identical to `LZ4_RAW`).
///
/// [decode] therefore tries the Hadoop framing first (one or more compressed
/// chunks per block) and falls back to a single raw LZ4 block on any structural
/// or decompression failure. [encode] emits a raw block, matching the most
/// portable of the two representations; v0.1.0 is read-only anyway.
final class Lz4LegacyCodec implements CompressionCodec {
  const Lz4LegacyCodec();

  @override
  int get codecId => CompressionType.lz4.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    return guardDecode('lz4', () {
      Exception? framedError;
      if (hasHadoopBlockHeader(input, uncompressedSize)) {
        try {
          return decodeHadoopBlockStream(input, uncompressedSize, _decodeChunk);
        } on Exception catch (e) {
          framedError = e;
        }
      }
      try {
        final out = lz4.lz4Decompress(
          input.asUint8List(),
          decompressedSize: uncompressedSize,
          maxDecompressedSize: uncompressedSize,
        );
        checkDecodedLength('lz4', out.length, uncompressedSize);
        return ownBytes(out);
      } on Exception catch (e, stackTrace) {
        throw ParquetFormatException(
          framedError == null
              ? 'LZ4 decompression failed: $e'
              : 'LZ4 decompression failed (Hadoop framing: $framedError; '
                    'raw block: $e)',
          e,
          stackTrace,
        );
      }
    });
  }

  @override
  Bytes encode(Bytes input) => const Lz4RawCodec().encode(input);

  static int _decodeChunk(Bytes chunk, Uint8List destination) =>
      lz4.lz4DecompressInto(chunk.asUint8List(), destination);
}
