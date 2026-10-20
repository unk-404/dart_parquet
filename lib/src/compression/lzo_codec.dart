/// The LZO Parquet codec (raw LZO1X blocks and Hadoop framing).
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;
import 'package:lzo_neo/lzo_neo.dart' as lzo;

import '../format/enums.dart';
import '../util/errors.dart';
import 'codec.dart';
import 'codec_util.dart';
import 'hadoop_block_stream.dart';

/// LZO1X codec for Parquet pages (codec 3).
///
/// The Parquet spec describes LZO as a bare LZO1X block, but real parquet-java
/// files use `com.hadoop.compression.lzo.LzoCodec`, i.e. Hadoop's
/// `BlockCompressorStream`: `[u32 BE uncompressed blockLen]
/// [u32 BE compressed chunkLen][chunk]...`, where a block normally carries
/// several 64 KiB chunks.
///
/// [decode] therefore accepts both representations: Hadoop framing first, then
/// a single raw LZO1X block whose output size is the page header's
/// `uncompressed_page_size`. [encode] writes framed data in 128 KiB blocks with
/// one chunk per block, which both Hadoop and this decoder read back.
///
/// LZO is deprecated in Parquet (it requires native code in most readers); this
/// implementation exists so legacy files remain readable.
final class LzoCodec implements CompressionCodec {
  LzoCodec();

  /// A single reusable codec: [lzo.Lzo1x] keeps its compression work memory
  /// between calls. It is not reentrant, but every public method here is
  /// synchronous, so calls cannot interleave.
  final lzo.Lzo1x _lzo = lzo.Lzo1x();

  /// Block size used by [encode]; matches hadoop-lzo's default 128 KiB block
  /// (its chunks inside a block are 64 KiB, but our decoder accepts any split).
  static const int encodeBlockSize = 128 * 1024;

  @override
  int get codecId => CompressionType.lzo.value;

  @override
  Bytes decode(Bytes input, {required int uncompressedSize}) {
    checkUncompressedSize(uncompressedSize);
    if (uncompressedSize == 0 && input.isEmpty) return Bytes.empty;

    Exception? framedError;
    if (hasHadoopBlockHeader(input, uncompressedSize)) {
      try {
        return decodeHadoopBlockStream(input, uncompressedSize, _decodeChunk);
      } on Exception catch (e) {
        framedError = e;
      }
    }

    try {
      final output = Uint8List(uncompressedSize);
      final written = _lzo.decompressInto(input.asUint8List(), output);
      if (written != uncompressedSize) {
        throw ParquetFormatException(
          'lzo decoded $written bytes, page header declares $uncompressedSize',
        );
      }
      return Bytes.fromUint8List(output);
    } on Exception catch (e, stackTrace) {
      throw ParquetFormatException(
        framedError == null
            ? 'LZO decompression failed: $e'
            : 'LZO decompression failed (Hadoop framing: $framedError; '
                  'raw block: $e)',
        e,
        stackTrace,
      );
    }
  }

  @override
  Bytes encode(Bytes input) => guardEncode(
    'lzo',
    () => encodeHadoopBlockStream(
      input,
      blockSize: encodeBlockSize,
      compressChunk: _compressChunk,
    ),
  );

  int _decodeChunk(Bytes chunk, Uint8List destination) =>
      _lzo.decompressInto(chunk.asUint8List(), destination);

  Bytes _compressChunk(Bytes block) {
    final capacity = lzo.Lzo1x.compressBound(block.length);
    final output = Uint8List(capacity);
    final written = _lzo.compressInto(block.asUint8List(), output);
    return Bytes.view(output, 0, written);
  }
}
