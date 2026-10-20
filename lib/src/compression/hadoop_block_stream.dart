/// Hadoop `BlockCompressorStream` framing, shared by the legacy LZ4 and LZO
/// codecs.
///
/// parquet-java maps the `LZO` codec to `com.hadoop.compression.lzo.LzoCodec`
/// and the legacy `LZ4` codec to Hadoop's block compressor as well. Both write:
///
/// ```text
/// repeat:
///   u32 BE uncompressed block length
///   repeat until the block is full:
///     u32 BE compressed chunk length
///     compressed chunk bytes
/// ```
///
/// A block may hold one or more chunks; hadoop-lzo uses several 64 KiB chunks
/// per block, while our writer emits one chunk per block.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes, BytesMut;

import '../util/errors.dart';

/// Decompresses one Hadoop-framed chunk into [destination]; returns the number
/// of bytes written.
typedef ChunkDecompressor = int Function(Bytes chunk, Uint8List destination);

/// Whether [input] plausibly starts with a Hadoop block header for a page of
/// [uncompressedSize] bytes.
///
/// This is a cheap pre-check, not a full parse: it examines the first block
/// header and first chunk header only. It lets the legacy codecs skip the
/// framed attempt (and its output allocation) when the input is obviously a raw
/// block instead.
bool hasHadoopBlockHeader(Bytes input, int uncompressedSize) {
  if (uncompressedSize <= 0 || input.length < 8) return false;
  final source = input.asUint8List();
  final blockLength = _readUint32BE(source, 0);
  if (blockLength == 0 || blockLength > uncompressedSize) return false;
  final chunkLength = _readUint32BE(source, 4);
  return chunkLength > 0 && chunkLength <= source.length - 8;
}

/// Compresses one unframed block into a compressed chunk.
typedef ChunkCompressor = Bytes Function(Bytes block);

/// Decodes a Hadoop-framed compressed stream of exactly [uncompressedSize]
/// bytes.
///
/// The output is allocated once, at [uncompressedSize]; every parsed length is
/// validated against it before use, so a corrupt header cannot trigger an
/// oversized allocation. Structural corruption throws [ParquetFormatException]
/// so callers can fall back to a raw block representation.
Bytes decodeHadoopBlockStream(
  Bytes input,
  int uncompressedSize,
  ChunkDecompressor decompressChunk,
) {
  if (uncompressedSize < 0) {
    throw ParquetFormatException(
      'negative uncompressed size: $uncompressedSize',
    );
  }
  final source = input.asUint8List();
  final output = Uint8List(uncompressedSize);
  var position = 0;
  var produced = 0;

  while (produced < uncompressedSize) {
    if (source.length - position < 4) {
      throw ParquetFormatException(
        'truncated Hadoop block header at byte $position',
      );
    }
    final blockLength = _readUint32BE(source, position);
    position += 4;
    if (blockLength == 0) {
      throw ParquetFormatException('Hadoop block has zero length');
    }
    final remainingBlock = uncompressedSize - produced;
    if (blockLength > remainingBlock) {
      throw ParquetFormatException(
        'Hadoop block declares $blockLength bytes, '
        'only $remainingBlock remain in the page',
      );
    }

    var blockProduced = 0;
    while (blockProduced < blockLength) {
      if (source.length - position < 4) {
        throw ParquetFormatException(
          'truncated Hadoop chunk header at byte $position',
        );
      }
      final chunkLength = _readUint32BE(source, position);
      position += 4;
      if (chunkLength == 0) {
        throw ParquetFormatException('Hadoop chunk has zero length');
      }
      if (chunkLength > source.length - position) {
        throw ParquetFormatException(
          'Hadoop chunk length $chunkLength overruns the page '
          '(${source.length - position} bytes left)',
        );
      }
      final chunk = input.slice(position, position + chunkLength);
      position += chunkLength;

      final destination = Uint8List.sublistView(
        output,
        produced,
        produced + blockLength - blockProduced,
      );
      final written = decompressChunk(chunk, destination);
      if (written <= 0 || written > blockLength - blockProduced) {
        throw ParquetFormatException(
          'Hadoop chunk produced $written bytes for a block with '
          '${blockLength - blockProduced} bytes left',
        );
      }
      produced += written;
      blockProduced += written;
    }
  }

  if (position != source.length) {
    throw ParquetFormatException(
      'Hadoop stream has ${source.length - position} trailing bytes',
    );
  }
  return Bytes.fromUint8List(output);
}

/// Encodes [input] as a Hadoop block stream with one compressed chunk per
/// [blockSize]-byte block.
///
/// Empty input encodes to an empty byte stream (Hadoop writes no header for an
/// empty stream), which [decodeHadoopBlockStream] accepts for size 0.
Bytes encodeHadoopBlockStream(
  Bytes input, {
  int blockSize = 128 * 1024,
  required ChunkCompressor compressChunk,
}) {
  if (blockSize <= 0) {
    throw RangeError.value(blockSize, 'blockSize', 'must be positive');
  }
  if (input.isEmpty) return Bytes.empty;

  final builder = BytesMut(input.length + 64);
  for (var offset = 0; offset < input.length; offset += blockSize) {
    final end = offset + blockSize < input.length
        ? offset + blockSize
        : input.length;
    final block = input.slice(offset, end);
    final chunk = compressChunk(block);
    if (chunk.isEmpty) {
      throw ParquetFormatException(
        'codec produced an empty chunk for ${block.length} input bytes',
      );
    }
    builder
      ..putUint32(block.length)
      ..putUint32(chunk.length)
      ..putBytes(chunk);
  }
  return builder.freeze();
}

int _readUint32BE(Uint8List data, int position) =>
    (data[position] << 24) |
    (data[position + 1] << 16) |
    (data[position + 2] << 8) |
    data[position + 3];
