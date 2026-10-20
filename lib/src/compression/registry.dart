/// Codec registry: maps Parquet `CompressionCodec` values to implementations.
library;

import '../format/enums.dart';
import '../util/errors.dart';
import 'brotli_codec.dart';
import 'codec.dart';
import 'gzip_codec.dart';
import 'lz4_codec.dart';
import 'lzo_codec.dart';
import 'snappy_codec.dart';
import 'uncompressed_codec.dart';
import 'zstd_codec.dart';

/// Shared, stateless codec instances.
///
/// Codecs hold no per-page state (the LZO codec only keeps reusable work
/// memory), so one instance per code is safe to share across readers.
final Map<CompressionType, CompressionCodec> _codecs =
    Map<CompressionType, CompressionCodec>.unmodifiable({
      CompressionType.uncompressed: const UncompressedCodec(),
      CompressionType.snappy: const SnappyCodec(),
      CompressionType.gzip: const GzipCodec(),
      CompressionType.lzo: LzoCodec(),
      CompressionType.brotli: const BrotliCodec(),
      CompressionType.lz4: const Lz4LegacyCodec(),
      CompressionType.zstd: const ZstdCodec(),
      CompressionType.lz4Raw: const Lz4RawCodec(),
    });

/// All codecs keyed by their Parquet compression code.
Map<CompressionType, CompressionCodec> get compressionCodecs => _codecs;

/// Returns the codec for [type].
///
/// The returned instance is shared; callers must not store per-page state on
/// it.
CompressionCodec codecFor(CompressionType type) {
  final codec = _codecs[type];
  if (codec == null) {
    throw ParquetUnsupportedException('unsupported compression codec: $type');
  }
  return codec;
}

/// Alias for [codecFor], for call sites that think in terms of construction.
CompressionCodec createCodec(CompressionType type) => codecFor(type);

/// Returns the codec for a raw Thrift compression value.
///
/// Throws [ParquetUnsupportedException] for values this version does not know
/// (future Parquet codecs, or corrupt metadata).
CompressionCodec codecForValue(int codecId) {
  for (final type in CompressionType.values) {
    if (type.value == codecId) return codecFor(type);
  }
  throw ParquetUnsupportedException(
    'unsupported compression codec id: $codecId',
  );
}
