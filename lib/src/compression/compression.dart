/// Public entry point for the Parquet compression layer.
///
/// ```dart
/// import 'package:parquet/src/compression/compression.dart';
///
/// final codec = codecFor(CompressionType.zstd);
/// final raw = codec.decode(pageBody, uncompressedSize: headerSize);
/// ```
library;

export 'brotli_codec.dart';
export 'codec.dart';
export 'gzip_codec.dart';
export 'lz4_codec.dart';
export 'lzo_codec.dart';
export 'registry.dart';
export 'snappy_codec.dart';
export 'uncompressed_codec.dart';
export 'zstd_codec.dart';
