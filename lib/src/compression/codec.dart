/// Compression codec abstraction.
library;

import 'package:blit/blit.dart' show Bytes;

/// A Parquet compression codec.
///
/// `codecId` is the value stored in the Parquet `CompressionCodec` enum.
abstract interface class CompressionCodec {
  int get codecId;

  /// Decompresses one page body. [uncompressedSize] comes from the page header
  /// and must be used to bound the output (decompression-bomb protection).
  Bytes decode(Bytes input, {required int uncompressedSize});

  /// Compresses one page body. Read-only consumers may ignore this.
  Bytes encode(Bytes input);
}
