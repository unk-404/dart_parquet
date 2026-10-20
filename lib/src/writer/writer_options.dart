/// Options accepted by the Parquet writer.
library;

import '../compression/codec.dart';
import '../compression/registry.dart';
import '../format/enums.dart';

/// Configuration for [ParquetWriter].
///
/// Defaults mirror the common Parquet writer defaults: uncompressed pages,
/// dictionary encoding enabled, 1 MiB data pages and format data page v1.
final class ParquetWriterOptions {
  /// Creates writer options.
  const ParquetWriterOptions({
    this.compression = CompressionType.uncompressed,
    this.useDictionary = true,
    this.dictionaryPageSizeLimit = 1024 * 1024,
    this.dataPageVersion = 1,
    this.writeStatistics = true,
    this.pageSize = 1024 * 1024,
    this.createdBy = 'parquet-dart version 0.1.1',
    this.keyValueMetadata,
  });

  /// Page compression codec.
  final CompressionType compression;

  /// Whether BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY values may use dictionary
  /// encoding. Falls back to PLAIN when the dictionary exceeds
  /// [dictionaryPageSizeLimit].
  final bool useDictionary;

  /// Maximum uncompressed dictionary page size in bytes (including the
  /// per-value length prefixes for BYTE_ARRAY).
  final int dictionaryPageSizeLimit;

  /// Data page format version: 1 or 2.
  final int dataPageVersion;

  /// Whether chunk and page statistics are written.
  final bool writeStatistics;

  /// Target uncompressed page size in bytes. Pages never split a row.
  final int pageSize;

  /// Value written to `FileMetaData.created_by`.
  final String createdBy;

  /// Optional file-level key/value metadata.
  final Map<String, String>? keyValueMetadata;

  /// Resolved codec implementation for [compression].
  CompressionCodec get codec => codecFor(compression);

  /// Validates option ranges; throws [ArgumentError] when invalid.
  void validate() {
    if (dataPageVersion != 1 && dataPageVersion != 2) {
      throw ArgumentError.value(
        dataPageVersion,
        'dataPageVersion',
        'must be 1 or 2',
      );
    }
    if (pageSize <= 0) {
      throw ArgumentError.value(pageSize, 'pageSize', 'must be positive');
    }
    if (dictionaryPageSizeLimit < 0) {
      throw ArgumentError.value(
        dictionaryPageSizeLimit,
        'dictionaryPageSizeLimit',
        'must not be negative',
      );
    }
  }
}
