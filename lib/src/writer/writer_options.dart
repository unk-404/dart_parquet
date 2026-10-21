/// Options accepted by the Parquet writer.
library;

import '../compression/codec.dart';
import '../compression/registry.dart';
import '../format/enums.dart';

/// Configuration for [ParquetWriter].
///
/// Defaults mirror the common Parquet writer defaults: uncompressed pages,
/// dictionary encoding enabled, 1 MiB data pages and format data page v1.
/// The v0.1.2 features ([writeCrc], [writePageIndex], [writeBloomFilter]) are
/// off by default so default output keeps the v0.1.1 layout.
final class ParquetWriterOptions {
  /// Creates writer options.
  const ParquetWriterOptions({
    this.compression = CompressionType.uncompressed,
    this.useDictionary = true,
    this.dictionaryPageSizeLimit = 1024 * 1024,
    this.dataPageVersion = 1,
    this.writeStatistics = true,
    this.pageSize = 1024 * 1024,
    this.createdBy = 'parquet-dart version 0.1.3',
    this.keyValueMetadata,
    this.writeCrc = false,
    this.writePageIndex = false,
    this.writeBloomFilter = false,
    this.bloomFilterFpp = 0.01,
    this.bloomFilterMaxBytes = 1024 * 1024,
    this.enableAlp = false,
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

  /// Whether `PageHeader.crc` (CRC-32 IEEE of the compressed page body) is
  /// written for every page.
  ///
  /// Defaults to `false`, matching the v0.1.1 output.
  final bool writeCrc;

  /// Whether the `OffsetIndex` / `ColumnIndex` page index is written after the
  /// column chunks.
  ///
  /// Defaults to `false`.
  final bool writePageIndex;

  /// Whether a Split Block Bloom Filter is written for every column chunk with
  /// at least one non-null value.
  ///
  /// Defaults to `false`.
  final bool writeBloomFilter;

  /// Target false-positive probability for Bloom filters.
  final double bloomFilterFpp;

  /// Upper bound on a Bloom filter's bitset size in bytes.
  ///
  /// Must be at least 32; the actual size is rounded down to a power of two.
  final int bloomFilterMaxBytes;

  /// Whether FLOAT/DOUBLE pages use ALP (Parquet encoding 10).
  ///
  /// ALP is a preview encoding and defaults to `false`; readers that do not
  /// implement ALP will reject the file.
  final bool enableAlp;

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
    if (!(bloomFilterFpp > 0 && bloomFilterFpp < 1)) {
      throw ArgumentError.value(
        bloomFilterFpp,
        'bloomFilterFpp',
        'must be between 0 and 1 (exclusive)',
      );
    }
    if (bloomFilterMaxBytes < 32) {
      throw ArgumentError.value(
        bloomFilterMaxBytes,
        'bloomFilterMaxBytes',
        'must be at least 32',
      );
    }
  }
}
