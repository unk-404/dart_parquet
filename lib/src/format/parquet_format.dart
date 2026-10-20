/// Typed, read-only views of the Parquet Thrift metadata structures.
///
/// These classes mirror the definitions in `parquet.thrift`. Parsing is done
/// from the generic maps produced by [ThriftCompactReader]; unknown fields are
/// ignored and binary values are kept as zero-copy [Uint8List] views.
library;

import 'dart:convert';
import 'dart:typed_data';

import '../util/errors.dart';
import 'enums.dart';
import 'logical_type.dart';
import 'thrift.dart';

export 'enums.dart';
export 'logical_type.dart';

/// A key/value pair from file or column metadata.
final class KeyValue {
  /// Creates a key/value pair.
  const KeyValue({required this.key, this.value});

  /// Parses a Thrift `KeyValue` struct.
  factory KeyValue.fromThrift(Map<int, Object?> map) => KeyValue(
    key: _requiredString(map, 1, 'KeyValue.key'),
    value: _optionalString(map, 2),
  );

  /// Metadata key.
  final String key;

  /// Optional metadata value.
  final String? value;

  @override
  String toString() => 'KeyValue($key, $value)';
}

/// Column chunk statistics (min/max and null counts).
final class Statistics {
  /// Creates statistics. All fields are optional in Parquet.
  const Statistics({
    this.max,
    this.min,
    this.nullCount,
    this.distinctCount,
    this.maxValue,
    this.minValue,
    this.isMaxValueExact,
    this.isMinValueExact,
    this.nanCount,
  });

  /// Parses a Thrift `Statistics` struct.
  factory Statistics.fromThrift(Map<int, Object?> map) => Statistics(
    max: _optionalBinary(map, 1),
    min: _optionalBinary(map, 2),
    nullCount: _optionalInt(map, 3),
    distinctCount: _optionalInt(map, 4),
    maxValue: _optionalBinary(map, 5),
    minValue: _optionalBinary(map, 6),
    isMaxValueExact: _optionalBool(map, 7),
    isMinValueExact: _optionalBool(map, 8),
    nanCount: _optionalInt(map, 9),
  );

  /// Deprecated maximum value (PLAIN encoding, signed ordering).
  final Uint8List? max;

  /// Deprecated minimum value (PLAIN encoding, signed ordering).
  final Uint8List? min;

  /// Number of null values, if known.
  final int? nullCount;

  /// Number of distinct values, if known.
  final int? distinctCount;

  /// Maximum value according to the column order.
  final Uint8List? maxValue;

  /// Minimum value according to the column order.
  final Uint8List? minValue;

  /// Whether [maxValue] is an exact bound.
  final bool? isMaxValueExact;

  /// Whether [minValue] is an exact bound.
  final bool? isMinValueExact;

  /// Number of NaN values for floating point columns, if known.
  final int? nanCount;

  /// Whether both a minimum and a maximum are available.
  bool get hasMinMax => minValue != null && maxValue != null;

  /// The effective maximum value, preferring `max_value` over deprecated `max`.
  Uint8List? get effectiveMax => maxValue ?? max;

  /// The effective minimum value, preferring `min_value` over deprecated `min`.
  Uint8List? get effectiveMin => minValue ?? min;
}

/// Statistics about the encoded size of a column chunk.
final class SizeStatistics {
  /// Creates size statistics.
  const SizeStatistics({
    this.unencodedByteArrayDataBytes,
    this.repetitionLevelHistogram,
    this.definitionLevelHistogram,
  });

  /// Parses a Thrift `SizeStatistics` struct.
  factory SizeStatistics.fromThrift(Map<int, Object?> map) => SizeStatistics(
    unencodedByteArrayDataBytes: _optionalInt(map, 1),
    repetitionLevelHistogram: _optionalIntList(map, 2),
    definitionLevelHistogram: _optionalIntList(map, 3),
  );

  /// Unencoded bytes needed for BYTE_ARRAY values.
  final int? unencodedByteArrayDataBytes;

  /// Number of values at each repetition level.
  final List<int>? repetitionLevelHistogram;

  /// Number of values at each definition level.
  final List<int>? definitionLevelHistogram;
}

/// A column sort order hint inside a row group.
final class SortingColumn {
  /// Creates a sorting column.
  const SortingColumn({
    required this.columnIdx,
    required this.descending,
    required this.nullsFirst,
  });

  /// Parses a Thrift `SortingColumn` struct.
  factory SortingColumn.fromThrift(Map<int, Object?> map) => SortingColumn(
    columnIdx: _requiredInt(map, 1, 'SortingColumn.column_idx'),
    descending: _requiredBool(map, 2, 'SortingColumn.descending'),
    nullsFirst: _requiredBool(map, 3, 'SortingColumn.nulls_first'),
  );

  /// Leaf column index inside the row group.
  final int columnIdx;

  /// Whether values are sorted descending.
  final bool descending;

  /// Whether nulls sort before non-null values.
  final bool nullsFirst;
}

/// Number of pages of a given type/encoding in a column chunk.
final class PageEncodingStats {
  /// Creates page encoding statistics.
  const PageEncodingStats({
    required this.pageType,
    required this.encoding,
    required this.count,
  });

  /// Parses a Thrift `PageEncodingStats` struct.
  factory PageEncodingStats.fromThrift(Map<int, Object?> map) =>
      PageEncodingStats(
        pageType: _enumValue(
          PageType.values,
          _requiredInt(map, 1, 'PageEncodingStats.page_type'),
          'PageEncodingStats.page_type',
          (value) => value.value,
        ),
        encoding: _enumValue(
          Encoding.values,
          _requiredInt(map, 2, 'PageEncodingStats.encoding'),
          'PageEncodingStats.encoding',
          (value) => value.value,
        ),
        count: _requiredInt(map, 3, 'PageEncodingStats.count'),
      );

  /// Type of page.
  final PageType pageType;

  /// Encoding used by those pages.
  final Encoding encoding;

  /// Number of pages.
  final int count;
}

/// Union discriminator for [ColumnOrder].
enum ColumnOrderKind {
  /// Order defined by the physical/logical type.
  typeOrder,

  /// IEEE-754 total order.
  ieee754TotalOrder,

  /// Chronological INT96 timestamp order.
  int96TimestampOrder,
}

/// A union describing how min/max statistics are ordered.
final class ColumnOrder {
  /// Creates a column order.
  const ColumnOrder(this.kind);

  /// Parses a Thrift `ColumnOrder` union.
  factory ColumnOrder.fromThrift(Map<int, Object?> map) {
    if (map.isEmpty) {
      throw ParquetFormatException('empty ColumnOrder union');
    }
    final fieldId = map.keys.reduce((a, b) => a < b ? a : b);
    final kind = switch (fieldId) {
      1 => ColumnOrderKind.typeOrder,
      2 => ColumnOrderKind.ieee754TotalOrder,
      3 => ColumnOrderKind.int96TimestampOrder,
      _ => throw ParquetFormatException(
        'unknown ColumnOrder union field $fieldId',
      ),
    };
    return ColumnOrder(kind);
  }

  /// The active union member.
  final ColumnOrderKind kind;
}

/// Encryption algorithm declared by the file metadata.
enum EncryptionAlgorithmKind {
  /// AES-GCM with a single footer key.
  aesGcmV1,

  /// AES-GCM with per-column keys derived via CTR.
  aesGcmCtrV1,
}

/// Encryption parameters from `FileMetaData.encryption_algorithm`.
final class EncryptionAlgorithm {
  /// Creates encryption parameters.
  const EncryptionAlgorithm({
    required this.kind,
    this.aadPrefix,
    this.aadFileUnique,
    this.supplyAadPrefix,
  });

  /// Parses a Thrift `EncryptionAlgorithm` union.
  factory EncryptionAlgorithm.fromThrift(Map<int, Object?> map) {
    if (map.isEmpty) {
      throw ParquetFormatException('empty EncryptionAlgorithm union');
    }
    final fieldId = map.keys.reduce((a, b) => a < b ? a : b);
    final kind = switch (fieldId) {
      1 => EncryptionAlgorithmKind.aesGcmV1,
      2 => EncryptionAlgorithmKind.aesGcmCtrV1,
      _ => throw ParquetFormatException(
        'unknown EncryptionAlgorithm union field $fieldId',
      ),
    };
    final payload = _asStruct(map[fieldId], 'EncryptionAlgorithm');
    return EncryptionAlgorithm(
      kind: kind,
      aadPrefix: _optionalBinary(payload, 1),
      aadFileUnique: _optionalBinary(payload, 2),
      supplyAadPrefix: _optionalBool(payload, 3),
    );
  }

  /// Active union member.
  final EncryptionAlgorithmKind kind;

  /// Additional authenticated data prefix, if stored.
  final Uint8List? aadPrefix;

  /// Unique file identifier used in the AAD suffix.
  final Uint8List? aadFileUnique;

  /// Whether the reader must supply the AAD prefix from elsewhere.
  final bool? supplyAadPrefix;
}

/// One element of a flattened Parquet schema.
final class SchemaElement {
  /// Creates a schema element.
  const SchemaElement({
    this.type,
    this.typeLength,
    this.repetitionType,
    required this.name,
    this.numChildren,
    this.convertedType,
    this.scale,
    this.precision,
    this.fieldId,
    this.logicalType,
  });

  /// Parses a Thrift `SchemaElement` struct.
  factory SchemaElement.fromThrift(Map<int, Object?> map) => SchemaElement(
    type: _optionalPhysicalType(map, 1),
    typeLength: _optionalInt(map, 2),
    repetitionType: _optionalRepetitionType(map, 3),
    name: _requiredString(map, 4, 'SchemaElement.name'),
    numChildren: _optionalInt(map, 5),
    convertedType: _optionalConvertedType(map, 6),
    scale: _optionalInt(map, 7),
    precision: _optionalInt(map, 8),
    fieldId: _optionalInt(map, 9),
    logicalType: _optionalLogicalType(map[10]),
  );

  /// Physical type; `null` for group nodes.
  final PhysicalType? type;

  /// Byte length for `FIXED_LEN_BYTE_ARRAY`, or maximum bit length.
  final int? typeLength;

  /// Repetition of this field (absent for the root).
  final FieldRepetitionType? repetitionType;

  /// Field name.
  final String name;

  /// Number of children for group nodes.
  final int? numChildren;

  /// Legacy converted type annotation.
  final ConvertedType? convertedType;

  /// Decimal scale, for legacy `DECIMAL` annotations.
  final int? scale;

  /// Decimal precision, for legacy `DECIMAL` annotations.
  final int? precision;

  /// Original field id, when the source schema had one.
  final int? fieldId;

  /// Modern logical type annotation.
  final LogicalType? logicalType;

  /// Whether this is a leaf (has a physical type).
  bool get isLeaf => type != null;

  /// Whether this is a group node.
  bool get isGroup => type == null;

  @override
  String toString() => 'SchemaElement($name, type: $type)';
}

/// Metadata for one column chunk.
final class ColumnMetaData {
  /// Creates column chunk metadata.
  const ColumnMetaData({
    required this.type,
    required this.encodings,
    required this.pathInSchema,
    required this.codec,
    required this.numValues,
    required this.totalUncompressedSize,
    required this.totalCompressedSize,
    this.keyValueMetadata,
    this.dataPageOffset,
    this.indexPageOffset,
    this.dictionaryPageOffset,
    this.statistics,
    this.encodingStats,
    this.bloomFilterOffset,
    this.bloomFilterLength,
    this.sizeStatistics,
  });

  /// Parses a Thrift `ColumnMetaData` struct.
  factory ColumnMetaData.fromThrift(Map<int, Object?> map) => ColumnMetaData(
    type: _enumValue(
      PhysicalType.values,
      _requiredInt(map, 1, 'ColumnMetaData.type'),
      'ColumnMetaData.type',
      (value) => value.value,
    ),
    encodings: List.unmodifiable([
      for (final raw in _requiredList(map[2], 'ColumnMetaData.encodings'))
        _enumValue(
          Encoding.values,
          _asInt(raw, 'ColumnMetaData.encodings[]'),
          'ColumnMetaData.encodings',
          (value) => value.value,
        ),
    ]),
    pathInSchema: _requiredStringList(map[3], 'ColumnMetaData.path_in_schema'),
    codec: _enumValue(
      CompressionType.values,
      _requiredInt(map, 4, 'ColumnMetaData.codec'),
      'ColumnMetaData.codec',
      (value) => value.value,
    ),
    numValues: _requiredInt(map, 5, 'ColumnMetaData.num_values'),
    totalUncompressedSize: _requiredInt(
      map,
      6,
      'ColumnMetaData.total_uncompressed_size',
    ),
    totalCompressedSize: _requiredInt(
      map,
      7,
      'ColumnMetaData.total_compressed_size',
    ),
    keyValueMetadata: _optionalStructList(
      map[8],
      'ColumnMetaData.key_value_metadata',
      KeyValue.fromThrift,
    ),
    dataPageOffset: _optionalInt(map, 9),
    indexPageOffset: _optionalInt(map, 10),
    dictionaryPageOffset: _optionalInt(map, 11),
    statistics: _optionalStruct(map[12], Statistics.fromThrift),
    encodingStats: _optionalStructList(
      map[13],
      'ColumnMetaData.encoding_stats',
      PageEncodingStats.fromThrift,
    ),
    bloomFilterOffset: _optionalInt(map, 14),
    bloomFilterLength: _optionalInt(map, 15),
    sizeStatistics: _optionalStruct(map[16], SizeStatistics.fromThrift),
  );

  /// Physical type of values in this chunk.
  final PhysicalType type;

  /// All encodings used by pages in this chunk.
  final List<Encoding> encodings;

  /// Leaf path in the schema (without the root name).
  final List<String> pathInSchema;

  /// Compression codec.
  final CompressionType codec;

  /// Number of values, including nulls.
  final int numValues;

  /// Total uncompressed size of all pages, including headers.
  final int totalUncompressedSize;

  /// Total compressed size of all pages, including headers.
  final int totalCompressedSize;

  /// Optional per-column key/value metadata.
  final List<KeyValue>? keyValueMetadata;

  /// Offset of the first data page.
  final int? dataPageOffset;

  /// Offset of the index page, if any.
  final int? indexPageOffset;

  /// Offset of the dictionary page, if any.
  final int? dictionaryPageOffset;

  /// Column chunk statistics.
  final Statistics? statistics;

  /// Per-page type/encoding counts.
  final List<PageEncodingStats>? encodingStats;

  /// Offset of the Bloom filter data, if any.
  final int? bloomFilterOffset;

  /// Size of the Bloom filter data including its header, if known.
  final int? bloomFilterLength;

  /// Encoded size statistics, if present.
  final SizeStatistics? sizeStatistics;
}

/// One column chunk inside a row group.
final class ColumnChunk {
  /// Creates a column chunk.
  const ColumnChunk({
    this.filePath,
    this.fileOffset,
    required this.metaData,
    this.offsetIndexOffset,
    this.offsetIndexLength,
    this.columnIndexOffset,
    this.columnIndexLength,
  });

  /// Parses a Thrift `ColumnChunk` struct.
  factory ColumnChunk.fromThrift(Map<int, Object?> map) {
    final metaData = _optionalStruct(map[3], ColumnMetaData.fromThrift);
    if (metaData == null) {
      throw ParquetFormatException('ColumnChunk.meta_data is required');
    }
    return ColumnChunk(
      filePath: _optionalString(map, 1),
      fileOffset: _optionalInt(map, 2),
      metaData: metaData,
      offsetIndexOffset: _optionalInt(map, 4),
      offsetIndexLength: _optionalInt(map, 5),
      columnIndexOffset: _optionalInt(map, 6),
      columnIndexLength: _optionalInt(map, 7),
    );
  }

  /// External file path (summary files only).
  final String? filePath;

  /// Deprecated offset of a replicated copy of the column metadata.
  final int? fileOffset;

  /// Column chunk metadata.
  final ColumnMetaData metaData;

  /// Offset of the `OffsetIndex`, if present.
  final int? offsetIndexOffset;

  /// Length of the `OffsetIndex`, if present.
  final int? offsetIndexLength;

  /// Offset of the `ColumnIndex`, if present.
  final int? columnIndexOffset;

  /// Length of the `ColumnIndex`, if present.
  final int? columnIndexLength;
}

/// A horizontal slice of rows.
final class RowGroup {
  /// Creates a row group.
  const RowGroup({
    required this.columns,
    required this.totalByteSize,
    required this.numRows,
    this.sortingColumns,
    this.fileOffset,
    this.totalCompressedSize,
    this.ordinal,
  });

  /// Parses a Thrift `RowGroup` struct.
  factory RowGroup.fromThrift(Map<int, Object?> map) => RowGroup(
    columns: _requiredStructList(
      map[1],
      'RowGroup.columns',
      ColumnChunk.fromThrift,
    ),
    totalByteSize: _requiredInt(map, 2, 'RowGroup.total_byte_size'),
    numRows: _requiredInt(map, 3, 'RowGroup.num_rows'),
    sortingColumns: _optionalStructList(
      map[4],
      'RowGroup.sorting_columns',
      SortingColumn.fromThrift,
    ),
    fileOffset: _optionalInt(map, 5),
    totalCompressedSize: _optionalInt(map, 6),
    ordinal: _optionalInt(map, 7),
  );

  /// Column chunks, in schema leaf order.
  final List<ColumnChunk> columns;

  /// Total uncompressed byte size of all column data.
  final int totalByteSize;

  /// Number of rows in this row group.
  final int numRows;

  /// Optional sort order of the rows.
  final List<SortingColumn>? sortingColumns;

  /// Offset of the first page (data or dictionary) in this row group.
  final int? fileOffset;

  /// Total compressed byte size of all column data.
  final int? totalCompressedSize;

  /// Row group ordinal within the file.
  final int? ordinal;
}

/// Data page header (format v1).
final class DataPageHeader {
  /// Creates a v1 data page header.
  const DataPageHeader({
    required this.numValues,
    required this.encoding,
    required this.definitionLevelEncoding,
    required this.repetitionLevelEncoding,
    this.statistics,
  });

  /// Parses a Thrift `DataPageHeader` struct.
  factory DataPageHeader.fromThrift(Map<int, Object?> map) => DataPageHeader(
    numValues: _requiredInt(map, 1, 'DataPageHeader.num_values'),
    encoding: _enumValue(
      Encoding.values,
      _requiredInt(map, 2, 'DataPageHeader.encoding'),
      'DataPageHeader.encoding',
      (value) => value.value,
    ),
    definitionLevelEncoding: _enumValue(
      Encoding.values,
      _requiredInt(map, 3, 'DataPageHeader.definition_level_encoding'),
      'DataPageHeader.definition_level_encoding',
      (value) => value.value,
    ),
    repetitionLevelEncoding: _enumValue(
      Encoding.values,
      _requiredInt(map, 4, 'DataPageHeader.repetition_level_encoding'),
      'DataPageHeader.repetition_level_encoding',
      (value) => value.value,
    ),
    statistics: _optionalStruct(map[5], Statistics.fromThrift),
  );

  /// Number of values, including nulls.
  final int numValues;

  /// Encoding of the values.
  final Encoding encoding;

  /// Encoding of definition levels.
  final Encoding definitionLevelEncoding;

  /// Encoding of repetition levels.
  final Encoding repetitionLevelEncoding;

  /// Optional page statistics.
  final Statistics? statistics;
}

/// Empty index page header.
final class IndexPageHeader {
  /// Creates the empty header.
  const IndexPageHeader();
}

/// Dictionary page header.
final class DictionaryPageHeader {
  /// Creates a dictionary page header.
  const DictionaryPageHeader({
    required this.numValues,
    required this.encoding,
    this.isSorted,
  });

  /// Parses a Thrift `DictionaryPageHeader` struct.
  factory DictionaryPageHeader.fromThrift(Map<int, Object?> map) =>
      DictionaryPageHeader(
        numValues: _requiredInt(map, 1, 'DictionaryPageHeader.num_values'),
        encoding: _enumValue(
          Encoding.values,
          _requiredInt(map, 2, 'DictionaryPageHeader.encoding'),
          'DictionaryPageHeader.encoding',
          (value) => value.value,
        ),
        isSorted: _optionalBool(map, 3),
      );

  /// Number of dictionary entries.
  final int numValues;

  /// Encoding of dictionary entries (normally `PLAIN`).
  final Encoding encoding;

  /// Whether entries are sorted.
  final bool? isSorted;
}

/// Data page header (format v2).
final class DataPageHeaderV2 {
  /// Creates a v2 data page header.
  const DataPageHeaderV2({
    required this.numValues,
    required this.numNulls,
    required this.numRows,
    required this.encoding,
    required this.definitionLevelsByteLength,
    required this.repetitionLevelsByteLength,
    required this.isCompressed,
    this.statistics,
  });

  /// Parses a Thrift `DataPageHeaderV2` struct.
  factory DataPageHeaderV2.fromThrift(Map<int, Object?> map) =>
      DataPageHeaderV2(
        numValues: _requiredInt(map, 1, 'DataPageHeaderV2.num_values'),
        numNulls: _requiredInt(map, 2, 'DataPageHeaderV2.num_nulls'),
        numRows: _requiredInt(map, 3, 'DataPageHeaderV2.num_rows'),
        encoding: _enumValue(
          Encoding.values,
          _requiredInt(map, 4, 'DataPageHeaderV2.encoding'),
          'DataPageHeaderV2.encoding',
          (value) => value.value,
        ),
        definitionLevelsByteLength: _requiredInt(
          map,
          5,
          'DataPageHeaderV2.definition_levels_byte_length',
        ),
        repetitionLevelsByteLength: _requiredInt(
          map,
          6,
          'DataPageHeaderV2.repetition_levels_byte_length',
        ),
        isCompressed: _optionalBool(map, 7) ?? true,
        statistics: _optionalStruct(map[8], Statistics.fromThrift),
      );

  /// Number of values, including nulls.
  final int numValues;

  /// Number of null values.
  final int numNulls;

  /// Number of rows (pages never split a row).
  final int numRows;

  /// Encoding of the data section.
  final Encoding encoding;

  /// Length of the uncompressed definition levels section.
  final int definitionLevelsByteLength;

  /// Length of the uncompressed repetition levels section.
  final int repetitionLevelsByteLength;

  /// Whether the data section is compressed (`true` when absent).
  final bool isCompressed;

  /// Optional page statistics.
  final Statistics? statistics;
}

/// A Parquet page header.
///
/// [headerSize] is the number of bytes consumed by the header itself; the page
/// body starts at `headerStart + headerSize`.
final class PageHeader {
  /// Creates a page header.
  const PageHeader({
    required this.type,
    required this.uncompressedPageSize,
    required this.compressedPageSize,
    this.crc,
    this.dataPageHeader,
    this.indexPageHeader,
    this.dictionaryPageHeader,
    this.dataPageHeaderV2,
    this.headerSize = 0,
  });

  /// Parses a Thrift `PageHeader` struct.
  factory PageHeader.fromThrift(Map<int, Object?> map, {int headerSize = 0}) {
    final uncompressedPageSize = _requiredInt(
      map,
      2,
      'PageHeader.uncompressed_page_size',
    );
    final compressedPageSize = _requiredInt(
      map,
      3,
      'PageHeader.compressed_page_size',
    );
    if (uncompressedPageSize < 0 || compressedPageSize < 0) {
      throw ParquetFormatException(
        'negative page size: uncompressed=$uncompressedPageSize '
        'compressed=$compressedPageSize',
      );
    }
    return PageHeader(
      type: _enumValue(
        PageType.values,
        _requiredInt(map, 1, 'PageHeader.type'),
        'PageHeader.type',
        (value) => value.value,
      ),
      uncompressedPageSize: uncompressedPageSize,
      compressedPageSize: compressedPageSize,
      crc: _optionalInt(map, 4),
      dataPageHeader: _optionalStruct(map[5], DataPageHeader.fromThrift),
      indexPageHeader: map[6] == null ? null : const IndexPageHeader(),
      dictionaryPageHeader: _optionalStruct(
        map[7],
        DictionaryPageHeader.fromThrift,
      ),
      dataPageHeaderV2: _optionalStruct(map[8], DataPageHeaderV2.fromThrift),
      headerSize: headerSize,
    );
  }

  /// Page type.
  final PageType type;

  /// Uncompressed body size in bytes.
  final int uncompressedPageSize;

  /// Compressed body size in bytes.
  final int compressedPageSize;

  /// Optional CRC32 checksum of the body.
  final int? crc;

  /// Header for a v1 data page.
  final DataPageHeader? dataPageHeader;

  /// Header for an index page.
  final IndexPageHeader? indexPageHeader;

  /// Header for a dictionary page.
  final DictionaryPageHeader? dictionaryPageHeader;

  /// Header for a v2 data page.
  final DataPageHeaderV2? dataPageHeaderV2;

  /// Number of bytes the serialised header occupied.
  final int headerSize;

  /// Alias for [headerSize].
  int get bytesConsumed => headerSize;
}

/// The parsed contents of a Parquet file footer.
final class FileMetaData {
  /// Creates file metadata.
  const FileMetaData({
    required this.version,
    required this.schema,
    required this.numRows,
    required this.rowGroups,
    this.keyValueMetadata,
    this.createdBy,
    this.columnOrders,
    this.encryptionAlgorithm,
    this.footerSigningKeyMetadata,
  });

  /// Parses a Thrift `FileMetaData` struct.
  factory FileMetaData.fromThrift(Map<int, Object?> map) => FileMetaData(
    version: _requiredInt(map, 1, 'FileMetaData.version'),
    schema: _requiredStructList(
      map[2],
      'FileMetaData.schema',
      SchemaElement.fromThrift,
    ),
    numRows: _requiredInt(map, 3, 'FileMetaData.num_rows'),
    rowGroups: _requiredStructList(
      map[4],
      'FileMetaData.row_groups',
      RowGroup.fromThrift,
    ),
    keyValueMetadata: _optionalStructList(
      map[5],
      'FileMetaData.key_value_metadata',
      KeyValue.fromThrift,
    ),
    createdBy: _optionalString(map, 6),
    columnOrders: _optionalStructList(
      map[7],
      'FileMetaData.column_orders',
      ColumnOrder.fromThrift,
    ),
    encryptionAlgorithm: _optionalStruct(
      map[8],
      EncryptionAlgorithm.fromThrift,
    ),
    footerSigningKeyMetadata: _optionalBinary(map, 9),
  );

  /// Format version written by the file.
  final int version;

  /// Flattened schema elements, root first.
  final List<SchemaElement> schema;

  /// Number of rows in the file.
  final int numRows;

  /// Row groups.
  final List<RowGroup> rowGroups;

  /// File-level key/value metadata.
  final List<KeyValue>? keyValueMetadata;

  /// Writer identification string.
  final String? createdBy;

  /// Per-leaf column order used for the new min/max statistics.
  final List<ColumnOrder>? columnOrders;

  /// Encryption parameters for plaintext-footers files.
  final EncryptionAlgorithm? encryptionAlgorithm;

  /// Metadata for the footer signing key, if any.
  final Uint8List? footerSigningKeyMetadata;

  /// Looks up a key/value metadata entry by [key].
  String? lookupMetadata(String key) {
    for (final entry in keyValueMetadata ?? const <KeyValue>[]) {
      if (entry.key == key) return entry.value;
    }
    return null;
  }
}

Map<int, Object?> _asStruct(Object? value, String context) {
  if (value is Map<int, Object?>) return value;
  throw ParquetFormatException(
    'expected a struct for $context, got ${value.runtimeType}',
  );
}

List<Object?> _asList(Object? value, String context) {
  if (value is List<Object?>) return value;
  throw ParquetFormatException(
    'expected a list for $context, got ${value.runtimeType}',
  );
}

int _asInt(Object? value, String context) {
  if (value is int) return value;
  throw ParquetFormatException(
    'expected an integer for $context, got ${value.runtimeType}',
  );
}

T _enumValue<T extends Enum>(
  List<T> values,
  int raw,
  String context,
  int Function(T value) valueOf,
) {
  for (final value in values) {
    if (valueOf(value) == raw) return value;
  }
  throw ParquetFormatException('unknown $context value $raw');
}

T? _optionalStruct<T>(Object? value, T Function(Map<int, Object?>) parse) {
  if (value == null) return null;
  return parse(_asStruct(value, 'struct'));
}

List<T>? _optionalStructList<T>(
  Object? value,
  String context,
  T Function(Map<int, Object?>) parse,
) {
  if (value == null) return null;
  final raw = _asList(value, context);
  return List.unmodifiable([
    for (var i = 0; i < raw.length; i++)
      parse(_asStruct(raw[i], '$context[$i]')),
  ]);
}

List<T> _requiredStructList<T>(
  Object? value,
  String context,
  T Function(Map<int, Object?>) parse,
) {
  final parsed = _optionalStructList(value, context, parse);
  if (parsed == null) {
    throw ParquetFormatException('missing required list $context');
  }
  return parsed;
}

List<Object?> _requiredList(Object? value, String context) {
  if (value == null) {
    throw ParquetFormatException('missing required list $context');
  }
  return _asList(value, context);
}

int _requiredInt(Map<int, Object?> map, int fieldId, String context) {
  final value = map[fieldId];
  if (value is! int) {
    throw ParquetFormatException('missing required integer $context');
  }
  return value;
}

int? _optionalInt(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  return value is int ? value : null;
}

bool _requiredBool(Map<int, Object?> map, int fieldId, String context) {
  final value = map[fieldId];
  if (value is! bool) {
    throw ParquetFormatException('missing required boolean $context');
  }
  return value;
}

bool? _optionalBool(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  return value is bool ? value : null;
}

Uint8List? _optionalBinary(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  if (value == null) return null;
  if (value is Uint8List) return value;
  throw ParquetFormatException(
    'field $fieldId is not binary (${value.runtimeType})',
  );
}

String? _optionalString(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  if (value == null) return null;
  if (value is String) return value;
  if (value is Uint8List) {
    try {
      return const Utf8Decoder().convert(value);
    } on FormatException catch (error) {
      throw ParquetFormatException('field $fieldId is not valid UTF-8', error);
    }
  }
  throw ParquetFormatException(
    'field $fieldId is not a string (${value.runtimeType})',
  );
}

String _requiredString(Map<int, Object?> map, int fieldId, String context) {
  final value = _optionalString(map, fieldId);
  if (value == null) {
    throw ParquetFormatException('missing required string $context');
  }
  return value;
}

List<String> _requiredStringList(Object? value, String context) {
  final raw = _requiredList(value, context);
  return List.unmodifiable([
    for (var i = 0; i < raw.length; i++) _stringValue(raw[i], '$context[$i]'),
  ]);
}

String _stringValue(Object? value, String context) {
  if (value is String) return value;
  if (value is Uint8List) {
    try {
      return const Utf8Decoder().convert(value);
    } on FormatException catch (error) {
      throw ParquetFormatException('$context is not valid UTF-8', error);
    }
  }
  throw ParquetFormatException(
    '$context is not a string (${value.runtimeType})',
  );
}

List<int>? _optionalIntList(Map<int, Object?> map, int fieldId) {
  final value = map[fieldId];
  if (value == null) return null;
  final raw = _asList(value, 'list field $fieldId');
  return List.unmodifiable([
    for (var i = 0; i < raw.length; i++) _asInt(raw[i], 'list field $fieldId'),
  ]);
}

PhysicalType? _optionalPhysicalType(Map<int, Object?> map, int fieldId) {
  final value = _optionalInt(map, fieldId);
  if (value == null) return null;
  return _enumValue(
    PhysicalType.values,
    value,
    'SchemaElement.type',
    (candidate) => candidate.value,
  );
}

FieldRepetitionType? _optionalRepetitionType(
  Map<int, Object?> map,
  int fieldId,
) {
  final value = _optionalInt(map, fieldId);
  if (value == null) return null;
  return _enumValue(
    FieldRepetitionType.values,
    value,
    'SchemaElement.repetition_type',
    (candidate) => candidate.value,
  );
}

ConvertedType? _optionalConvertedType(Map<int, Object?> map, int fieldId) {
  final value = _optionalInt(map, fieldId);
  if (value == null) return null;
  return ConvertedType.fromValue(value);
}

LogicalType? _optionalLogicalType(Object? value) {
  if (value == null) return null;
  return LogicalType.fromThrift(_asStruct(value, 'LogicalType'));
}
