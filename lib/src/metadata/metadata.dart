/// Read-only row-group and column-chunk metadata bound to a schema.
library;

import '../format/parquet_format.dart';
import '../io/source.dart';
import '../schema/schema.dart';
import '../util/errors.dart';
import 'page_index.dart';

export '../format/enums.dart' show CompressionType, Encoding, PhysicalType;
export '../format/parquet_format.dart'
    show
        ColumnMetaData,
        ColumnOrder,
        FileMetaData,
        KeyValue,
        PageEncodingStats,
        PageHeader,
        PageType,
        RowGroup,
        SchemaElement,
        SizeStatistics,
        SortingColumn,
        Statistics;
export '../schema/schema.dart' show ColumnDescriptor, ParquetSchema, SchemaNode;
export 'page_index.dart';

/// Metadata for a whole Parquet file: file metadata, schema and row groups.
final class ParquetMetadata {
  /// Binds [fileMetadata] to a [schema].
  ///
  /// The schema is derived from [fileMetadata] unless one is supplied.
  factory ParquetMetadata(FileMetaData fileMetadata, {ParquetSchema? schema}) {
    final effectiveSchema =
        schema ?? ParquetSchema.fromFileMetaData(fileMetadata);
    final rowGroups = <RowGroupMetaData>[];
    for (var i = 0; i < fileMetadata.rowGroups.length; i++) {
      rowGroups.add(
        RowGroupMetaData.fromRowGroup(
          fileMetadata.rowGroups[i],
          effectiveSchema,
          rowGroupIndex: i,
        ),
      );
    }
    return ParquetMetadata._(
      fileMetadata,
      effectiveSchema,
      List<RowGroupMetaData>.unmodifiable(rowGroups),
    );
  }

  /// Alias for the main [ParquetMetadata] constructor.
  static ParquetMetadata fromFileMetaData(
    FileMetaData fileMetadata, {
    ParquetSchema? schema,
  }) => ParquetMetadata(fileMetadata, schema: schema);

  ParquetMetadata._(this.fileMetadata, this.schema, this.rowGroups);

  /// Raw Thrift file metadata.
  final FileMetaData fileMetadata;

  /// Parsed schema tree and leaf descriptors.
  final ParquetSchema schema;

  /// Row groups in file order.
  final List<RowGroupMetaData> rowGroups;

  /// Number of rows in the file.
  int get numRows => fileMetadata.numRows;

  /// Number of row groups.
  int get numRowGroups => rowGroups.length;

  /// Format version written by the file.
  int get version => fileMetadata.version;

  /// Writer identification string.
  String? get createdBy => fileMetadata.createdBy;

  /// Returns the row group at [index].
  RowGroupMetaData rowGroup(int index) {
    if (index < 0 || index >= rowGroups.length) {
      throw ParquetRangeException(
        'row group index $index out of range (0..${rowGroups.length - 1})',
      );
    }
    return rowGroups[index];
  }

  /// Returns the column chunk for [columnIndex] in [rowGroupIndex].
  ColumnChunkMetaData columnChunk(int rowGroupIndex, int columnIndex) =>
      rowGroup(rowGroupIndex).column(columnIndex);

  @override
  String toString() =>
      'ParquetMetadata($numRows rows, $numRowGroups row groups, '
      '${schema.numColumns} columns)';
}

/// Metadata for one row group.
final class RowGroupMetaData {
  RowGroupMetaData._({
    required this.columns,
    required this.numRows,
    required this.totalByteSize,
    this.totalCompressedSize,
    this.fileOffset,
    this.sortingColumns,
    this.ordinal,
    required this.index,
  });

  /// Builds row-group metadata, resolving every column chunk against [schema].
  factory RowGroupMetaData.fromRowGroup(
    RowGroup rowGroup,
    ParquetSchema schema, {
    int rowGroupIndex = 0,
  }) {
    if (rowGroup.columns.length != schema.numColumns) {
      throw ParquetFormatException(
        'row group $rowGroupIndex has ${rowGroup.columns.length} column '
        'chunk(s) but the schema has ${schema.numColumns} leaf column(s)',
      );
    }

    final columns = <ColumnChunkMetaData>[];
    for (var i = 0; i < rowGroup.columns.length; i++) {
      final chunk = rowGroup.columns[i];
      final path = chunk.metaData.pathInSchema;
      final descriptor =
          schema.columnByPath(path) ?? (path.isEmpty ? schema.column(i) : null);
      if (descriptor == null) {
        throw ParquetFormatException(
          'row group $rowGroupIndex column chunk $i has path '
          '"${path.join('.')}" which is not a leaf in the schema',
        );
      }
      columns.add(ColumnChunkMetaData._(chunk, descriptor));
    }

    return RowGroupMetaData._(
      columns: List<ColumnChunkMetaData>.unmodifiable(columns),
      numRows: rowGroup.numRows,
      totalByteSize: rowGroup.totalByteSize,
      totalCompressedSize: rowGroup.totalCompressedSize,
      fileOffset: rowGroup.fileOffset,
      sortingColumns: rowGroup.sortingColumns,
      ordinal: rowGroup.ordinal,
      index: rowGroupIndex,
    );
  }

  /// Column chunks, in row-group order (normally schema leaf order).
  final List<ColumnChunkMetaData> columns;

  /// Number of rows in this row group.
  final int numRows;

  /// Total uncompressed byte size of all column data.
  final int totalByteSize;

  /// Total compressed byte size of all column data, if recorded.
  final int? totalCompressedSize;

  /// Offset of the first page in this row group, if recorded.
  final int? fileOffset;

  /// Sort order hints, if any.
  final List<SortingColumn>? sortingColumns;

  /// Row group ordinal, if recorded.
  final int? ordinal;

  /// Zero-based index of this row group in the file.
  final int index;

  /// Number of column chunks.
  int get numColumns => columns.length;

  /// Returns the column chunk at [index].
  ColumnChunkMetaData column(int index) {
    if (index < 0 || index >= columns.length) {
      throw ParquetRangeException(
        'column index $index out of range (0..${columns.length - 1})',
      );
    }
    return columns[index];
  }

  /// Returns the column chunk whose path is [path], or `null`.
  ColumnChunkMetaData? columnByPath(List<String> path) {
    for (final column in columns) {
      if (_samePath(column.path, path)) return column;
    }
    return null;
  }

  @override
  String toString() =>
      'RowGroupMetaData($numRows rows, ${columns.length} columns)';
}

/// Metadata for one column chunk, bound to its schema leaf.
final class ColumnChunkMetaData {
  ColumnChunkMetaData._(ColumnChunk chunk, this.descriptor)
    : filePath = chunk.filePath,
      fileOffset = chunk.fileOffset,
      path = List<String>.unmodifiable(chunk.metaData.pathInSchema),
      type = chunk.metaData.type,
      compression = chunk.metaData.codec,
      encodings = List<Encoding>.unmodifiable(chunk.metaData.encodings),
      numValues = chunk.metaData.numValues,
      totalUncompressedSize = chunk.metaData.totalUncompressedSize,
      totalCompressedSize = chunk.metaData.totalCompressedSize,
      dataPageOffset = chunk.metaData.dataPageOffset,
      indexPageOffset = chunk.metaData.indexPageOffset,
      dictionaryPageOffset = chunk.metaData.dictionaryPageOffset,
      statistics = chunk.metaData.statistics,
      keyValueMetadata = chunk.metaData.keyValueMetadata,
      encodingStats = chunk.metaData.encodingStats,
      bloomFilterOffset = chunk.metaData.bloomFilterOffset,
      bloomFilterLength = chunk.metaData.bloomFilterLength,
      sizeStatistics = chunk.metaData.sizeStatistics,
      offsetIndexOffset = chunk.offsetIndexOffset,
      offsetIndexLength = chunk.offsetIndexLength,
      columnIndexOffset = chunk.columnIndexOffset,
      columnIndexLength = chunk.columnIndexLength;

  /// Schema leaf this chunk belongs to.
  final ColumnDescriptor? descriptor;

  /// External file path (summary files only).
  final String? filePath;

  /// Deprecated replicated-metadata offset.
  final int? fileOffset;

  /// Column path without the schema root.
  final List<String> path;

  /// Physical type of the column.
  final PhysicalType type;

  /// Compression codec.
  final CompressionType compression;

  /// All encodings used by the column.
  final List<Encoding> encodings;

  /// Number of values, including nulls.
  final int numValues;

  /// Total uncompressed size including page headers.
  final int totalUncompressedSize;

  /// Total compressed size including page headers.
  final int totalCompressedSize;

  /// Offset of the first data page.
  final int? dataPageOffset;

  /// Offset of the index page, if any.
  final int? indexPageOffset;

  /// Offset of the dictionary page, if any.
  final int? dictionaryPageOffset;

  /// Column chunk statistics.
  final Statistics? statistics;

  /// Per-column key/value metadata.
  final List<KeyValue>? keyValueMetadata;

  /// Per-page type/encoding counts.
  final List<PageEncodingStats>? encodingStats;

  /// Bloom filter offset, if any.
  final int? bloomFilterOffset;

  /// Bloom filter length including its header, if known.
  final int? bloomFilterLength;

  /// Encoded size statistics, if present.
  final SizeStatistics? sizeStatistics;

  /// Offset of the `OffsetIndex`, if present.
  final int? offsetIndexOffset;

  /// Length of the `OffsetIndex`, if present.
  final int? offsetIndexLength;

  /// Offset of the `ColumnIndex`, if present.
  final int? columnIndexOffset;

  /// Length of the `ColumnIndex`, if present.
  final int? columnIndexLength;

  /// Reads and parses the `OffsetIndex` for this chunk, if present.
  OffsetIndex? readOffsetIndex(SyncParquetSource source) {
    final offset = offsetIndexOffset;
    final length = offsetIndexLength;
    if (offset == null || length == null) return null;
    return parseOffsetIndex(source.read(offset, length));
  }

  /// Asynchronously reads and parses the `OffsetIndex`, if present.
  Future<OffsetIndex?> readOffsetIndexAsync(ParquetSource source) async {
    final offset = offsetIndexOffset;
    final length = offsetIndexLength;
    if (offset == null || length == null) return null;
    return parseOffsetIndex(await source.read(offset, length));
  }

  /// Reads and parses the `ColumnIndex` for this chunk, if present.
  ColumnIndex? readColumnIndex(SyncParquetSource source) {
    final offset = columnIndexOffset;
    final length = columnIndexLength;
    if (offset == null || length == null) return null;
    return parseColumnIndex(source.read(offset, length));
  }

  /// Asynchronously reads and parses the `ColumnIndex`, if present.
  Future<ColumnIndex?> readColumnIndexAsync(ParquetSource source) async {
    final offset = columnIndexOffset;
    final length = columnIndexLength;
    if (offset == null || length == null) return null;
    return parseColumnIndex(await source.read(offset, length));
  }

  /// Alias for [type].
  PhysicalType get physicalType => type;

  /// Alias for [totalUncompressedSize].
  int get uncompressedSize => totalUncompressedSize;

  /// Alias for [totalCompressedSize].
  int get compressedSize => totalCompressedSize;

  /// Whether this chunk has a dictionary page.
  bool get hasDictionaryPage => dictionaryPageOffset != null;

  /// Whether this chunk records a data page offset.
  bool get hasDataPage => dataPageOffset != null;

  /// Whether the chunk contains dictionary-encoded pages.
  bool get isDictionaryEncoded =>
      encodings.contains(Encoding.rleDictionary) ||
      encodings.contains(Encoding.plainDictionary);

  /// Dotted column path.
  String get dotPath => path.join('.');

  /// First byte of the column chunk (dictionary page when present).
  int get startOffset {
    final data = dataPageOffset;
    final dictionary = dictionaryPageOffset;
    if (dictionary != null && data != null) {
      return dictionary < data ? dictionary : data;
    }
    final start = dictionary ?? data;
    if (start == null) {
      throw ParquetFormatException(
        'column "$dotPath" has neither data nor dictionary page offset',
      );
    }
    return start;
  }

  /// Offset one past the last byte of the column chunk.
  int get endOffset => startOffset + totalCompressedSize;

  @override
  String toString() => 'ColumnChunkMetaData($dotPath, $type, $compression)';
}

bool _samePath(List<String> a, List<String> b) {
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}
