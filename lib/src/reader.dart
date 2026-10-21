/// High-level Parquet reader.
library;

import 'dart:async';
import 'dart:typed_data';

import 'column/page_reader.dart';
import 'format/footer.dart';
import 'io/source.dart';
import 'metadata/metadata.dart';
import 'parallel/isolate_runner.dart';
import 'predicate/predicate.dart';
import 'schema/projection.dart';
import 'table/column.dart';
import 'table/nested_assembler.dart';
import 'table/record_batch.dart';
import 'table/table.dart';
import 'feature.dart';
import 'table/table_concat.dart';
import 'util/errors.dart';
import 'parallel/worker_pool.dart';

/// Predicate over row-group metadata used for statistics pruning.
typedef RowGroupFilter = bool Function(RowGroupMetaData rowGroup);

/// Options for opening/reading.
final class ParquetReaderOptions {
  const ParquetReaderOptions({
    this.columns,
    this.maxRows,
    this.rowGroupFilter,
    this.predicate,
    this.parallelism,
    this.verifyCrc = false,
  });

  /// Top-level column projection; null means all columns.
  final List<String>? columns;

  /// Optional row limit for [ParquetReader.readTable].
  final int? maxRows;

  /// Optional row-group pruning callback. Returning `false` skips the group
  /// before any page or column chunk is read.
  final RowGroupFilter? rowGroupFilter;

  /// Optional typed predicate. It prunes row groups via statistics and then
  /// filters decoded rows (flat tables in v0.1.2).
  final ParquetPredicate? predicate;

  /// Isolate workers for async memory-source reads.
  ///
  /// `null` selects `auto` on the VM (up to 8 workers, only for multi-row-group
  /// buffers >= 1 MiB), `1` forces the current isolate, and `N > 1` uses up to
  /// N workers. Web always stays on the current event loop. Sync methods never
  /// use isolates.
  final int? parallelism;

  /// Verify page-header CRC32 values when present.
  final bool verifyCrc;
}

/// Read-only Parquet reader.
///
/// Use [fromBytes] for a synchronous in-memory reader, or [open] for a
/// [ParquetSource] (native file, OPFS, Blob, HTTP Range).
final class ParquetReader {
  ParquetReader._(
    this.metadata,
    this._sync,
    this._async,
    this.options, {
    this._bytes,
  }) : schema = metadata.schema;

  final ParquetMetadata metadata;
  final ParquetSchema schema;
  final SyncParquetSource? _sync;
  final ParquetSource? _async;
  final ParquetReaderOptions options;
  final Uint8List? _bytes;

  int get numRowGroups => metadata.numRowGroups;
  int get numRows => metadata.numRows;
  int get version => metadata.version;
  String? get createdBy => metadata.createdBy;

  /// Metadata cache for immutable in-memory buffers.
  ///
  /// Parquet footers are often read repeatedly. The cache is keyed by the
  /// backing [ByteBuffer] and is weak, so a buffer can be collected normally.
  /// Call [clearMetadataCache] if a buffer is mutated in place and must be
  /// parsed again.
  static Expando<ParquetMetadata> _metadataCache =
      Expando<ParquetMetadata>('parquet.metadata');

  /// Drops all cached in-memory metadata.
  static void clearMetadataCache() {
    _metadataCache = Expando<ParquetMetadata>('parquet.metadata');
  }

  /// Reads a footer from an in-memory buffer.
  ///
  /// The buffer is treated as immutable while readers may reference it; this
  /// is required by the zero-copy column views. Repeated calls with the same
  /// backing buffer reuse the parsed metadata.
  factory ParquetReader.fromBytes(
    Uint8List bytes, {
    ParquetReaderOptions options = const ParquetReaderOptions(),
  }) {
    final source = MemorySource.fromUint8List(bytes);
    final cached = _metadataCache[bytes];
    if (cached != null) {
      return ParquetReader._(cached, source, null, options, bytes: bytes);
    }
    final footer = _readFooterSync(source);
    final metadata = ParquetMetadata.fromFileMetaData(footer.fileMetadata);
    _metadataCache[bytes] = metadata;
    return ParquetReader._(metadata, source, null, options, bytes: bytes);
  }

  /// Creates a reader over an already-open synchronous source (native file,
  /// mmap, memory).
  factory ParquetReader.fromSyncSource(
    SyncParquetSource source, {
    ParquetReaderOptions options = const ParquetReaderOptions(),
  }) {
    final footer = _readFooterSync(source);
    final metadata = ParquetMetadata.fromFileMetaData(footer.fileMetadata);
    return ParquetReader._(metadata, source, null, options);
  }

  /// Opens an asynchronous source (file, OPFS, Blob, HTTP Range).
  static Future<ParquetReader> open(
    ParquetSource source, {
    ParquetReaderOptions options = const ParquetReaderOptions(),
  }) async {
    final footer = await _readFooterAsync(source);
    final metadata = ParquetMetadata.fromFileMetaData(footer.fileMetadata);
    return ParquetReader._(metadata, null, source, options);
  }

  /// Whether synchronous row-group reads are available.
  bool get supportsSyncReads => _sync != null;

  /// Reads one row group synchronously. Only available for [SyncParquetSource].
  ParquetTable readRowGroupSync(
    int index, {
    List<String>? columns,
    int? maxRows,
  }) {
    final source = _sync;
    if (source == null) {
      throw ParquetUnsupportedException(
        'this reader was opened from an asynchronous source; use '
        'readRowGroup()',
      );
    }
    final rowGroup = metadata.rowGroup(index);
    final projection = projectSchema(schema, columns ?? options.columns);
    final leaves = <int, DecodedColumnChunk>{};
    for (var i = 0; i < projection.sourceColumns.length; i++) {
      final descriptor = projection.sourceColumns[i];
      final chunk = rowGroup.column(descriptor.leafIndex);
      leaves[i] = ColumnChunkReader(
        chunk,
        descriptor,
        verifyCrc: options.verifyCrc,
      ).readSync(source);
    }
    final out = <ParquetColumn>[
      for (final node in projection.schema.root.children)
        buildNodeColumn(node, projection.schema, leaves, rowGroup.numRows),
    ];
    var rows = rowGroup.numRows;
    if (maxRows != null && maxRows < rows) {
      rows = maxRows;
      for (var i = 0; i < out.length; i++) {
        out[i] = sliceColumn(out[i], rows);
      }
    }
    return ParquetTable(schema: projection.schema, columns: out, numRows: rows);
  }

  /// Reads one row group asynchronously.
  Future<ParquetTable> readRowGroup(
    int index, {
    List<String>? columns,
    int? maxRows,
  }) async {
    final rowGroup = metadata.rowGroup(index);
    final source = _async ?? SyncSourceAdapter(_sync!);
    final projection = projectSchema(schema, columns ?? options.columns);
    final leaves = <int, DecodedColumnChunk>{};
    for (var i = 0; i < projection.sourceColumns.length; i++) {
      final descriptor = projection.sourceColumns[i];
      final chunk = rowGroup.column(descriptor.leafIndex);
      leaves[i] = await ColumnChunkReader(
        chunk,
        descriptor,
        verifyCrc: options.verifyCrc,
      ).read(source);
    }
    final out = <ParquetColumn>[
      for (final node in projection.schema.root.children)
        buildNodeColumn(node, projection.schema, leaves, rowGroup.numRows),
    ];
    var rows = rowGroup.numRows;
    if (maxRows != null && maxRows < rows) {
      rows = maxRows;
      for (var i = 0; i < out.length; i++) {
        out[i] = sliceColumn(out[i], rows);
      }
    }
    return ParquetTable(schema: projection.schema, columns: out, numRows: rows);
  }

  /// Reads one row group as a [RecordBatch] synchronously.
  RecordBatch readBatchSync(int index, {List<String>? columns, int? maxRows}) =>
      RecordBatch.fromTable(
        readRowGroupSync(index, columns: columns, maxRows: maxRows),
      );

  /// Reads one row group as a [RecordBatch] asynchronously.
  Future<RecordBatch> readBatch(
    int index, {
    List<String>? columns,
    int? maxRows,
  }) async => RecordBatch.fromTable(
    await readRowGroup(index, columns: columns, maxRows: maxRows),
  );

  /// Streams one [RecordBatch] per row group.
  Stream<RecordBatch> readBatches({
    List<String>? columns,
    int? maxRows,
    RowGroupFilter? rowGroupFilter,
    ParquetPredicate? predicate,
  }) async* {
    await for (final table in readRowGroups(
      columns: columns,
      maxRows: maxRows,
      rowGroupFilter: rowGroupFilter,
      predicate: predicate,
    )) {
      yield RecordBatch.fromTable(table);
    }
  }

  /// Reads all row groups synchronously and concatenates their columns.
  ///
  /// [rowGroupFilter] is applied before any column chunk is read; returning
  /// `false` skips the group entirely.
  ParquetTable readTableSync({
    List<String>? columns,
    int? maxRows,
    RowGroupFilter? rowGroupFilter,
    ParquetPredicate? predicate,
  }) {
    final source = _sync;
    if (source == null) {
      throw ParquetUnsupportedException(
        'this reader was opened from an asynchronous source; use readTable()',
      );
    }
    final typed = predicate ?? options.predicate;
    final rowFilter = rowGroupFilter ?? options.rowGroupFilter;
    final filter = (typed == null && rowFilter == null)
        ? null
        : (RowGroupMetaData group) =>
              (typed?.mayMatch(group) ?? true) &&
              (rowFilter?.call(group) ?? true);
    final projection = projectSchema(schema, columns ?? options.columns);
    final tables = <ParquetTable>[];
    var remaining = maxRows ?? options.maxRows;
    for (var i = 0; i < numRowGroups; i++) {
      if (remaining != null && remaining <= 0) break;
      final group = metadata.rowGroup(i);
      if (filter != null && !filter(group)) continue;
      final table = readRowGroupSync(
        i,
        columns: columns ?? options.columns,
        maxRows: remaining,
      );
      tables.add(table);
      if (remaining != null) remaining -= table.numRows;
    }
    final table = concatTables(projection.schema, tables);
    return typed == null ? table : table.filterRows(typed);
  }

  /// Reads all row groups asynchronously and concatenates their columns.
  Future<ParquetTable> readTable({
    List<String>? columns,
    int? maxRows,
    RowGroupFilter? rowGroupFilter,
    ParquetPredicate? predicate,
  }) async {
    final typed = predicate ?? options.predicate;
    final rowFilter = rowGroupFilter ?? options.rowGroupFilter;
    final filter = (typed == null && rowFilter == null)
        ? null
        : (RowGroupMetaData group) =>
              (typed?.mayMatch(group) ?? true) &&
              (rowFilter?.call(group) ?? true);
    final projection = projectSchema(schema, columns ?? options.columns);
    final totalLimit = maxRows ?? options.maxRows;
    final bytes = _bytes;
    final requested = options.parallelism;
    if (bytes != null &&
        totalLimit == null &&
        (requested == null || requested != 1) &&
        numRowGroups > 1 &&
        bytes.length >= (1 << 20)) {
      final indices = <int>[
        for (var i = 0; i < numRowGroups; i++)
          if (filter == null || filter(metadata.rowGroup(i))) i,
      ];
      final auto = requested ?? defaultParquetParallelism;
      final workers = auto <= 0 ? defaultParquetParallelism : auto;
      if (indices.length > 1 && workers > 1) {
        return _readTableParallel(
          bytes,
          indices,
          columns ?? options.columns,
          projection.schema,
          workers < indices.length ? workers : indices.length,
          typed,
        );
      }
    }
    final tables = <ParquetTable>[];
    var remaining = totalLimit;
    for (var i = 0; i < numRowGroups; i++) {
      if (remaining != null && remaining <= 0) break;
      final group = metadata.rowGroup(i);
      if (filter != null && !filter(group)) continue;
      final table = await readRowGroup(
        i,
        columns: columns ?? options.columns,
        maxRows: remaining,
      );
      tables.add(table);
      if (remaining != null) remaining -= table.numRows;
    }
    final table = concatTables(projection.schema, tables);
    return typed == null ? table : table.filterRows(typed);
  }

  Future<ParquetTable> _readTableParallel(
    Uint8List bytes,
    List<int> indices,
    List<String>? columns,
    ParquetSchema projectionSchema,
    int workers,
    ParquetPredicate? predicate,
  ) async {
    final pool = await WorkerPool.start(workers: workers);
    try {
      final tables = <ParquetTable>[];
      for (var start = 0; start < indices.length; start += workers) {
        final end = start + workers < indices.length
            ? start + workers
            : indices.length;
        final batch = await pool.runAll([
          for (var i = start; i < end; i++)
            () {
              final reader = ParquetReader.fromBytes(
                bytes,
                options: ParquetReaderOptions(columns: columns, parallelism: 1),
              );
              return reader.readRowGroupSync(indices[i]);
            },
        ]);
        tables.addAll(batch);
      }
      final table = concatTables(projectionSchema, tables);
      return predicate == null ? table : table.filterRows(predicate);
    } finally {
      await pool.close();
    }
  }

  /// Streams one table per row group, bounding peak memory to one row group.
  ///
  /// The returned stream is lazy. Each table has the same columns and schema
  /// as [readTable]. [rowGroupFilter] can skip whole groups using statistics
  /// before their pages are fetched.
  Stream<ParquetTable> readRowGroups({
    List<String>? columns,
    int? maxRows,
    RowGroupFilter? rowGroupFilter,
    ParquetPredicate? predicate,
  }) async* {
    final typed = predicate ?? options.predicate;
    final rowFilter = rowGroupFilter ?? options.rowGroupFilter;
    final filter = (typed == null && rowFilter == null)
        ? null
        : (RowGroupMetaData group) =>
              (typed?.mayMatch(group) ?? true) &&
              (rowFilter?.call(group) ?? true);
    var remaining = maxRows ?? options.maxRows;
    for (var i = 0; i < numRowGroups; i++) {
      if (remaining != null && remaining <= 0) break;
      final group = metadata.rowGroup(i);
      if (filter != null && !filter(group)) continue;
      var table = await readRowGroup(
        i,
        columns: columns ?? options.columns,
        maxRows: remaining,
      );
      if (remaining != null) remaining -= table.numRows;
      if (typed != null) table = table.filterRows(typed);
      yield table;
    }
  }

  void close() {
    _sync?.close();
    _async?.close();
  }
}

Footer _readFooterSync(SyncParquetSource source) {
  final length = source.length;
  if (length < 8) {
    throw ParquetFormatException('file is too short for Parquet: $length');
  }
  final header = source.read(0, 4);
  final magic = String.fromCharCodes(header.asUint8List());
  if (magic == parquetEncryptedMagic) {
    throw ParquetUnsupportedFeatureException(
      ParquetFeature.encryption,
      'encrypted Parquet files (PARE) are not supported',
    );
  }
  if (magic != parquetMagic) {
    throw ParquetFormatException('bad Parquet magic: "$magic"');
  }
  final trailer = source.read(length - 8, 8);
  final footerLength = readFooterLength(trailer);
  if (footerLength <= 0 || footerLength + 8 > length) {
    throw ParquetFormatException('invalid footer length $footerLength');
  }
  final footerBytes = source.read(length - 8 - footerLength, footerLength + 8);
  return parseFooter(footerBytes);
}

Future<Footer> _readFooterAsync(ParquetSource source) async {
  final length = await source.length;
  if (length < 8) {
    throw ParquetFormatException('file is too short for Parquet: $length');
  }
  final header = await source.read(0, 4);
  final magic = String.fromCharCodes(header.asUint8List());
  if (magic == parquetEncryptedMagic) {
    throw ParquetUnsupportedFeatureException(
      ParquetFeature.encryption,
      'encrypted Parquet files (PARE) are not supported',
    );
  }
  if (magic != parquetMagic) {
    throw ParquetFormatException('bad Parquet magic: "$magic"');
  }
  final trailer = await source.read(length - 8, 8);
  final footerLength = readFooterLength(trailer);
  if (footerLength <= 0 || footerLength + 8 > length) {
    throw ParquetFormatException('invalid footer length $footerLength');
  }
  final footerBytes = await source.read(
    length - 8 - footerLength,
    footerLength + 8,
  );
  return parseFooter(footerBytes);
}

