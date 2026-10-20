/// High-level Parquet reader.
library;

import 'dart:typed_data';

import 'column/page_reader.dart';
import 'format/footer.dart';
import 'io/source.dart';
import 'metadata/metadata.dart';
import 'table/column.dart';
import 'table/nested_assembler.dart';
import 'table/table.dart';
import 'table/table_concat.dart';
import 'util/errors.dart';

/// Options for opening/reading.
final class ParquetReaderOptions {
  const ParquetReaderOptions({this.columns, this.maxRows});

  /// Top-level column projection; null means all columns.
  final List<String>? columns;

  /// Optional row limit for [ParquetReader.readTable].
  final int? maxRows;
}

/// Read-only Parquet reader.
///
/// Use [fromBytes] for a synchronous in-memory reader, or [open] for a
/// [ParquetSource] (native file, OPFS, Blob, HTTP Range).
final class ParquetReader {
  ParquetReader._(this.metadata, this._sync, this._async, this.options)
    : schema = metadata.schema;

  final ParquetMetadata metadata;
  final ParquetSchema schema;
  final SyncParquetSource? _sync;
  final ParquetSource? _async;
  final ParquetReaderOptions options;

  int get numRowGroups => metadata.numRowGroups;
  int get numRows => metadata.numRows;
  int get version => metadata.version;
  String? get createdBy => metadata.createdBy;

  /// Reads a footer from an in-memory buffer.
  factory ParquetReader.fromBytes(
    Uint8List bytes, {
    ParquetReaderOptions options = const ParquetReaderOptions(),
  }) {
    final source = MemorySource.fromUint8List(bytes);
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
    final nodes = _selectNodes(columns ?? options.columns);
    final leaves = <int, DecodedColumnChunk>{};
    for (final node in nodes) {
      for (final leaf in _leavesOf(node)) {
        final descriptor = schema.column(leaf.leafIndex!);
        final chunk = rowGroup.column(descriptor.leafIndex);
        leaves[leaf.leafIndex!] = ColumnChunkReader(
          chunk,
          descriptor,
        ).readSync(source);
      }
    }
    final out = <ParquetColumn>[
      for (final node in nodes)
        buildNodeColumn(node, schema, leaves, rowGroup.numRows),
    ];
    var rows = rowGroup.numRows;
    if (maxRows != null && maxRows < rows) {
      rows = maxRows;
      for (var i = 0; i < out.length; i++) {
        out[i] = sliceColumn(out[i], rows);
      }
    }
    return ParquetTable(schema: schema, columns: out, numRows: rows);
  }

  /// Reads one row group asynchronously.
  Future<ParquetTable> readRowGroup(
    int index, {
    List<String>? columns,
    int? maxRows,
  }) async {
    final rowGroup = metadata.rowGroup(index);
    final source = _async ?? SyncSourceAdapter(_sync!);
    final nodes = _selectNodes(columns ?? options.columns);
    final leaves = <int, DecodedColumnChunk>{};
    for (final node in nodes) {
      for (final leaf in _leavesOf(node)) {
        final descriptor = schema.column(leaf.leafIndex!);
        final chunk = rowGroup.column(descriptor.leafIndex);
        leaves[leaf.leafIndex!] = await ColumnChunkReader(
          chunk,
          descriptor,
        ).read(source);
      }
    }
    final out = <ParquetColumn>[
      for (final node in nodes)
        buildNodeColumn(node, schema, leaves, rowGroup.numRows),
    ];
    var rows = rowGroup.numRows;
    if (maxRows != null && maxRows < rows) {
      rows = maxRows;
      for (var i = 0; i < out.length; i++) {
        out[i] = sliceColumn(out[i], rows);
      }
    }
    return ParquetTable(schema: schema, columns: out, numRows: rows);
  }

  /// Reads all row groups synchronously and concatenates their columns.
  ParquetTable readTableSync({List<String>? columns, int? maxRows}) {
    final source = _sync;
    if (source == null) {
      throw ParquetUnsupportedException(
        'this reader was opened from an asynchronous source; use readTable()',
      );
    }
    final tables = <ParquetTable>[];
    var remaining = maxRows ?? options.maxRows;
    for (var i = 0; i < numRowGroups; i++) {
      if (remaining != null && remaining <= 0) break;
      final table = readRowGroupSync(
        i,
        columns: columns ?? options.columns,
        maxRows: remaining,
      );
      tables.add(table);
      if (remaining != null) remaining -= table.numRows;
    }
    return concatTables(schema, tables);
  }

  /// Reads all row groups asynchronously and concatenates their columns.
  Future<ParquetTable> readTable({List<String>? columns, int? maxRows}) async {
    final tables = <ParquetTable>[];
    var remaining = maxRows ?? options.maxRows;
    for (var i = 0; i < numRowGroups; i++) {
      if (remaining != null && remaining <= 0) break;
      final table = await readRowGroup(
        i,
        columns: columns ?? options.columns,
        maxRows: remaining,
      );
      tables.add(table);
      if (remaining != null) remaining -= table.numRows;
    }
    return concatTables(schema, tables);
  }

  List<SchemaNode> _selectNodes(List<String>? names) {
    final nodes = schema.root.children;
    if (names == null || names.isEmpty) return nodes;
    final byName = {for (final node in nodes) node.name: node};
    final out = <SchemaNode>[];
    for (final name in names) {
      final node = byName[name];
      if (node == null) {
        throw ParquetFormatException('no such column: $name');
      }
      out.add(node);
    }
    return out;
  }

  Iterable<SchemaNode> _leavesOf(SchemaNode node) sync* {
    if (node.isLeaf) {
      yield node;
      return;
    }
    for (final child in node.children) {
      yield* _leavesOf(child);
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
    throw ParquetUnsupportedException(
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
    throw ParquetUnsupportedException(
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
