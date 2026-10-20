/// Parquet writer: assembles column chunks, row groups and the file footer.
///
/// ```dart
/// final writer = ParquetWriter(schema: table.schema);
/// writer.add(table);
/// final bytes = writer.finish();
/// ```
///
/// The output is a normal Parquet file: `PAR1`, column chunks (dictionary
/// pages followed by data pages), the Thrift-compact `FileMetaData` footer, the
/// 4-byte little-endian footer length and the trailing `PAR1` magic.
library;

import 'dart:typed_data';

import '../format/thrift.dart';
import '../schema/schema.dart';
import '../table/table.dart';
import '../util/bytes.dart';
import 'column_writer.dart';
import 'page_index_writer.dart';
import 'schema_encoder.dart';
import 'shredder.dart';
import 'writer_options.dart';

export 'file_output.dart';
export 'schema_builder.dart';
export 'writer_options.dart';

/// Streaming Parquet writer: every [add] call becomes one row group.
final class ParquetWriter {
  /// Creates a writer for [schema].
  ///
  /// Throws [ArgumentError] for invalid [options] and
  /// [ParquetUnsupportedException] for schema shapes the matching reader cannot
  /// reconstruct.
  ParquetWriter({
    required this.schema,
    this.options = const ParquetWriterOptions(),
  }) {
    options.validate();
    validateSchemaShape(schema);
    _schemaElements = encodeSchemaElements(schema);
    _out.writeUint8List(_magic);
  }

  /// Target schema.
  final ParquetSchema schema;

  /// Writer options.
  final ParquetWriterOptions options;

  static const List<int> _magic = <int>[0x50, 0x41, 0x52, 0x31]; // PAR1

  final ByteWriter _out = ByteWriter();
  late final List<Map<int, Object?>> _schemaElements;
  final List<Map<int, Object?>> _rowGroups = <Map<int, Object?>>[];
  final List<List<ColumnChunkWriteResult>> _rowGroupChunks =
      <List<ColumnChunkWriteResult>>[];
  int _numRows = 0;
  int _rowGroupOrdinal = 0;
  bool _finished = false;

  /// Number of rows added so far (across all row groups).
  int get numRows => _numRows;

  /// Number of row groups added so far.
  int get numRowGroups => _rowGroups.length;

  /// Adds [table] as one row group.
  ///
  /// Validates that the table's columns match [schema] in count, names and
  /// column shapes, then shreds and writes the row group immediately.
  void add(ParquetTable table) {
    if (_finished) {
      throw StateError('ParquetWriter.finish() has already been called');
    }
    validateTableShape(table, schema);
    final leaves = shredTable(
      table,
      schema,
      fastFlat: !options.writeBloomFilter,
    );

    final rowGroupStart = _out.length;
    final chunks = <Map<int, Object?>>[];
    final chunkResults = <ColumnChunkWriteResult>[];
    var totalByteSize = 0;
    var totalCompressedSize = 0;
    for (final leaf in leaves) {
      final result = writeLeafChunk(leaf, _out, options);
      chunkResults.add(result);
      chunks.add(result.metadata);
      totalByteSize += result.totalUncompressedSize;
      totalCompressedSize += result.totalCompressedSize;
    }

    _rowGroupChunks.add(chunkResults);
    _rowGroups.add(<int, Object?>{
      1: TList(TCompactType.struct, chunks),
      2: ThriftI64(totalByteSize),
      3: ThriftI64(table.numRows),
      5: ThriftI64(rowGroupStart),
      6: ThriftI64(totalCompressedSize),
      7: ThriftI16(_rowGroupOrdinal++),
    });
    _numRows += table.numRows;
  }

  /// Serialises the footer and returns the complete file bytes.
  ///
  /// Subsequent calls throw [StateError].
  Uint8List finish() {
    if (_finished) {
      throw StateError('ParquetWriter.finish() has already been called');
    }
    _finished = true;

    if (options.writePageIndex) _writePageIndexes();
    if (options.writeBloomFilter) _writeBloomFilters();

    final footer = <int, Object?>{
      1: ThriftI32(1), // FileMetaData.version
      2: TList(TCompactType.struct, _schemaElements),
      3: ThriftI64(_numRows),
      4: TList(TCompactType.struct, _rowGroups),
      if (options.keyValueMetadata != null)
        5: TList(TCompactType.struct, [
          for (final entry in options.keyValueMetadata!.entries)
            <int, Object?>{1: entry.key, 2: entry.value},
        ]),
      6: options.createdBy,
      // Type-defined column order for every leaf; required for readers to
      // interpret min_value / max_value.
      7: TList(TCompactType.struct, [
        for (var i = 0; i < schema.numColumns; i++)
          <int, Object?>{1: <int, Object?>{}},
      ]),
    };

    final thrift = ThriftCompactWriter();
    thrift.writeStruct(footer);
    final footerBytes = thrift.toBytes();

    _out
      ..writeBytes(footerBytes)
      ..writeUint32LE(footerBytes.length)
      ..writeUint8List(_magic);
    return Uint8List.fromList(_out.bytes.asUint8List());
  }

  /// Appends every chunk's `OffsetIndex` (and `ColumnIndex` when available)
  /// after the column data and patches the `ColumnChunk` offsets.
  void _writePageIndexes() {
    for (final chunks in _rowGroupChunks) {
      for (final chunk in chunks) {
        final offsetIndex = encodeOffsetIndex(chunk.pages);
        final offsetIndexOffset = _out.length;
        _out.writeBytes(Bytes.fromUint8List(offsetIndex));
        chunk.metadata[4] = ThriftI64(offsetIndexOffset);
        chunk.metadata[5] = ThriftI32(offsetIndex.length);

        final columnIndex = chunk.columnIndex;
        if (columnIndex == null) continue;
        final bytes = encodeColumnIndex(columnIndex);
        final columnIndexOffset = _out.length;
        _out.writeBytes(Bytes.fromUint8List(bytes));
        chunk.metadata[6] = ThriftI64(columnIndexOffset);
        chunk.metadata[7] = ThriftI32(bytes.length);
      }
    }
  }

  /// Appends every chunk's Bloom filter and patches `ColumnMetaData` fields
  /// 14 (offset) and 15 (length).
  void _writeBloomFilters() {
    for (final chunks in _rowGroupChunks) {
      for (final chunk in chunks) {
        final bytes = chunk.bloomFilter;
        if (bytes == null) continue;
        final offset = _out.length;
        _out.writeBytes(Bytes.fromUint8List(bytes));
        final columnMetadata = chunk.metadata[3]! as Map<int, Object?>;
        columnMetadata[14] = ThriftI64(offset);
        columnMetadata[15] = ThriftI32(bytes.length);
      }
    }
  }
}

/// Writes [table] as a single-row-group Parquet file with [options].
Uint8List writeParquetTable(
  ParquetTable table, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
}) {
  final writer = ParquetWriter(schema: table.schema, options: options);
  writer.add(table);
  return writer.finish();
}
