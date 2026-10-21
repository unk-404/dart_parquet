/// File-backed streaming Parquet writer.
///
/// Unlike [ParquetWriter], which buffers the complete encoded file before
/// returning bytes, this writer appends each row group to a blit [BytesFile]
/// as soon as it is encoded. Only one column chunk is held in memory at a time.
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:blit/blit.dart' show BytesFile;

import '../format/thrift.dart';
import '../schema/schema.dart';
import '../table/table.dart';
import '../util/bytes.dart';
import 'column_writer.dart';
import 'page_index_writer.dart';
import 'schema_encoder.dart';
import 'shredder.dart';
import 'writer_options.dart';

/// Writes a Parquet file to [file] one row group at a time.
///
/// The writer does not close [file] by default; pass `close: true` to
/// [finish] when the file handle is owned by the writer. Operations are
/// serialised, so multiple [add] calls can be issued without awaiting each one.
///
/// ```dart
/// final file = await openBytesFile('data.parquet', readOnly: false);
/// final writer = ParquetStreamWriter(schema, file);
/// await writer.add(table);
/// await writer.finish(close: true);
/// ```
final class ParquetStreamWriter {
  /// Creates a stream writer for [schema] on an already-open [file].
  ParquetStreamWriter(
    this.schema,
    this.file, {
    this.options = const ParquetWriterOptions(),
  }) {
    options.validate();
    validateSchemaShape(schema);
    _schemaElements = encodeSchemaElements(schema);
  }

  /// Target schema.
  final ParquetSchema schema;

  /// Destination file. Owned by the caller unless [finish] is called with
  /// `close: true`.
  final BytesFile file;

  /// Options for every row group.
  final ParquetWriterOptions options;

  static const List<int> _magic = <int>[0x50, 0x41, 0x52, 0x31];

  late final List<Map<int, Object?>> _schemaElements;
  final List<Map<int, Object?>> _rowGroups = <Map<int, Object?>>[];
  int _numRows = 0;
  int _rowGroupOrdinal = 0;
  int _position = 0;
  bool _started = false;
  bool _finished = false;
  Future<void> _tail = Future<void>.value();

  /// Rows added so far.
  int get numRows => _numRows;

  /// Row groups added so far.
  int get numRowGroups => _rowGroups.length;

  /// Current file write position.
  int get position => _position;

  /// Appends [table] as one row group.
  Future<void> add(ParquetTable table) => _enqueue(() => _add(table));

  /// Writes the footer, flushes, and optionally closes [file].
  ///
  /// After this call no more row groups can be added.
  Future<void> finish({bool close = false}) =>
      _enqueue(() => _finish(close: close));

  /// Finishes and closes the file.
  Future<void> close() => finish(close: true);

  Future<void> _enqueue(Future<void> Function() operation) {
    final result = _tail.then((_) => operation());
    _tail = result.catchError((Object _) {});
    return result;
  }

  Future<void> _ensureStarted() async {
    if (_started) return;
    await file.truncate(0);
    await _writeAt(0, Uint8List.fromList(_magic));
    _position = _magic.length;
    _started = true;
  }

  Future<void> _add(ParquetTable table) async {
    if (_finished) {
      throw StateError('ParquetStreamWriter.finish() has already been called');
    }
    await _ensureStarted();
    validateTableShape(table, schema);
    final leaves = shredTable(
      table,
      schema,
      fastFlat: !options.writeBloomFilter,
    );

    final rowGroupStart = _position;
    final chunks = <Map<int, Object?>>[];
    var totalUncompressed = 0;
    var totalCompressed = 0;
    for (final leaf in leaves) {
      // One chunk at a time: encode, write, then drop its bytes.
      final buffer = ByteWriter();
      final result = writeLeafChunk(leaf, buffer, options);
      final bytes = buffer.bytes.asUint8List();
      final chunkStart = _position;
      await _writeAt(chunkStart, bytes);
      _position += bytes.length;
      _patchChunkOffsets(result, chunkStart);
      await _writeChunkIndexes(result);
      chunks.add(result.metadata);
      totalUncompressed += result.totalUncompressedSize;
      totalCompressed += result.totalCompressedSize;
    }

    _rowGroups.add(<int, Object?>{
      1: TList(TCompactType.struct, chunks),
      2: ThriftI64(totalUncompressed),
      3: ThriftI64(table.numRows),
      5: ThriftI64(rowGroupStart),
      6: ThriftI64(totalCompressed),
      7: ThriftI16(_rowGroupOrdinal++),
    });
    _numRows += table.numRows;
  }

  /// `writeLeafChunk` records offsets relative to a fresh buffer; translate
  /// them to absolute file offsets.
  void _patchChunkOffsets(ColumnChunkWriteResult result, int chunkStart) {
    result.metadata[2] = ThriftI64(chunkStart);
    final meta = result.metadata[3]! as Map<int, Object?>;
    _shift(meta, 9, chunkStart);
    _shift(meta, 10, chunkStart);
    _shift(meta, 11, chunkStart);
  }

  void _shift(Map<int, Object?> map, int field, int delta) {
    final value = map[field];
    if (value is ThriftI64) {
      map[field] = ThriftI64(value.value + delta);
    }
  }

  Future<void> _writeChunkIndexes(ColumnChunkWriteResult result) async {
    if (options.writePageIndex) {
      final pages = [
        for (final page in result.pages)
          PageIndexEntry(
            offset: page.offset + (result.metadata[2]! as ThriftI64).value,
            compressedSize: page.compressedSize,
            firstRowIndex: page.firstRowIndex,
            nullCount: page.nullCount,
            nullPage: page.nullPage,
            minValue: page.minValue,
            maxValue: page.maxValue,
          ),
      ];
      final offsetIndex = encodeOffsetIndex(pages);
      final offset = _position;
      await _writeAt(offset, Uint8List.fromList(offsetIndex));
      _position += offsetIndex.length;
      result.metadata[4] = ThriftI64(offset);
      result.metadata[5] = ThriftI32(offsetIndex.length);

      final columnIndex = result.columnIndex;
      if (columnIndex != null) {
        final bytes = encodeColumnIndex(
          ColumnIndexData(
            pages: pages,
            boundaryOrder: columnIndex.boundaryOrder,
          ),
        );
        final columnOffset = _position;
        await _writeAt(columnOffset, Uint8List.fromList(bytes));
        _position += bytes.length;
        result.metadata[6] = ThriftI64(columnOffset);
        result.metadata[7] = ThriftI32(bytes.length);
      }
    }
    if (options.writeBloomFilter) {
      final bytes = result.bloomFilter;
      if (bytes != null) {
        final offset = _position;
        await _writeAt(offset, bytes);
        _position += bytes.length;
        final meta = result.metadata[3]! as Map<int, Object?>;
        meta[14] = ThriftI64(offset);
        meta[15] = ThriftI32(bytes.length);
      }
    }
  }

  Future<void> _finish({required bool close}) async {
    if (_finished) {
      throw StateError('ParquetStreamWriter.finish() has already been called');
    }
    await _ensureStarted();
    _finished = true;

    final footer = <int, Object?>{
      1: ThriftI32(1),
      2: TList(TCompactType.struct, _schemaElements),
      3: ThriftI64(_numRows),
      4: TList(TCompactType.struct, _rowGroups),
      if (options.keyValueMetadata != null)
        5: TList(TCompactType.struct, [
          for (final entry in options.keyValueMetadata!.entries)
            <int, Object?>{1: entry.key, 2: entry.value},
        ]),
      6: options.createdBy,
      7: TList(TCompactType.struct, [
        for (var i = 0; i < schema.numColumns; i++)
          <int, Object?>{1: <int, Object?>{}},
      ]),
    };
    final thrift = ThriftCompactWriter();
    thrift.writeStruct(footer);
    final footerBytes = Uint8List.fromList(thrift.toBytes().asUint8List());
    final trailer = ByteWriter()
      ..writeUint8List(footerBytes)
      ..writeUint32LE(footerBytes.length)
      ..writeUint8List(_magic);

    await _writeAt(_position, trailer.bytes.asUint8List());
    _position += trailer.length;
    await file.truncate(_position);
    await file.flush();
    if (close && !file.isClosed) await file.close();
  }

  Future<void> _writeAt(int offset, Uint8List bytes) async {
    var written = 0;
    while (written < bytes.length) {
      final count = await file.writeAt(offset + written, bytes, start: written);
      if (count <= 0) {
        throw StateError(
          'BytesFile.writeAt made no progress at offset ${offset + written}',
        );
      }
      written += count;
    }
  }
}
