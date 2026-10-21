/// File-backed streaming Parquet writer.
///
/// Unlike [ParquetWriter], which buffers the complete encoded file before
/// returning bytes, this writer appends each row group to a blit [BlitFile]
/// as soon as it is encoded. Only one column chunk is held in memory at a time.
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:blit/blit.dart' show BlitFile;

import '../format/parquet_format.dart';
import '../format/thrift.dart';
import '../metadata/file_metadata_codec.dart';
import '../schema/schema.dart';
import '../table/table.dart';
import '../util/bytes.dart';
import '../util/errors.dart';
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
/// final file = await blit.open('data.parquet');
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
    _resumedVersion = null;
    _resumedKeyValueMetadata = null;
    _resumedColumnOrders = null;
  }

  ParquetStreamWriter._resume(
    this.schema,
    this.file,
    this.options,
    Map<int, Object?> resumedRaw,
  ) : _resumedVersion = resumedRaw[1],
      _resumedKeyValueMetadata = resumedRaw[5],
      _resumedColumnOrders = resumedRaw[7] {
    _schemaElements = encodeSchemaElements(schema);
  }

  /// Resumes writing to an existing, footer-less Parquet prefix.
  ///
  /// [file] must start with `PAR1`; a file longer than four bytes must not end
  /// with the `PAR1` trailer (that means it already has a footer). The prefix
  /// is not truncated or rewritten: [position] is set to the current file
  /// length.
  ///
  /// [fileMetaData] describes the row groups already present in the prefix.
  /// Its row groups, row count, ordinal, version, key/value metadata and column
  /// orders are restored; [options] apply to row groups added from now on.
  static Future<ParquetStreamWriter> resume(
    ParquetSchema schema,
    BlitFile file,
    FileMetaData fileMetaData, {
    ParquetWriterOptions options = const ParquetWriterOptions(),
  }) async {
    options.validate();
    validateSchemaShape(schema);
    _requireMatchingSchema(
      schema,
      ParquetSchema.fromFileMetaData(fileMetaData),
    );

    final length = await file.length;
    if (length < 4) {
      throw ParquetFormatException(
        'Parquet prefix is too short to resume: $length bytes',
      );
    }
    if (!_isMagic(await _readExactly(file, 0, 4))) {
      throw ParquetFormatException('resume target does not start with PAR1');
    }
    if (length > 4 && _isMagic(await _readExactly(file, length - 4, 4))) {
      throw ParquetFormatException(
        'resume target already has a Parquet footer (PAR1 trailer)',
      );
    }

    final raw = fileMetaDataToThrift(fileMetaData);
    final writer = ParquetStreamWriter._resume(schema, file, options, raw);
    final rowGroups = raw[4];
    if (rowGroups is! TList) {
      throw ParquetFormatException('resume metadata has no row group list');
    }
    writer._rowGroups.addAll(rowGroups.values.cast<Map<int, Object?>>());
    var rows = 0;
    var ordinal = 0;
    for (var i = 0; i < writer._rowGroups.length; i++) {
      final group = writer._rowGroups[i];
      rows += _rawInt(group[3]);
      final groupOrdinal = group[7] == null ? i : _rawInt(group[7]);
      if (groupOrdinal >= ordinal) ordinal = groupOrdinal + 1;
    }
    if (fileMetaData.numRows != 0 && fileMetaData.numRows != rows) {
      throw ParquetFormatException(
        'resume metadata num_rows is ${fileMetaData.numRows}, '
        'row groups contain $rows',
      );
    }
    writer._numRows = rows;
    writer._rowGroupOrdinal = ordinal;
    writer._position = length;
    writer._started = true;
    return writer;
  }

  /// Target schema.
  final ParquetSchema schema;

  /// Destination file. Owned by the caller unless [finish] is called with
  /// `close: true`.
  final BlitFile file;

  /// Options for every row group.
  final ParquetWriterOptions options;

  static const List<int> _magic = <int>[0x50, 0x41, 0x52, 0x31];

  late final List<Map<int, Object?>> _schemaElements;
  final List<Map<int, Object?>> _rowGroups = <Map<int, Object?>>[];
  late final Object? _resumedVersion;
  late final Object? _resumedKeyValueMetadata;
  late final Object? _resumedColumnOrders;
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

  /// The metadata that [finish] will write, as a standalone [FileMetaData].
  ///
  /// This can be persisted as a crash-safe sidecar and passed to [resume].
  /// It is valid at any point before [finish] and throws [StateError]
  /// afterwards.
  FileMetaData get fileMetaData {
    if (_finished) {
      throw StateError('ParquetStreamWriter.finish() has already been called');
    }
    final thrift = ThriftCompactWriter();
    thrift.writeStruct(_footerStruct());
    return readFileMetaData(Uint8List.fromList(thrift.toBytes().asUint8List()));
  }

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
      fastFlat: !options.writeBloomFilter && !options.enableAlp,
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

    final thrift = ThriftCompactWriter();
    thrift.writeStruct(_footerStruct());
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

  /// Builds the `FileMetaData` Thrift map used by [fileMetaData] and
  /// [_finish].
  ///
  /// Resumed files keep their original version, key/value metadata and column
  /// orders; new row groups and `created_by` come from the current options.
  Map<int, Object?> _footerStruct() {
    final resumeKv = _resumedKeyValueMetadata;
    final keyValueMetadata = options.keyValueMetadata != null
        ? TList(TCompactType.struct, [
            for (final entry in options.keyValueMetadata!.entries)
              <int, Object?>{1: entry.key, 2: entry.value},
          ])
        : resumeKv;
    return <int, Object?>{
      1: _resumedVersion ?? ThriftI32(1),
      2: TList(TCompactType.struct, _schemaElements),
      3: ThriftI64(_numRows),
      4: TList(TCompactType.struct, _rowGroups),
      5: ?keyValueMetadata,
      6: options.createdBy,
      7:
          _resumedColumnOrders ??
          TList(TCompactType.struct, [
            for (var i = 0; i < schema.numColumns; i++)
              <int, Object?>{1: <int, Object?>{}},
          ]),
    };
  }

  Future<void> _writeAt(int offset, Uint8List bytes) async {
    await file.writeAt(offset, bytes);
  }
}

bool _isMagic(Uint8List bytes) =>
    bytes.length == 4 &&
    bytes[0] == 0x50 &&
    bytes[1] == 0x41 &&
    bytes[2] == 0x52 &&
    bytes[3] == 0x31;

Future<Uint8List> _readExactly(BlitFile file, int offset, int length) async {
  final bytes = await file.readAt(offset, length);
  if (bytes.length != length) {
    throw ParquetFormatException(
      'unexpected EOF reading $length bytes at $offset',
    );
  }
  return bytes.asUint8List();
}

int _rawInt(Object? value) {
  if (value is int) return value;
  if (value is ThriftI64) return value.value;
  if (value is ThriftI32) return value.value;
  if (value is ThriftI16) return value.value;
  throw ParquetFormatException(
    'expected an integer in resumed metadata, got ${value.runtimeType}',
  );
}

void _requireMatchingSchema(ParquetSchema expected, ParquetSchema actual) {
  _compareSchemaNode(expected.root, actual.root, expected.root.name);
}

void _compareSchemaNode(SchemaNode a, SchemaNode b, String path) {
  Never mismatch(String detail) =>
      throw ParquetFormatException('resume schema mismatch at $path: $detail');

  if (a.name != b.name) mismatch('name "${a.name}" != "${b.name}"');
  if (a.repetition != b.repetition) {
    mismatch('repetition ${a.repetition} != ${b.repetition}');
  }
  if (a.physicalType != b.physicalType) {
    mismatch('physical type ${a.physicalType} != ${b.physicalType}');
  }
  if (a.typeLength != b.typeLength) {
    mismatch('type length ${a.typeLength} != ${b.typeLength}');
  }
  if (a.convertedType != b.convertedType) {
    mismatch('converted type ${a.convertedType} != ${b.convertedType}');
  }
  if (a.logicalType.runtimeType != b.logicalType.runtimeType ||
      a.logicalType != b.logicalType) {
    mismatch('logical type ${a.logicalType} != ${b.logicalType}');
  }
  if (a.scale != b.scale || a.precision != b.precision) {
    mismatch(
      'scale/precision ${a.scale}/${a.precision} != '
      '${b.scale}/${b.precision}',
    );
  }
  if (a.children.length != b.children.length) {
    mismatch('child count ${a.children.length} != ${b.children.length}');
  }
  for (var i = 0; i < a.children.length; i++) {
    _compareSchemaNode(
      a.children[i],
      b.children[i],
      '$path.${a.children[i].name}',
    );
  }
}
