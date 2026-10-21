/// Standalone (footer-less) FileMetaData codec.
///
/// A Parquet footer stores a single Thrift `FileMetaData` struct between the
/// column data and the trailer. These helpers serialise and parse that struct
/// on its own, so callers can keep metadata in a sidecar (for example two
/// mirrored `.meta` slots) and pass it back to [ParquetWriter.resume].
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../format/parquet_format.dart';
import '../format/thrift.dart';
import '../util/errors.dart';

/// Serialises [metadata] as a bare Thrift-compact `FileMetaData` struct.
///
/// The result contains no `PAR1` magic and no footer length; it is exactly the
/// payload that `readFileMetaData` accepts.
Uint8List writeFileMetaData(FileMetaData metadata) {
  final writer = ThriftCompactWriter();
  writer.writeStruct(fileMetaDataToThrift(metadata));
  return Uint8List.fromList(writer.toBytes().asUint8List());
}

/// Converts typed [metadata] to the Thrift-writer map representation.
///
/// This is the same structure used by the footer writer; callers that need to
/// merge or resume metadata can use it without serialising a file.
Map<int, Object?> fileMetaDataToThrift(FileMetaData metadata) =>
    _fileMetadata(metadata);

/// Parses a bare Thrift-compact `FileMetaData` struct.
///
/// This accepts the output of [writeFileMetaData], and the same bytes that sit
/// inside a Parquet footer.
FileMetaData readFileMetaData(Uint8List bytes) {
  if (bytes.isEmpty) {
    throw ParquetFormatException('empty FileMetaData');
  }
  final reader = ThriftCompactReader.fromBytes(Bytes.fromUint8List(bytes));
  return FileMetaData.fromThrift(reader.readStruct());
}

Map<int, Object?> _fileMetadata(FileMetaData metadata) {
  if (metadata.encryptionAlgorithm != null) {
    throw ParquetUnsupportedException(
      'writing encrypted FileMetaData is not supported',
    );
  }
  return <int, Object?>{
    1: ThriftI32(metadata.version),
    2: TList(TCompactType.struct, [
      for (final element in metadata.schema) _schemaElement(element),
    ]),
    3: ThriftI64(metadata.numRows),
    4: TList(TCompactType.struct, [
      for (final group in metadata.rowGroups) _rowGroup(group),
    ]),
    if (metadata.keyValueMetadata != null)
      5: TList(TCompactType.struct, [
        for (final kv in metadata.keyValueMetadata!) _keyValue(kv),
      ]),
    if (metadata.createdBy != null) 6: metadata.createdBy,
    if (metadata.columnOrders != null)
      7: TList(TCompactType.struct, [
        for (final order in metadata.columnOrders!) _columnOrder(order),
      ]),
    if (metadata.footerSigningKeyMetadata != null)
      9: metadata.footerSigningKeyMetadata,
  };
}

Map<int, Object?> _schemaElement(SchemaElement element) => <int, Object?>{
  if (element.type != null) 1: ThriftI32(element.type!.value),
  if (element.typeLength != null) 2: ThriftI32(element.typeLength!),
  if (element.repetitionType != null)
    3: ThriftI32(element.repetitionType!.value),
  4: element.name,
  if (element.numChildren != null) 5: ThriftI32(element.numChildren!),
  if (element.convertedType != null) 6: ThriftI32(element.convertedType!.value),
  if (element.scale != null) 7: ThriftI32(element.scale!),
  if (element.precision != null) 8: ThriftI32(element.precision!),
  if (element.fieldId != null) 9: ThriftI32(element.fieldId!),
  if (element.logicalType != null) 10: _logicalType(element.logicalType!),
};

Map<int, Object?> _rowGroup(RowGroup group) => <int, Object?>{
  1: TList(TCompactType.struct, [
    for (final chunk in group.columns) _columnChunk(chunk),
  ]),
  2: ThriftI64(group.totalByteSize),
  3: ThriftI64(group.numRows),
  if (group.sortingColumns != null)
    4: TList(TCompactType.struct, [
      for (final sort in group.sortingColumns!) _sortingColumn(sort),
    ]),
  if (group.fileOffset != null) 5: ThriftI64(group.fileOffset!),
  if (group.totalCompressedSize != null)
    6: ThriftI64(group.totalCompressedSize!),
  if (group.ordinal != null) 7: ThriftI16(group.ordinal!),
};

Map<int, Object?> _columnChunk(ColumnChunk chunk) => <int, Object?>{
  if (chunk.filePath != null) 1: chunk.filePath,
  if (chunk.fileOffset != null) 2: ThriftI64(chunk.fileOffset!),
  3: _columnMetaData(chunk.metaData),
  if (chunk.offsetIndexOffset != null) 4: ThriftI64(chunk.offsetIndexOffset!),
  if (chunk.offsetIndexLength != null) 5: ThriftI32(chunk.offsetIndexLength!),
  if (chunk.columnIndexOffset != null) 6: ThriftI64(chunk.columnIndexOffset!),
  if (chunk.columnIndexLength != null) 7: ThriftI32(chunk.columnIndexLength!),
};

Map<int, Object?> _columnMetaData(ColumnMetaData metadata) => <int, Object?>{
  1: ThriftI32(metadata.type.value),
  2: TList(TCompactType.i32, [
    for (final encoding in metadata.encodings) ThriftI32(encoding.value),
  ]),
  3: TList(TCompactType.binary, [
    for (final path in metadata.pathInSchema) path,
  ]),
  4: ThriftI32(metadata.codec.value),
  5: ThriftI64(metadata.numValues),
  6: ThriftI64(metadata.totalUncompressedSize),
  7: ThriftI64(metadata.totalCompressedSize),
  if (metadata.keyValueMetadata != null)
    8: TList(TCompactType.struct, [
      for (final kv in metadata.keyValueMetadata!) _keyValue(kv),
    ]),
  if (metadata.dataPageOffset != null) 9: ThriftI64(metadata.dataPageOffset!),
  if (metadata.indexPageOffset != null)
    10: ThriftI64(metadata.indexPageOffset!),
  if (metadata.dictionaryPageOffset != null)
    11: ThriftI64(metadata.dictionaryPageOffset!),
  if (metadata.statistics != null) 12: _statistics(metadata.statistics!),
  if (metadata.encodingStats != null)
    13: TList(TCompactType.struct, [
      for (final stat in metadata.encodingStats!) _encodingStats(stat),
    ]),
  if (metadata.bloomFilterOffset != null)
    14: ThriftI64(metadata.bloomFilterOffset!),
  if (metadata.bloomFilterLength != null)
    15: ThriftI32(metadata.bloomFilterLength!),
  if (metadata.sizeStatistics != null)
    16: _sizeStatistics(metadata.sizeStatistics!),
};

Map<int, Object?> _statistics(Statistics statistics) => <int, Object?>{
  if (statistics.max != null) 1: statistics.max,
  if (statistics.min != null) 2: statistics.min,
  if (statistics.nullCount != null) 3: ThriftI64(statistics.nullCount!),
  if (statistics.distinctCount != null) 4: ThriftI64(statistics.distinctCount!),
  if (statistics.maxValue != null) 5: statistics.maxValue,
  if (statistics.minValue != null) 6: statistics.minValue,
  if (statistics.isMaxValueExact != null) 7: statistics.isMaxValueExact,
  if (statistics.isMinValueExact != null) 8: statistics.isMinValueExact,
  if (statistics.nanCount != null) 9: ThriftI64(statistics.nanCount!),
};

Map<int, Object?> _encodingStats(PageEncodingStats stats) => <int, Object?>{
  1: ThriftI32(stats.pageType.value),
  2: ThriftI32(stats.encoding.value),
  3: ThriftI32(stats.count),
};

Map<int, Object?> _sizeStatistics(SizeStatistics statistics) => <int, Object?>{
  if (statistics.unencodedByteArrayDataBytes != null)
    1: ThriftI64(statistics.unencodedByteArrayDataBytes!),
  if (statistics.repetitionLevelHistogram != null)
    2: TList(TCompactType.i64, [
      for (final value in statistics.repetitionLevelHistogram!)
        ThriftI64(value),
    ]),
  if (statistics.definitionLevelHistogram != null)
    3: TList(TCompactType.i64, [
      for (final value in statistics.definitionLevelHistogram!)
        ThriftI64(value),
    ]),
};

Map<int, Object?> _keyValue(KeyValue kv) => <int, Object?>{
  1: kv.key,
  if (kv.value != null) 2: kv.value,
};

Map<int, Object?> _sortingColumn(SortingColumn column) => <int, Object?>{
  1: ThriftI32(column.columnIdx),
  2: column.descending,
  3: column.nullsFirst,
};

Map<int, Object?> _columnOrder(ColumnOrder order) => <int, Object?>{
  switch (order.kind) {
    ColumnOrderKind.typeOrder => 1,
    ColumnOrderKind.ieee754TotalOrder => 2,
    ColumnOrderKind.int96TimestampOrder => 3,
  }: <int, Object?>{},
};

Map<int, Object?> _logicalType(LogicalType logical) {
  final kind = logical.kind;
  final id = switch (kind) {
    LogicalTypeKind.string => 1,
    LogicalTypeKind.map => 2,
    LogicalTypeKind.list => 3,
    LogicalTypeKind.enumType => 4,
    LogicalTypeKind.decimal => 5,
    LogicalTypeKind.date => 6,
    LogicalTypeKind.time => 7,
    LogicalTypeKind.timestamp => 8,
    LogicalTypeKind.integer => 10,
    LogicalTypeKind.unknown => 11,
    LogicalTypeKind.json => 12,
    LogicalTypeKind.bson => 13,
    LogicalTypeKind.uuid => 14,
    LogicalTypeKind.float16 => 15,
    LogicalTypeKind.variant => 16,
    LogicalTypeKind.geometry => 17,
    LogicalTypeKind.geography => 18,
    LogicalTypeKind.file => 19,
  };
  final payload = switch (logical) {
    DecimalLogicalType() => <int, Object?>{
      1: ThriftI32(logical.scale),
      2: ThriftI32(logical.precision),
    },
    TimeLogicalType() => <int, Object?>{
      1: logical.isAdjustedToUtc,
      2: _timeUnit(logical.unit),
    },
    TimestampLogicalType() => <int, Object?>{
      1: logical.isAdjustedToUtc,
      2: _timeUnit(logical.unit),
    },
    IntegerLogicalType() => <int, Object?>{
      1: ThriftI32(logical.bitWidth),
      2: logical.isSigned,
    },
    VariantLogicalType() => <int, Object?>{
      if (logical.specificationVersion != null)
        1: ThriftI32(logical.specificationVersion!),
    },
    GeometryLogicalType() => <int, Object?>{
      if (logical.crs != null) 1: logical.crs,
    },
    GeographyLogicalType() => <int, Object?>{
      if (logical.crs != null) 1: logical.crs,
      if (logical.algorithm != null) 2: ThriftI32(logical.algorithm!.value),
    },
    _ => const <int, Object?>{},
  };
  return <int, Object?>{id: payload};
}

Map<int, Object?> _timeUnit(TimeUnit unit) => <int, Object?>{
  switch (unit) {
    TimeUnit.millis => 1,
    TimeUnit.micros => 2,
    TimeUnit.nanos => 3,
  }: <int, Object?>{},
};
