/// Parquet OffsetIndex / ColumnIndex (page index) structs.
library;

import 'dart:typed_data';

import '../format/enums.dart';
import '../format/thrift.dart';
import '../util/bytes.dart';
import '../util/errors.dart';

/// One page location in an [OffsetIndex].
final class PageLocation {
  const PageLocation({
    required this.offset,
    required this.compressedPageSize,
    required this.firstRowIndex,
  });

  /// Absolute file offset of the page header.
  final int offset;

  /// Compressed page size, including the header.
  final int compressedPageSize;

  /// First row index in this page.
  final int firstRowIndex;

  @override
  String toString() =>
      'PageLocation(offset: $offset, size: $compressedPageSize, '
      'firstRow: $firstRowIndex)';
}

/// Parsed `OffsetIndex`.
final class OffsetIndex {
  const OffsetIndex({
    required this.pageLocations,
    this.unencodedByteArrayDataBytes,
  });

  final List<PageLocation> pageLocations;
  final List<int>? unencodedByteArrayDataBytes;
}

/// Parsed `ColumnIndex`.
final class ColumnIndex {
  const ColumnIndex({
    required this.nullPages,
    required this.minValues,
    required this.maxValues,
    required this.boundaryOrder,
    this.nullCounts,
    this.repetitionLevelHistograms,
    this.definitionLevelHistograms,
  });

  final List<bool> nullPages;
  final List<Uint8List> minValues;
  final List<Uint8List> maxValues;
  final BoundaryOrder boundaryOrder;
  final List<int>? nullCounts;
  final List<int>? repetitionLevelHistograms;
  final List<int>? definitionLevelHistograms;
}

/// Parses an `OffsetIndex` Thrift struct.
OffsetIndex parseOffsetIndex(Bytes bytes) {
  final raw = ThriftCompactReader.fromBytes(bytes).readStruct();
  final locations = raw[1];
  if (locations is! List) {
    throw ParquetFormatException('OffsetIndex.page_locations is required');
  }
  return OffsetIndex(
    pageLocations: [
      for (final item in locations)
        if (item is Map<int, Object?>)
          PageLocation(
            offset: _requiredInt(item, 1, 'PageLocation.offset'),
            compressedPageSize: _requiredInt(
              item,
              2,
              'PageLocation.compressed_page_size',
            ),
            firstRowIndex: _requiredInt(
              item,
              3,
              'PageLocation.first_row_index',
            ),
          )
        else
          throw ParquetFormatException('invalid PageLocation'),
    ],
    unencodedByteArrayDataBytes: _optionalIntList(
      raw[2],
      'OffsetIndex.unencoded_byte_array_data_bytes',
    ),
  );
}

/// Parses a `ColumnIndex` Thrift struct.
ColumnIndex parseColumnIndex(Bytes bytes) {
  final raw = ThriftCompactReader.fromBytes(bytes).readStruct();
  final nullPages = raw[1];
  final mins = raw[2];
  final maxs = raw[3];
  if (nullPages is! List || mins is! List || maxs is! List) {
    throw ParquetFormatException(
      'ColumnIndex requires null_pages, min_values and max_values',
    );
  }
  final boundary = _requiredInt(raw, 4, 'ColumnIndex.boundary_order');
  return ColumnIndex(
    nullPages: [for (final value in nullPages) value == true],
    minValues: [
      for (final value in mins)
        if (value is Uint8List)
          value
        else
          throw ParquetFormatException('invalid ColumnIndex.min_values'),
    ],
    maxValues: [
      for (final value in maxs)
        if (value is Uint8List)
          value
        else
          throw ParquetFormatException('invalid ColumnIndex.max_values'),
    ],
    boundaryOrder: BoundaryOrder.values.firstWhere(
      (e) => e.value == boundary,
      orElse: () =>
          throw ParquetFormatException('unknown BoundaryOrder $boundary'),
    ),
    nullCounts: _optionalIntList(raw[5], 'ColumnIndex.null_counts'),
    repetitionLevelHistograms: _optionalIntList(
      raw[6],
      'ColumnIndex.repetition_level_histograms',
    ),
    definitionLevelHistograms: _optionalIntList(
      raw[7],
      'ColumnIndex.definition_level_histograms',
    ),
  );
}

int _requiredInt(Map<int, Object?> map, int id, String name) {
  final value = map[id];
  if (value is! int) {
    throw ParquetFormatException('$name is required');
  }
  return value;
}

List<int>? _optionalIntList(Object? value, String name) {
  if (value == null) return null;
  if (value is! List) {
    throw ParquetFormatException('$name must be a list');
  }
  return [
    for (final item in value)
      if (item is int)
        item
      else
        throw ParquetFormatException('$name contains a non-integer'),
  ];
}
