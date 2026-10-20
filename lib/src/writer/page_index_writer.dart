/// Serializes Parquet `OffsetIndex` and `ColumnIndex` structs.
library;

import 'dart:typed_data';

import '../format/enums.dart';
import '../format/thrift.dart';

/// One data page's entry in the page index.
final class PageIndexEntry {
  /// Creates a page entry.
  const PageIndexEntry({
    required this.offset,
    required this.compressedSize,
    required this.firstRowIndex,
    required this.nullCount,
    required this.nullPage,
    this.minValue,
    this.maxValue,
  });

  /// Absolute file offset of the page header.
  final int offset;

  /// Compressed page size including the serialized page header.
  final int compressedSize;

  /// Index (inside the row group) of the first row in this page.
  final int firstRowIndex;

  /// Number of null values in the page.
  final int nullCount;

  /// Whether the page contains only nulls.
  final bool nullPage;

  /// Raw physical minimum value, or `null` when unavailable.
  final Uint8List? minValue;

  /// Raw physical maximum value, or `null` when unavailable.
  final Uint8List? maxValue;
}

/// Per-chunk data used to serialize a `ColumnIndex`.
final class ColumnIndexData {
  /// Creates column-index data.
  const ColumnIndexData({required this.pages, required this.boundaryOrder});

  /// Page entries, in the same order as the chunk's `OffsetIndex`.
  final List<PageIndexEntry> pages;

  /// Ordering of `min_values` / `max_values` across pages.
  final BoundaryOrder boundaryOrder;
}

/// Serializes an `OffsetIndex` (Thrift compact).
Uint8List encodeOffsetIndex(List<PageIndexEntry> pages) {
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: TList(TCompactType.struct, [
      for (final page in pages)
        <int, Object?>{
          1: ThriftI64(page.offset),
          2: ThriftI32(page.compressedSize),
          3: ThriftI64(page.firstRowIndex),
        },
    ]),
  });
  return writer.toBytes().asUint8List();
}

/// Serializes a `ColumnIndex` (Thrift compact).
Uint8List encodeColumnIndex(ColumnIndexData data) {
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: TList(TCompactType.boolTrue, [
      for (final page in data.pages) page.nullPage,
    ]),
    2: TList(TCompactType.binary, [
      for (final page in data.pages)
        page.nullPage ? Uint8List(0) : (page.minValue ?? Uint8List(0)),
    ]),
    3: TList(TCompactType.binary, [
      for (final page in data.pages)
        page.nullPage ? Uint8List(0) : (page.maxValue ?? Uint8List(0)),
    ]),
    4: ThriftI32(data.boundaryOrder.value),
    5: TList(TCompactType.i64, [
      for (final page in data.pages) ThriftI64(page.nullCount),
    ]),
  });
  return writer.toBytes().asUint8List();
}
