/// Writes one shredded leaf column chunk: dictionary/data pages, headers,
/// compression and column-level metadata.
library;

import 'dart:typed_data';

import '../encodings/bitpacking.dart' show requiredBits;
import '../format/enums.dart';
import '../format/thrift.dart';
import '../schema/schema.dart';
import '../util/bytes.dart';
import '../util/crc32.dart';
import 'bloom_filter.dart';
import 'encoders.dart';
import 'page_index_writer.dart';
import 'shredder.dart';
import 'writer_options.dart';

/// Result of writing one column chunk.
final class ColumnChunkWriteResult {
  /// Creates a result.
  const ColumnChunkWriteResult({
    required this.metadata,
    required this.totalUncompressedSize,
    required this.totalCompressedSize,
    required this.pages,
    this.columnIndex,
    this.bloomFilter,
  });

  /// Thrift `ColumnChunk` map (fields 2 and 3).
  final Map<int, Object?> metadata;

  /// Total uncompressed chunk size, including page headers.
  final int totalUncompressedSize;

  /// Total compressed chunk size, including page headers.
  final int totalCompressedSize;

  /// Data pages in file order, used to build the page index.
  final List<PageIndexEntry> pages;

  /// Per-page column index data, or `null` when the column order cannot
  /// represent the bounds (INT96 / all-NaN pages).
  final ColumnIndexData? columnIndex;

  /// Serialized Bloom filter (header followed by bitset), or `null`.
  final Uint8List? bloomFilter;
}

/// Encodes [leaf]'s pages into [out] and returns its `ColumnChunk` metadata.
///
/// [out]'s current length is the absolute offset of the chunk's first page.
ColumnChunkWriteResult writeLeafChunk(
  LeafData leaf,
  ByteWriter out,
  ParquetWriterOptions options,
) {
  final descriptor = leaf.descriptor;
  final type = descriptor.physicalType;
  final typeLength = descriptor.typeLength;
  final codec = options.codec;
  final dictionary = _planDictionary(leaf, options);
  final indices = dictionary == null
      ? null
      : [
          for (final value in leaf.values)
            dictionary.index[_ByteKey(value as Uint8List)]!,
        ];

  final maxDef = descriptor.maxDefLevel;
  final maxRep = descriptor.maxRepLevel;
  final defBits = requiredBits(maxDef);
  final repBits = requiredBits(maxRep);
  final entries = leaf.numEntries;

  // Value slot for every entry: present values are those at the leaf's max
  // definition level.
  final valueOffsets = List<int>.filled(entries + 1, 0);
  for (var i = 0; i < entries; i++) {
    final present = maxDef == 0 || leaf.defs[i] == maxDef;
    valueOffsets[i + 1] = valueOffsets[i] + (present ? 1 : 0);
  }

  final levelEstimate =
      (maxDef > 0 ? (defBits + 7) >> 3 : 0) +
      (maxRep > 0 ? (repBits + 7) >> 3 : 0);
  final dictValueEstimate = dictionary == null
      ? 0
      : (dictionary.bitWidth + 7) >> 3;

  final ranges = <_PageRange>[];
  if (entries == 0) {
    ranges.add(const _PageRange(0, 0));
  } else {
    var start = 0;
    var estimate = 0;
    for (var i = 0; i < entries; i++) {
      estimate += levelEstimate;
      final present = maxDef == 0 || leaf.defs[i] == maxDef;
      if (present) {
        estimate += dictionary == null
            ? plainValueSize(leaf.values[valueOffsets[i]], type)
            : (dictValueEstimate == 0 ? 1 : dictValueEstimate);
      }
      final next = i + 1;
      if (next < entries &&
          estimate >= options.pageSize &&
          _canBreakAt(leaf, next, maxRep)) {
        ranges.add(_PageRange(start, next));
        start = next;
        estimate = 0;
      }
    }
    if (start < entries) ranges.add(_PageRange(start, entries));
  }

  final chunkStart = out.length;
  var totalUncompressed = 0;
  var totalCompressed = 0;
  int? dictionaryPageOffset;
  final encodingStats = <Map<int, Object?>>[];

  if (dictionary != null) {
    dictionaryPageOffset = out.length;
    final dictionaryBody = encodePlainValues(
      dictionary.values,
      type,
      typeLength: typeLength,
    );
    final compressed = codec.encode(Bytes.fromUint8List(dictionaryBody));
    final header = _dictionaryPageHeader(
      uncompressedSize: dictionaryBody.length,
      compressedSize: compressed.length,
      numValues: dictionary.values.length,
      crc: options.writeCrc ? crc32(compressed.asUint8List()) : null,
    );
    out
      ..writeBytes(header)
      ..writeBytes(compressed);
    totalUncompressed += header.length + dictionaryBody.length;
    totalCompressed += header.length + compressed.length;
    encodingStats.add({
      1: ThriftI32(PageType.dictionaryPage.value),
      2: ThriftI32(Encoding.plain.value),
      3: ThriftI32(1),
    });
  }

  final dataPageOffset = out.length;
  var dataPageCount = 0;
  var firstRowIndex = 0;
  final pages = <PageIndexEntry>[];
  final pageType = options.dataPageVersion == 2
      ? PageType.dataPageV2
      : PageType.dataPage;
  final valueEncoding = dictionary != null
      ? Encoding.rleDictionary
      : Encoding.plain;

  for (final range in ranges) {
    final presentStart = valueOffsets[range.start];
    final presentEnd = valueOffsets[range.end];
    final pageValues = leaf.values.sublist(presentStart, presentEnd);
    final nullCount = (range.end - range.start) - pageValues.length;
    final pageStats = (options.writeStatistics || options.writePageIndex)
        ? _computeStats(
            pageValues,
            type,
            nullCount: nullCount,
            unsigned: _isUnsigned(descriptor),
          )
        : null;
    final pageStatsMap = pageStats == null || !options.writeStatistics
        ? null
        : _statisticsToThrift(pageStats, type, descriptor);
    final rowsInPage = _countRows(leaf, range, maxRep);

    final Uint8List valueBytes;
    if (dictionary == null) {
      valueBytes = encodePlainValues(pageValues, type, typeLength: typeLength);
    } else {
      valueBytes = encodeDictionaryIndices(
        indices!.sublist(presentStart, presentEnd),
        dictionary.bitWidth,
      );
    }

    final pageOffset = out.length;
    final Bytes header;
    final Uint8List body;
    if (options.dataPageVersion == 2) {
      final repBytes = maxRep > 0
          ? encodeRleBitPacked(
              leaf.reps.sublist(range.start, range.end),
              repBits,
            )
          : Uint8List(0);
      final defBytes = maxDef > 0
          ? encodeRleBitPacked(
              leaf.defs.sublist(range.start, range.end),
              defBits,
            )
          : Uint8List(0);
      final compressedValues = codec.encode(Bytes.fromUint8List(valueBytes));
      final writer = ByteWriter()
        ..writeUint8List(repBytes)
        ..writeUint8List(defBytes)
        ..writeBytes(compressedValues);
      final pageBody = writer.bytes.asUint8List();
      header = _dataPageV2Header(
        uncompressedSize: repBytes.length + defBytes.length + valueBytes.length,
        compressedSize: pageBody.length,
        numValues: range.end - range.start,
        numNulls: nullCount,
        numRows: rowsInPage,
        encoding: valueEncoding,
        definitionLevelsByteLength: defBytes.length,
        repetitionLevelsByteLength: repBytes.length,
        statistics: pageStatsMap,
        crc: options.writeCrc ? crc32(pageBody) : null,
      );
      body = pageBody;
      totalUncompressed +=
          header.length + repBytes.length + defBytes.length + valueBytes.length;
      totalCompressed += header.length + pageBody.length;
    } else {
      final writer = ByteWriter();
      if (maxRep > 0) {
        final repBytes = encodeRleBitPacked(
          leaf.reps.sublist(range.start, range.end),
          repBits,
        );
        writer
          ..writeUint32LE(repBytes.length)
          ..writeUint8List(repBytes);
      }
      if (maxDef > 0) {
        final defBytes = encodeRleBitPacked(
          leaf.defs.sublist(range.start, range.end),
          defBits,
        );
        writer
          ..writeUint32LE(defBytes.length)
          ..writeUint8List(defBytes);
      }
      writer.writeUint8List(valueBytes);
      final pageBody = writer.bytes.asUint8List();
      final compressed = codec.encode(Bytes.fromUint8List(pageBody));
      body = compressed.asUint8List();
      header = _dataPageV1Header(
        uncompressedSize: pageBody.length,
        compressedSize: body.length,
        numValues: range.end - range.start,
        encoding: valueEncoding,
        statistics: pageStatsMap,
        crc: options.writeCrc ? crc32(body) : null,
      );
      totalUncompressed += header.length + pageBody.length;
      totalCompressed += header.length + body.length;
    }
    out
      ..writeBytes(header)
      ..writeBytes(Bytes.fromUint8List(body));
    pages.add(
      PageIndexEntry(
        offset: pageOffset,
        compressedSize: header.length + body.length,
        firstRowIndex: firstRowIndex,
        nullCount: nullCount,
        nullPage: pageValues.isEmpty,
        minValue: pageStats?.min,
        maxValue: pageStats?.max,
      ),
    );
    firstRowIndex += rowsInPage;
    dataPageCount++;
  }

  encodingStats.add({
    1: ThriftI32(pageType.value),
    2: ThriftI32(valueEncoding.value),
    3: ThriftI32(dataPageCount),
  });

  final chunkStats = options.writeStatistics
      ? _computeStats(
          leaf.values,
          type,
          nullCount: entries - leaf.values.length,
          unsigned: _isUnsigned(descriptor),
          distinctCount: dictionary?.values.length,
        )
      : null;

  final encodings = <Encoding>{
    Encoding.plain,
    Encoding.rle,
    if (dictionary != null) Encoding.rleDictionary,
  }.toList()..sort((a, b) => a.value.compareTo(b.value));

  final columnIndex = options.writePageIndex
      ? _buildColumnIndex(pages, descriptor)
      : null;
  final bloomFilter = options.writeBloomFilter && leaf.values.isNotEmpty
      ? _buildBloomFilter(leaf.values, type, options)
      : null;

  final columnMetadata = <int, Object?>{
    1: ThriftI32(type.value),
    2: TList(TCompactType.i32, [for (final e in encodings) ThriftI32(e.value)]),
    3: TList(TCompactType.binary, descriptor.path),
    4: ThriftI32(options.compression.value),
    5: ThriftI64(entries),
    6: ThriftI64(totalUncompressed),
    7: ThriftI64(totalCompressed),
    9: ThriftI64(dataPageOffset),
    if (dictionaryPageOffset != null) 11: ThriftI64(dictionaryPageOffset),
    if (chunkStats != null)
      12: _statisticsToThrift(chunkStats, type, descriptor),
    13: TList(TCompactType.struct, encodingStats),
  };

  final chunkMetadata = <int, Object?>{
    2: ThriftI64(0), // deprecated ColumnChunk.file_offset
    3: columnMetadata,
  };

  assert(out.length - chunkStart == totalCompressed);
  return ColumnChunkWriteResult(
    metadata: chunkMetadata,
    totalUncompressedSize: totalUncompressed,
    totalCompressedSize: totalCompressed,
    pages: pages,
    columnIndex: columnIndex,
    bloomFilter: bloomFilter,
  );
}

ColumnIndexData? _buildColumnIndex(
  List<PageIndexEntry> pages,
  ColumnDescriptor descriptor,
) {
  final type = descriptor.physicalType;
  // INT96 uses TYPE_ORDER column order, for which writers must not emit a
  // ColumnIndex.
  if (type == PhysicalType.int96) return null;
  for (final page in pages) {
    if (page.nullPage) continue;
    // All-NaN FLOAT/DOUBLE pages (or any page whose bounds are unavailable)
    // force the whole ColumnIndex to be omitted.
    if (page.minValue == null || page.maxValue == null) return null;
  }
  return ColumnIndexData(
    pages: pages,
    boundaryOrder: _boundaryOrder(pages, descriptor),
  );
}

BoundaryOrder _boundaryOrder(
  List<PageIndexEntry> pages,
  ColumnDescriptor descriptor,
) {
  final comparable = <PageIndexEntry>[
    for (final page in pages)
      if (!page.nullPage && page.minValue != null && page.maxValue != null)
        page,
  ];
  if (comparable.length < 2) return BoundaryOrder.unordered;
  var ascending = true;
  var descending = true;
  for (var i = 1; i < comparable.length; i++) {
    final previous = comparable[i - 1];
    final current = comparable[i];
    final minOrder = _compareBounds(
      descriptor,
      previous.minValue!,
      current.minValue!,
    );
    final maxOrder = _compareBounds(
      descriptor,
      previous.maxValue!,
      current.maxValue!,
    );
    if (minOrder > 0 || maxOrder > 0) ascending = false;
    if (minOrder < 0 || maxOrder < 0) descending = false;
  }
  if (ascending) return BoundaryOrder.ascending;
  if (descending) return BoundaryOrder.descending;
  return BoundaryOrder.unordered;
}

/// Orders two raw statistics values using the column's logical ordering.
int _compareBounds(ColumnDescriptor descriptor, Uint8List a, Uint8List b) {
  final unsigned = _isUnsigned(descriptor);
  switch (descriptor.physicalType) {
    case PhysicalType.int32:
      final left = ByteData.sublistView(a).getInt32(0, Endian.little);
      final right = ByteData.sublistView(b).getInt32(0, Endian.little);
      return _compareInt32(left, right, unsigned);
    case PhysicalType.int64:
      return _compareInt64(_int64FromBounds(a), _int64FromBounds(b), unsigned);
    case PhysicalType.float:
      return ByteData.sublistView(a)
          .getFloat32(0, Endian.little)
          .compareTo(ByteData.sublistView(b).getFloat32(0, Endian.little));
    case PhysicalType.double:
      return ByteData.sublistView(a)
          .getFloat64(0, Endian.little)
          .compareTo(ByteData.sublistView(b).getFloat64(0, Endian.little));
    case PhysicalType.boolean:
      return a[0].compareTo(b[0]);
    case PhysicalType.byteArray:
    case PhysicalType.fixedLenByteArray:
      return _compareBytes(a, b);
    case PhysicalType.int96:
      // INT96 never produces a ColumnIndex.
      return 0;
  }
}

Int64Value _int64FromBounds(Uint8List bytes) {
  final data = ByteData.sublistView(bytes);
  return Int64Value(
    data.getUint32(0, Endian.little),
    data.getUint32(4, Endian.little),
  );
}

Uint8List _buildBloomFilter(
  List<Object?> values,
  PhysicalType type,
  ParquetWriterOptions options,
) {
  final filter = ParquetBloomFilter.create(
    numValues: values.length,
    fpp: options.bloomFilterFpp,
    maxBytes: options.bloomFilterMaxBytes,
  );
  for (final value in values) {
    filter.addBytes(_bloomValueBytes(value, type));
  }
  return filter.toBytes();
}

Uint8List _bloomValueBytes(Object? value, PhysicalType type) {
  if (type == PhysicalType.int96) return value as Uint8List;
  return encodeStatisticsValue(value, type);
}

bool _canBreakAt(LeafData leaf, int entry, int maxRep) {
  if (maxRep == 0) return true;
  return leaf.reps[entry] == 0;
}

int _countRows(LeafData leaf, _PageRange range, int maxRep) {
  if (maxRep == 0) return range.end - range.start;
  var rows = 0;
  for (var i = range.start; i < range.end; i++) {
    if (leaf.reps[i] == 0) rows++;
  }
  return rows;
}

bool _isUnsigned(ColumnDescriptor descriptor) {
  final logical = descriptor.logicalType;
  return logical is IntegerLogicalType && !logical.isSigned;
}

Bytes _dataPageV1Header({
  required int uncompressedSize,
  required int compressedSize,
  required int numValues,
  required Encoding encoding,
  required Map<int, Object?>? statistics,
  int? crc,
}) {
  final dataHeader = <int, Object?>{
    1: ThriftI32(numValues),
    2: ThriftI32(encoding.value),
    3: ThriftI32(Encoding.rle.value),
    4: ThriftI32(Encoding.rle.value),
  };
  if (statistics != null) dataHeader[5] = statistics;
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: ThriftI32(PageType.dataPage.value),
    2: ThriftI32(uncompressedSize),
    3: ThriftI32(compressedSize),
    if (crc != null) 4: ThriftI32(_signedInt32(crc)),
    5: dataHeader,
  });
  return writer.toBytes();
}

Bytes _dataPageV2Header({
  required int uncompressedSize,
  required int compressedSize,
  required int numValues,
  required int numNulls,
  required int numRows,
  required Encoding encoding,
  required int definitionLevelsByteLength,
  required int repetitionLevelsByteLength,
  required Map<int, Object?>? statistics,
  int? crc,
}) {
  final dataHeader = <int, Object?>{
    1: ThriftI32(numValues),
    2: ThriftI32(numNulls),
    3: ThriftI32(numRows),
    4: ThriftI32(encoding.value),
    5: ThriftI32(definitionLevelsByteLength),
    6: ThriftI32(repetitionLevelsByteLength),
    7: true,
  };
  if (statistics != null) dataHeader[8] = statistics;
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: ThriftI32(PageType.dataPageV2.value),
    2: ThriftI32(uncompressedSize),
    3: ThriftI32(compressedSize),
    if (crc != null) 4: ThriftI32(_signedInt32(crc)),
    8: dataHeader,
  });
  return writer.toBytes();
}

Bytes _dictionaryPageHeader({
  required int uncompressedSize,
  required int compressedSize,
  required int numValues,
  int? crc,
}) {
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: ThriftI32(PageType.dictionaryPage.value),
    2: ThriftI32(uncompressedSize),
    3: ThriftI32(compressedSize),
    if (crc != null) 4: ThriftI32(_signedInt32(crc)),
    7: <int, Object?>{
      1: ThriftI32(numValues),
      2: ThriftI32(Encoding.plain.value),
      3: false,
    },
  });
  return writer.toBytes();
}

// ---------------------------------------------------------------------------
// Dictionary planning
// ---------------------------------------------------------------------------

final class _DictionaryPlan {
  _DictionaryPlan(this.values, this.index, this.bitWidth);

  final List<Uint8List> values;
  final Map<_ByteKey, int> index;
  final int bitWidth;
}

_DictionaryPlan? _planDictionary(LeafData leaf, ParquetWriterOptions options) {
  if (!options.useDictionary) return null;
  final type = leaf.descriptor.physicalType;
  if (type != PhysicalType.byteArray &&
      type != PhysicalType.fixedLenByteArray) {
    return null;
  }
  if (leaf.values.isEmpty) return null;

  final index = <_ByteKey, int>{};
  final values = <Uint8List>[];
  var size = 0;
  for (final raw in leaf.values) {
    final bytes = raw as Uint8List;
    final key = _ByteKey(bytes);
    if (index.containsKey(key)) continue;
    index[key] = values.length;
    values.add(bytes);
    size += type == PhysicalType.byteArray ? 4 + bytes.length : bytes.length;
    if (size > options.dictionaryPageSizeLimit) return null;
  }
  if (values.isEmpty) return null;
  return _DictionaryPlan(values, index, requiredBits(values.length - 1));
}

/// Content-addressed key for byte-array dictionary lookups.
final class _ByteKey {
  _ByteKey(this.bytes) : _hash = _fnv1a(bytes);

  final Uint8List bytes;
  final int _hash;

  static int _fnv1a(Uint8List bytes) {
    var hash = 0x811C9DC5;
    for (final byte in bytes) {
      hash ^= byte;
      hash = (hash * 0x01000193) & 0xFFFFFFFF;
    }
    return hash;
  }

  @override
  int get hashCode => _hash;

  @override
  bool operator ==(Object other) {
    if (other is! _ByteKey || other.bytes.length != bytes.length) return false;
    for (var i = 0; i < bytes.length; i++) {
      if (other.bytes[i] != bytes[i]) return false;
    }
    return true;
  }
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

final class _ColumnStatistics {
  _ColumnStatistics({
    required this.nullCount,
    this.min,
    this.max,
    this.distinctCount,
  });

  final int nullCount;
  final Uint8List? min;
  final Uint8List? max;
  final int? distinctCount;
}

_ColumnStatistics _computeStats(
  List<Object?> values,
  PhysicalType type, {
  required int nullCount,
  bool unsigned = false,
  int? distinctCount,
}) {
  if (values.isEmpty) {
    return _ColumnStatistics(
      nullCount: nullCount,
      distinctCount: distinctCount,
    );
  }
  switch (type) {
    case PhysicalType.boolean:
      var anyFalse = false;
      for (final value in values) {
        if (!(value as bool)) {
          anyFalse = true;
          break;
        }
      }
      return _ColumnStatistics(
        nullCount: nullCount,
        min: Uint8List.fromList([anyFalse ? 0 : 1]),
        max: Uint8List.fromList([1]),
        distinctCount: distinctCount,
      );
    case PhysicalType.int32:
      var min = values.first as int;
      var max = min;
      for (final value in values.skip(1)) {
        final v = value as int;
        if (_compareInt32(v, min, unsigned) < 0) min = v;
        if (_compareInt32(v, max, unsigned) > 0) max = v;
      }
      return _ColumnStatistics(
        nullCount: nullCount,
        min: encodeStatisticsValue(min, type),
        max: encodeStatisticsValue(max, type),
        distinctCount: distinctCount,
      );
    case PhysicalType.int64:
      var min = values.first as Int64Value;
      var max = min;
      for (final value in values.skip(1)) {
        final v = value as Int64Value;
        if (_compareInt64(v, min, unsigned) < 0) min = v;
        if (_compareInt64(v, max, unsigned) > 0) max = v;
      }
      return _ColumnStatistics(
        nullCount: nullCount,
        min: encodeStatisticsValue(min, type),
        max: encodeStatisticsValue(max, type),
        distinctCount: distinctCount,
      );
    case PhysicalType.float:
    case PhysicalType.double:
      var min = double.infinity;
      var max = double.negativeInfinity;
      var sawValue = false;
      for (final value in values) {
        final v = value as double;
        if (v.isNaN) continue;
        sawValue = true;
        if (v < min) min = v;
        if (v > max) max = v;
      }
      if (!sawValue) {
        // A page that is entirely NaN has no meaningful min/max.
        return _ColumnStatistics(
          nullCount: nullCount,
          distinctCount: distinctCount,
        );
      }
      return _ColumnStatistics(
        nullCount: nullCount,
        min: encodeStatisticsValue(min, type),
        max: encodeStatisticsValue(max, type),
        distinctCount: distinctCount,
      );
    case PhysicalType.byteArray:
    case PhysicalType.fixedLenByteArray:
      var min = values.first as Uint8List;
      var max = min;
      for (final value in values.skip(1)) {
        final v = value as Uint8List;
        if (_compareBytes(v, min) < 0) min = v;
        if (_compareBytes(v, max) > 0) max = v;
      }
      return _ColumnStatistics(
        nullCount: nullCount,
        min: Uint8List.fromList(min),
        max: Uint8List.fromList(max),
        distinctCount: distinctCount,
      );
    case PhysicalType.int96:
      // INT96 statistics are optional; skip them entirely.
      return _ColumnStatistics(
        nullCount: nullCount,
        distinctCount: distinctCount,
      );
  }
}

int _compareInt32(int a, int b, bool unsigned) {
  if (unsigned) {
    return _unsigned32(a).compareTo(_unsigned32(b));
  }
  return a.compareTo(b);
}

/// Converts a signed INT32 Dart value to its unsigned magnitude.
///
/// `value & 0xFFFFFFFF` is avoided because bitwise operators are 32-bit signed
/// on the web.
int _unsigned32(int value) => value < 0 ? value + 0x100000000 : value;

/// Converts an unsigned 32-bit value (CRC) to the signed representation that
/// Thrift's i32 zigzag encoding expects.
int _signedInt32(int value) =>
    value >= 0x80000000 ? value - 0x100000000 : value;

int _compareInt64(Int64Value a, Int64Value b, bool unsigned) {
  if (unsigned) {
    final hi = a.hi.compareTo(b.hi);
    if (hi != 0) return hi;
    return a.lo.compareTo(b.lo);
  }
  final hi = a.signedHi.compareTo(b.signedHi);
  if (hi != 0) return hi;
  return a.lo.compareTo(b.lo);
}

int _compareBytes(Uint8List a, Uint8List b) {
  final shared = a.length < b.length ? a.length : b.length;
  for (var i = 0; i < shared; i++) {
    final diff = a[i] - b[i];
    if (diff != 0) return diff;
  }
  return a.length.compareTo(b.length);
}

Map<int, Object?> _statisticsToThrift(
  _ColumnStatistics statistics,
  PhysicalType type,
  ColumnDescriptor descriptor,
) {
  final fields = <int, Object?>{};
  if (statistics.min != null && statistics.max != null) {
    fields[1] = statistics.max; // deprecated max (signed ordering only)
    fields[2] = statistics.min; // deprecated min
    fields[5] = statistics.max; // max_value
    fields[6] = statistics.min; // min_value
    fields[7] = true;
    fields[8] = true;
    // The deprecated fields are only valid for types whose signed physical
    // ordering matches the column order.
    if (type == PhysicalType.byteArray ||
        type == PhysicalType.fixedLenByteArray ||
        type == PhysicalType.int96 ||
        _isUnsigned(descriptor)) {
      fields.remove(1);
      fields.remove(2);
    }
  }
  fields[3] = ThriftI64(statistics.nullCount);
  final distinctCount = statistics.distinctCount;
  if (distinctCount != null) {
    fields[4] = ThriftI64(distinctCount);
  }
  return fields;
}

final class _PageRange {
  const _PageRange(this.start, this.end);

  final int start;
  final int end;
}
