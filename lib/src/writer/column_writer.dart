/// Writes one shredded leaf column chunk: dictionary/data pages, headers,
/// compression and column-level metadata.
library;

import 'dart:typed_data';

import '../encodings/bitpacking.dart' show requiredBits;
import '../format/enums.dart';
import '../format/thrift.dart';
import '../schema/schema.dart';
import '../util/bytes.dart';
import 'encoders.dart';
import 'shredder.dart';
import 'writer_options.dart';

/// Result of writing one column chunk.
final class ColumnChunkWriteResult {
  /// Creates a result.
  const ColumnChunkWriteResult({
    required this.metadata,
    required this.totalUncompressedSize,
    required this.totalCompressedSize,
  });

  /// Thrift `ColumnChunk` map (fields 2 and 3).
  final Map<int, Object?> metadata;

  /// Total uncompressed chunk size, including page headers.
  final int totalUncompressedSize;

  /// Total compressed chunk size, including page headers.
  final int totalCompressedSize;
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
    final pageStats = options.writeStatistics
        ? _computeStats(
            pageValues,
            type,
            nullCount: nullCount,
            unsigned: _isUnsigned(descriptor),
          )
        : null;
    final pageStatsMap = pageStats == null
        ? null
        : _statisticsToThrift(pageStats, type, descriptor);

    final Uint8List valueBytes;
    if (dictionary == null) {
      valueBytes = encodePlainValues(pageValues, type, typeLength: typeLength);
    } else {
      valueBytes = encodeDictionaryIndices(
        indices!.sublist(presentStart, presentEnd),
        dictionary.bitWidth,
      );
    }

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
        numRows: _countRows(leaf, range, maxRep),
        encoding: valueEncoding,
        definitionLevelsByteLength: defBytes.length,
        repetitionLevelsByteLength: repBytes.length,
        statistics: pageStatsMap,
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
      header = _dataPageV1Header(
        uncompressedSize: pageBody.length,
        compressedSize: compressed.length,
        numValues: range.end - range.start,
        encoding: valueEncoding,
        statistics: pageStatsMap,
      );
      body = compressed.asUint8List();
      totalUncompressed += header.length + pageBody.length;
      totalCompressed += header.length + body.length;
    }
    out
      ..writeBytes(header)
      ..writeBytes(Bytes.fromUint8List(body));
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
  );
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
    8: dataHeader,
  });
  return writer.toBytes();
}

Bytes _dictionaryPageHeader({
  required int uncompressedSize,
  required int compressedSize,
  required int numValues,
}) {
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: ThriftI32(PageType.dictionaryPage.value),
    2: ThriftI32(uncompressedSize),
    3: ThriftI32(compressedSize),
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
