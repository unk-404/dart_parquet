/// Helpers for concatenating typed value chunks decoded from multiple pages.
library;

import 'dart:math' as math;
import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../encodings/dictionary.dart' show decodeDictionaryIndices;
import '../encodings/plain.dart'
    show
        ByteArrayData,
        FixedLenData,
        decodePlainFloat32,
        decodePlainFloat64,
        decodePlainInt32,
        decodePlainInt64;
import '../format/enums.dart' show PhysicalType;
import '../schema/schema.dart' show ColumnDescriptor;
import '../util/errors.dart';
import '../util/int64.dart';

/// Concatenates primitive value chunks of the same runtime type.
///
/// Supported chunk types: [Int32List], [Int64Buffer], [Float32List],
/// [Float64List], [Uint8List] (one byte per boolean), [ByteArrayData],
/// [FixedLenData].
Object concatValueChunks(List<Object> chunks) {
  if (chunks.isEmpty) return Int32List(0);
  if (chunks.length == 1) return chunks.first;
  final first = chunks.first;
  if (first is Int32List) {
    var total = 0;
    for (final c in chunks) {
      total += (c as Int32List).length;
    }
    final out = Int32List(total);
    var pos = 0;
    for (final c in chunks) {
      final v = c as Int32List;
      out.setRange(pos, pos + v.length, v);
      pos += v.length;
    }
    return out;
  }
  if (first is Int64Buffer) {
    var total = 0;
    for (final c in chunks) {
      total += (c as Int64Buffer).length;
    }
    final out = Int64Buffer(total);
    var pos = 0;
    for (final c in chunks) {
      final v = c as Int64Buffer;
      for (var i = 0; i < v.length; i++) {
        final limbs = v.getLimbs(i);
        out.setLimbs(pos + i, limbs.$1, limbs.$2);
      }
      pos += v.length;
    }
    return out;
  }
  if (first is Float32List) {
    var total = 0;
    for (final c in chunks) {
      total += (c as Float32List).length;
    }
    final out = Float32List(total);
    var pos = 0;
    for (final c in chunks) {
      final v = c as Float32List;
      out.setRange(pos, pos + v.length, v);
      pos += v.length;
    }
    return out;
  }
  if (first is Float64List) {
    var total = 0;
    for (final c in chunks) {
      total += (c as Float64List).length;
    }
    final out = Float64List(total);
    var pos = 0;
    for (final c in chunks) {
      final v = c as Float64List;
      out.setRange(pos, pos + v.length, v);
      pos += v.length;
    }
    return out;
  }
  if (first is Uint8List) {
    var total = 0;
    for (final c in chunks) {
      total += (c as Uint8List).length;
    }
    final out = Uint8List(total);
    var pos = 0;
    for (final c in chunks) {
      final v = c as Uint8List;
      out.setRange(pos, pos + v.length, v);
      pos += v.length;
    }
    return out;
  }
  if (first is ByteArrayData) {
    var dataLength = 0;
    var values = 0;
    for (final c in chunks) {
      final v = c as ByteArrayData;
      dataLength += v.data.length;
      values += v.numValues;
    }
    final data = Uint8List(dataLength);
    final offsets = Uint32List(values + 1);
    var dataPos = 0;
    var valuePos = 0;
    for (final c in chunks) {
      final v = c as ByteArrayData;
      if (v.data.isNotEmpty) {
        data.setRange(dataPos, dataPos + v.data.length, v.data);
      }
      for (var i = 0; i < v.numValues; i++) {
        offsets[valuePos + i + 1] = dataPos + v.offsets[i + 1];
      }
      dataPos += v.data.length;
      valuePos += v.numValues;
    }
    return ByteArrayData(data, offsets);
  }
  if (first is FixedLenData) {
    final width = first.width;
    var totalValues = 0;
    for (final c in chunks) {
      final v = c as FixedLenData;
      if (v.width != width) {
        throw ParquetFormatException(
          'inconsistent fixed length $width vs ${v.width}',
        );
      }
      totalValues += v.numValues;
    }
    final data = Uint8List(totalValues * width);
    var pos = 0;
    for (final c in chunks) {
      final v = c as FixedLenData;
      if (v.data.isNotEmpty) {
        data.setRange(pos, pos + v.data.length, v.data);
      }
      pos += v.data.length;
    }
    return FixedLenData(data, width);
  }
  throw ParquetUnsupportedException(
    'cannot concatenate value chunks of type ${first.runtimeType}',
  );
}

/// Materialises dictionary references into compact values.
Object materialiseDictionary(Object dictionary, Int32List indices) {
  if (dictionary is Int32List) {
    final out = Int32List(indices.length);
    for (var i = 0; i < indices.length; i++) {
      out[i] = dictionary[indices[i]];
    }
    return out;
  }
  if (dictionary is Int64Buffer) {
    final out = Int64Buffer(indices.length);
    for (var i = 0; i < indices.length; i++) {
      final limbs = dictionary.getLimbs(indices[i]);
      out.setLimbs(i, limbs.$1, limbs.$2);
    }
    return out;
  }
  if (dictionary is Float32List) {
    final out = Float32List(indices.length);
    for (var i = 0; i < indices.length; i++) {
      out[i] = dictionary[indices[i]];
    }
    return out;
  }
  if (dictionary is Float64List) {
    final out = Float64List(indices.length);
    for (var i = 0; i < indices.length; i++) {
      out[i] = dictionary[indices[i]];
    }
    return out;
  }
  if (dictionary is ByteArrayData) {
    var total = 0;
    for (var i = 0; i < indices.length; i++) {
      final idx = indices[i];
      total += dictionary.offsets[idx + 1] - dictionary.offsets[idx];
    }
    final data = Uint8List(total);
    final offsets = Uint32List(indices.length + 1);
    var pos = 0;
    for (var i = 0; i < indices.length; i++) {
      final idx = indices[i];
      final start = dictionary.offsets[idx];
      final end = dictionary.offsets[idx + 1];
      data.setRange(pos, pos + (end - start), dictionary.data, start);
      pos += end - start;
      offsets[i + 1] = pos;
    }
    return ByteArrayData(data, offsets);
  }
  if (dictionary is FixedLenData) {
    final width = dictionary.width;
    final data = Uint8List(indices.length * width);
    for (var i = 0; i < indices.length; i++) {
      final idx = indices[i];
      data.setRange(i * width, (i + 1) * width, dictionary.data, idx * width);
    }
    return FixedLenData(data, width);
  }
  if (dictionary is Uint8List) {
    throw ParquetUnsupportedException(
      'dictionary encoding is not defined for BOOLEAN',
    );
  }
  throw ParquetUnsupportedException(
    'unsupported dictionary type ${dictionary.runtimeType}',
  );
}

/// Unpacks PLAIN boolean bits into one byte per value (0/1).
Uint8List unpackBooleans(Uint8List bits, int count) {
  final out = Uint8List(count);
  for (var i = 0; i < count; i++) {
    out[i] = (bits[i >> 3] & (1 << (i & 7))) == 0 ? 0 : 1;
  }
  return out;
}

/// Packs one-byte-per-value booleans into a bitmap.
Uint8List packBooleans(Uint8List values) {
  final out = Uint8List((values.length + 7) >> 3);
  for (var i = 0; i < values.length; i++) {
    if (values[i] != 0) out[i >> 3] |= 1 << (i & 7);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Chunk-level value preallocation
// ---------------------------------------------------------------------------

/// Writes the decoded values of a flat (`maxRepLevel == 0`) multi-page column
/// chunk into one buffer instead of one buffer per page plus a concatenation
/// copy.
///
/// The buffer is sized with the chunk's total entry count (or, for BYTE_ARRAY,
/// with the sum of the data pages' uncompressed value sizes) and is trimmed to
/// the number of present values by [finish]. Fixed-width chunks therefore use a
/// single value allocation per chunk; BYTE_ARRAY chunks avoid the per-page
/// data/offset buffers entirely.
///
/// Supports:
///  * PLAIN INT32/INT64/FLOAT/DOUBLE/FIXED_LEN_BYTE_ARRAY/INT96/BYTE_ARRAY;
///  * dictionary indices for every non-BOOLEAN physical type, materialised
///    directly into the preallocated output (no index concatenation buffer).
///
/// BOOLEAN and non-PLAIN/dictionary encodings keep the page-by-page path in
/// `ColumnChunkReader`.
final class FlatValueWriter {
  FlatValueWriter(this.descriptor, int entryCapacity, {int byteCapacity = 0})
    : _capacity = entryCapacity,
      _type = descriptor.physicalType,
      _width = _physicalWidth(descriptor) {
    if (entryCapacity < 0) {
      throw ArgumentError.value(entryCapacity, 'entryCapacity');
    }
    switch (_type) {
      case PhysicalType.int32:
        _int32Values = Int32List(entryCapacity);
      case PhysicalType.int64:
        _int64Values = Int64Buffer(entryCapacity);
      case PhysicalType.float:
        _float32Values = Float32List(entryCapacity);
      case PhysicalType.double:
        _float64Values = Float64List(entryCapacity);
      case PhysicalType.fixedLenByteArray:
      case PhysicalType.int96:
        _fixedValues = Uint8List(entryCapacity * _width);
      case PhysicalType.byteArray:
        _byteData = Uint8List(math.max(64, byteCapacity));
        _byteOffsets = Uint32List(entryCapacity + 1);
      case PhysicalType.boolean:
        throw ParquetUnsupportedException(
          'FlatValueWriter does not support BOOLEAN; use the page path',
        );
    }
  }

  final ColumnDescriptor descriptor;
  final int _capacity;
  final PhysicalType _type;
  final int _width;

  Object? _dictionaryValues;
  Int32List? _int32Values;
  Int64Buffer? _int64Values;
  Float32List? _float32Values;
  Float64List? _float64Values;
  Uint8List? _fixedValues;
  Uint8List? _byteData;
  Uint32List? _byteOffsets;
  int _valuePosition = 0;
  int _byteDataLength = 0;
  int _byteValueCount = 0;

  /// Number of values written so far.
  int get valueCount =>
      _type == PhysicalType.byteArray ? _byteValueCount : _valuePosition;

  /// Registers the decoded dictionary page; required before writing indices.
  void setDictionary(Object dictionary) {
    _dictionaryValues = dictionary;
    if (_type == PhysicalType.byteArray && dictionary is ByteArrayData) {
      final values = dictionary.numValues;
      if (values > 0) {
        final payload = dictionary.offsets[values] - dictionary.offsets[0];
        final average = (payload / values).ceil();
        var estimate = _capacity * average + 1024;
        const maxEstimate = 64 << 20;
        if (estimate > maxEstimate) estimate = maxEstimate;
        if (estimate > _byteData!.length) _growDataTo(estimate);
      }
    }
  }

  /// Writes one PLAIN page's [count] values.
  void writePlain(Bytes data, int count) {
    if (count < 0) throw ArgumentError.value(count, 'count');
    if (count == 0) return;
    _checkCapacity(count);
    switch (_type) {
      case PhysicalType.int32:
        final values = decodePlainInt32(data, numValues: count);
        _int32Values!.setRange(_valuePosition, _valuePosition + count, values);
        _valuePosition += count;
      case PhysicalType.int64:
        final values = decodePlainInt64(data, numValues: count);
        for (var i = 0; i < count; i++) {
          final limbs = values.getLimbs(i);
          _int64Values!.setLimbs(_valuePosition + i, limbs.$1, limbs.$2);
        }
        _valuePosition += count;
      case PhysicalType.float:
        final values = decodePlainFloat32(data, numValues: count);
        _float32Values!.setRange(
          _valuePosition,
          _valuePosition + count,
          values,
        );
        _valuePosition += count;
      case PhysicalType.double:
        final values = decodePlainFloat64(data, numValues: count);
        _float64Values!.setRange(
          _valuePosition,
          _valuePosition + count,
          values,
        );
        _valuePosition += count;
      case PhysicalType.fixedLenByteArray:
      case PhysicalType.int96:
        // FIXED_LEN_BYTE_ARRAY/INT96 pages are raw bytes: copy them straight
        // into the preallocated buffer instead of allocating a per-page copy.
        final needed = count * _width;
        if (data.length < needed) {
          throw ParquetEofException(
            'PLAIN FIXED_LEN_BYTE_ARRAY data truncated: need $needed bytes, '
            'only ${data.length} available',
          );
        }
        final start = _valuePosition * _width;
        _fixedValues!.setRange(start, start + needed, data.asUint8List());
        _valuePosition += count;
      case PhysicalType.byteArray:
        _writePlainByteArray(data, count);
      case PhysicalType.boolean:
        throw ParquetUnsupportedException(
          'FlatValueWriter does not support PLAIN BOOLEAN',
        );
    }
  }

  /// Writes one dictionary-encoded page's [count] values, materialising them
  /// directly into the preallocated output.
  void writeDictionaryIndices(Bytes data, int count) {
    if (count < 0) throw ArgumentError.value(count, 'count');
    if (count == 0) return;
    _checkCapacity(count);
    final dictionary = _dictionaryValues;
    if (dictionary == null) {
      throw ParquetFormatException(
        'column ${descriptor.dotPath} uses dictionary indices but has no '
        'dictionary page',
      );
    }
    final indices = decodeDictionaryIndices(data, numValues: count);
    switch (_type) {
      case PhysicalType.int32:
        final values = dictionary as Int32List;
        for (var i = 0; i < count; i++) {
          _int32Values![_valuePosition + i] = values[indices[i]];
        }
        _valuePosition += count;
      case PhysicalType.int64:
        final values = dictionary as Int64Buffer;
        for (var i = 0; i < count; i++) {
          final limbs = values.getLimbs(indices[i]);
          _int64Values!.setLimbs(_valuePosition + i, limbs.$1, limbs.$2);
        }
        _valuePosition += count;
      case PhysicalType.float:
        final values = dictionary as Float32List;
        for (var i = 0; i < count; i++) {
          _float32Values![_valuePosition + i] = values[indices[i]];
        }
        _valuePosition += count;
      case PhysicalType.double:
        final values = dictionary as Float64List;
        for (var i = 0; i < count; i++) {
          _float64Values![_valuePosition + i] = values[indices[i]];
        }
        _valuePosition += count;
      case PhysicalType.fixedLenByteArray:
      case PhysicalType.int96:
        final values = dictionary as FixedLenData;
        for (var i = 0; i < count; i++) {
          final source = indices[i] * _width;
          final target = (_valuePosition + i) * _width;
          _fixedValues!.setRange(target, target + _width, values.data, source);
        }
        _valuePosition += count;
      case PhysicalType.byteArray:
        final values = dictionary as ByteArrayData;
        final offsets = values.offsets;
        final dictionaryData = values.data;
        var dataLength = _byteDataLength;
        var valueCount = _byteValueCount;
        var out = _byteData!;
        final targetOffsets = _byteOffsets!;
        var pageBytes = 0;
        for (var i = 0; i < count; i++) {
          final index = indices[i];
          pageBytes += offsets[index + 1] - offsets[index];
        }
        if (dataLength + pageBytes > out.length) {
          _byteDataLength = dataLength;
          _ensureByteCapacity(pageBytes);
          out = _byteData!;
        }
        for (var i = 0; i < count; i++) {
          final index = indices[i];
          final start = offsets[index];
          final end = offsets[index + 1];
          final length = end - start;
          if (dataLength + length > out.length) {
            _byteDataLength = dataLength;
            _byteValueCount = valueCount;
            _ensureByteCapacity(length);
            out = _byteData!;
          }
          if (length > 0) {
            out.setRange(
              dataLength,
              dataLength + length,
              dictionaryData,
              start,
            );
          }
          dataLength += length;
          valueCount++;
          targetOffsets[valueCount] = dataLength;
        }
        _byteDataLength = dataLength;
        _byteValueCount = valueCount;
      case PhysicalType.boolean:
        throw ParquetUnsupportedException(
          'dictionary encoding is not defined for BOOLEAN',
        );
    }
  }

  /// Returns the decoded values, trimmed to [presentCount] entries.
  Object finish(int presentCount) {
    if (presentCount < 0 || presentCount > _capacity) {
      throw ParquetRangeException(
        'decoded $presentCount values for a preallocated capacity of '
        '$_capacity in ${descriptor.dotPath}',
      );
    }
    final actual = valueCount;
    if (actual != presentCount) {
      throw ParquetFormatException(
        'column ${descriptor.dotPath} wrote $actual values, expected '
        '$presentCount',
      );
    }
    switch (_type) {
      case PhysicalType.int32:
        return Int32List.view(_int32Values!.buffer, 0, presentCount);
      case PhysicalType.int64:
        final values = _int64Values!;
        if (values.length == presentCount) return values;
        final out = Int64Buffer(presentCount);
        for (var i = 0; i < presentCount; i++) {
          final limbs = values.getLimbs(i);
          out.setLimbs(i, limbs.$1, limbs.$2);
        }
        return out;
      case PhysicalType.float:
        return Float32List.view(_float32Values!.buffer, 0, presentCount);
      case PhysicalType.double:
        return Float64List.view(_float64Values!.buffer, 0, presentCount);
      case PhysicalType.fixedLenByteArray:
      case PhysicalType.int96:
        return FixedLenData(
          Uint8List.view(_fixedValues!.buffer, 0, presentCount * _width),
          _width,
        );
      case PhysicalType.byteArray:
        final dataLength = _byteDataLength;
        final data = _byteData!.length > dataLength * 2 + 4096
            ? (Uint8List(dataLength)..setRange(0, dataLength, _byteData!))
            : Uint8List.view(_byteData!.buffer, 0, dataLength);
        return ByteArrayData(
          data,
          Uint32List.view(_byteOffsets!.buffer, 0, presentCount + 1),
        );
      case PhysicalType.boolean:
        throw ParquetUnsupportedException(
          'FlatValueWriter does not support BOOLEAN',
        );
    }
  }

  void _checkCapacity(int count) {
    final used = valueCount;
    if (used + count > _capacity) {
      throw ParquetRangeException(
        'column ${descriptor.dotPath} produced more values than the '
        'preallocated capacity $_capacity',
      );
    }
  }

  void _writePlainByteArray(Bytes data, int count) {
    final source = data.asUint8List();
    final sourceLength = source.length;
    var position = 0;
    var dataLength = _byteDataLength;
    var valueCount = _byteValueCount;
    var out = _byteData!;
    final targetOffsets = _byteOffsets!;
    for (var i = 0; i < count; i++) {
      if (position + 4 > sourceLength) {
        throw ParquetEofException(
          'PLAIN BYTE_ARRAY length prefix for value $i is truncated at '
          '$position',
        );
      }
      final length =
          source[position] |
          (source[position + 1] << 8) |
          (source[position + 2] << 16) |
          (source[position + 3] << 24);
      position += 4;
      if (length < 0 || length > sourceLength - position) {
        throw ParquetEofException(
          'PLAIN BYTE_ARRAY value $i declares $length bytes but only '
          '${sourceLength - position} remain',
        );
      }
      if (dataLength + length > out.length) {
        _byteDataLength = dataLength;
        _byteValueCount = valueCount;
        _ensureByteCapacity(length);
        out = _byteData!;
      }
      if (length > 0) {
        out.setRange(dataLength, dataLength + length, source, position);
      }
      dataLength += length;
      position += length;
      valueCount++;
      targetOffsets[valueCount] = dataLength;
    }
    _byteDataLength = dataLength;
    _byteValueCount = valueCount;
  }

  void _ensureByteCapacity(int additional) {
    final needed = _byteDataLength + additional;
    final data = _byteData!;
    if (needed <= data.length) return;
    var capacity = data.isEmpty ? math.max(64, needed) : data.length;
    while (capacity < needed) {
      capacity *= 2;
    }
    _growDataTo(capacity);
  }

  void _growDataTo(int capacity) {
    final data = _byteData!;
    if (capacity <= data.length) return;
    final next = Uint8List(capacity);
    next.setRange(0, _byteDataLength, data);
    _byteData = next;
  }
}

int _physicalWidth(ColumnDescriptor descriptor) {
  switch (descriptor.physicalType) {
    case PhysicalType.int32:
    case PhysicalType.float:
      return 4;
    case PhysicalType.int64:
    case PhysicalType.double:
      return 8;
    case PhysicalType.int96:
      return 12;
    case PhysicalType.fixedLenByteArray:
      final width = descriptor.typeLength;
      if (width == null || width <= 0) {
        throw ParquetFormatException(
          'invalid FIXED_LEN_BYTE_ARRAY type length $width',
        );
      }
      return width;
    case PhysicalType.boolean:
    case PhysicalType.byteArray:
      return 0;
  }
}
