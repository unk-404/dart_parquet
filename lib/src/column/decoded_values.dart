/// Helpers for concatenating typed value chunks decoded from multiple pages.
library;

import 'dart:typed_data';

import '../encodings/plain.dart' show ByteArrayData, FixedLenData;
import '../util/errors.dart';
import '../util/int64.dart';

/// Concatenates primitive value chunks of the same runtime type.
///
/// Supported chunk types: [Int32List], [Int64Buffer], [Float32List],
/// [Float64List], [Uint8List] (one byte per boolean), [ByteArrayData],
/// [FixedLenData].
Object concatValueChunks(List<Object> chunks) {
  if (chunks.isEmpty) return Int32List(0);
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
