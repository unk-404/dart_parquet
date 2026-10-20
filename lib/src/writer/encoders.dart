/// Value, level and dictionary-index encoders used by the Parquet writer.
///
/// Everything here is web-safe: only `dart:typed_data`, `package:blit` and the
/// shared encoding helpers are imported. 64-bit values are carried as
/// [Int64Value] limb pairs so the writer never needs `Int64List`.
library;

import 'dart:typed_data';

import '../encodings/bitpacking.dart' show requiredBits;
import '../format/enums.dart';
import '../table/column.dart';
import '../util/bytes.dart';
import '../util/errors.dart';

/// Two's-complement representation of an INT64 value as unsigned 32-bit limbs.
///
/// Using limbs (instead of a Dart `int`) keeps the writer exact on dart2js,
/// where `int` is a double.
final class Int64Value {
  const Int64Value(this.lo, this.hi);

  /// Low 32 bits (unsigned).
  final int lo;

  /// High 32 bits (unsigned).
  final int hi;

  /// Signed high word, for ordering comparisons.
  int get signedHi => hi >= 0x80000000 ? hi - 0x100000000 : hi;

  @override
  bool operator ==(Object other) =>
      other is Int64Value && other.lo == lo && other.hi == hi;

  @override
  int get hashCode => Object.hash(lo, hi);

  @override
  String toString() => 'Int64Value($hi:${lo.toRadixString(16)})';
}

/// Bit width required to encode dictionary indices `0..maxValue`.
int dictionaryBitWidth(int maxValue) => requiredBits(maxValue);

/// Encodes the repeated level [value] [count] times in the RLE hybrid form.
///
/// A flat all-valid page has one long run of the same definition level, so
/// this avoids materialising and scanning a level array.
Uint8List encodeRleConstant(int count, int value, int bitWidth) {
  if (count <= 0 || bitWidth == 0) return Uint8List(0);
  final out = ByteWriter();
  out.writeVarint32(count << 1);
  final byteWidth = (bitWidth + 7) >> 3;
  for (var b = 0; b < byteWidth; b++) {
    out.writeUint8((value >> (8 * b)) & 0xFF);
  }
  return out.bytes.asUint8List();
}

/// Encodes values [start, end) of a flat typed column as PLAIN.
///
/// Fixed-width columns can be written straight from their typed storage,
/// without boxing every value into a list first.
Uint8List encodePlainColumn(
  ParquetColumn column,
  int start,
  int end, {
  required PhysicalType type,
  int? typeLength,
}) {
  final n = end - start;
  switch (type) {
    case PhysicalType.boolean:
      final bits = Uint8List((n + 7) >> 3);
      final values = (column as BooleanColumn).getBool;
      for (var i = 0; i < n; i++) {
        if (values(start + i)) bits[i >> 3] |= 1 << (i & 7);
      }
      return bits;
    case PhysicalType.int32:
      final values = (column as Int32Column).values;
      final out = Uint8List(n * 4);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        data.setInt32(i * 4, values[start + i], Endian.little);
      }
      return out;
    case PhysicalType.int64:
      final values = (column as Int64Column).values;
      final out = Uint8List(n * 8);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        final limbs = values.getLimbs(start + i);
        data.setUint32(i * 8, limbs.$1, Endian.little);
        data.setUint32(i * 8 + 4, limbs.$2, Endian.little);
      }
      return out;
    case PhysicalType.float:
      final values = (column as Float32Column).values;
      final out = Uint8List(n * 4);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        data.setFloat32(i * 4, values[start + i], Endian.little);
      }
      return out;
    case PhysicalType.double:
      final values = (column as Float64Column).values;
      final out = Uint8List(n * 8);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        data.setFloat64(i * 8, values[start + i], Endian.little);
      }
      return out;
    case PhysicalType.int96:
      final values = (column as Int96Column).data;
      return Uint8List.sublistView(values, start * 12, end * 12);
    case PhysicalType.fixedLenByteArray:
      final width = typeLength;
      if (width == null || width <= 0) {
        throw ParquetFormatException(
          'FIXED_LEN_BYTE_ARRAY requires a positive type_length',
        );
      }
      final values = (column as FixedLenColumn).data;
      return Uint8List.sublistView(values, start * width, end * width);
    case PhysicalType.byteArray:
      final binary = column as BinaryColumn;
      var total = 0;
      for (var i = start; i < end; i++) {
        total += 4 + binary.offsets[i + 1] - binary.offsets[i];
      }
      final out = Uint8List(total);
      final data = ByteData.sublistView(out);
      var pos = 0;
      for (var i = start; i < end; i++) {
        final valueStart = binary.offsets[i];
        final valueEnd = binary.offsets[i + 1];
        final length = valueEnd - valueStart;
        data.setUint32(pos, length, Endian.little);
        pos += 4;
        out.setRange(pos, pos + length, binary.data, valueStart);
        pos += length;
      }
      return out;
  }
}

/// Encodes [values] with the Parquet RLE / bit-packed hybrid encoding.
///
/// The returned byte stream has no length prefix (the layout used for v2 data
/// pages, dictionary indices and the payload of a v1 level section). Runs of at
/// least eight equal values use the RLE form, everything else is emitted as
/// bit-packed groups of eight (padded with zeroes).
Uint8List encodeRleBitPacked(List<int> values, int bitWidth) {
  if (bitWidth < 0 || bitWidth > 32) {
    throw ArgumentError.value(bitWidth, 'bitWidth', 'expected 0..32');
  }
  final out = ByteWriter();
  final n = values.length;
  var i = 0;
  while (i < n) {
    final run = _equalRun(values, i);
    if (run >= 8) {
      out.writeVarint32(run << 1); // low bit 0 => RLE run
      if (bitWidth > 0) {
        final byteWidth = (bitWidth + 7) >> 3;
        var value = values[i];
        if (bitWidth < 32) value &= (1 << bitWidth) - 1;
        for (var b = 0; b < byteWidth; b++) {
          out.writeUint8((value >> (8 * b)) & 0xFF);
        }
      }
      i += run;
      continue;
    }
    // Gather whole groups of eight until the next RLE run starts.
    final start = i;
    var groups = 0;
    while (i < n) {
      if (_equalRun(values, i) >= 8) break;
      groups++;
      i += 8;
      if (i >= n) {
        i = n;
        break;
      }
    }
    out.writeVarint32((groups << 1) | 1); // low bit 1 => bit-packed run
    _writePacked(
      out,
      values,
      start,
      groups * 8,
      bitWidth,
      available: n - start,
    );
  }
  return out.bytes.asUint8List();
}

int _equalRun(List<int> values, int start) {
  final n = values.length;
  final first = values[start];
  var run = 1;
  while (start + run < n && values[start + run] == first) {
    run++;
  }
  return run;
}

void _writePacked(
  ByteWriter out,
  List<int> values,
  int start,
  int count,
  int bitWidth, {
  required int available,
}) {
  if (bitWidth == 0 || count == 0) return;
  final bytes = Uint8List((bitWidth * count + 7) >> 3);
  var bitPosition = 0;
  for (var k = 0; k < count; k++) {
    // Values past the logical end are zero padding; the RLE hybrid requires
    // whole groups of eight.
    var value = k < available ? values[start + k] : 0;
    var remaining = bitWidth;
    var position = bitPosition;
    while (remaining > 0) {
      final byteIndex = position >> 3;
      final bitInByte = position & 7;
      final take = (8 - bitInByte) < remaining ? (8 - bitInByte) : remaining;
      bytes[byteIndex] |= (value & ((1 << take) - 1)) << bitInByte;
      value >>= take;
      remaining -= take;
      position += take;
    }
    bitPosition += bitWidth;
  }
  out.writeUint8List(bytes);
}

/// Encodes [values] using the Parquet PLAIN encoding for [type].
///
/// The canonical in-memory shapes are:
/// * `bool` for BOOLEAN,
/// * `int` for INT32,
/// * [Int64Value] for INT64,
/// * `double` for FLOAT/DOUBLE,
/// * `Uint8List` for BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY and INT96.
Uint8List encodePlainValues(
  List<Object?> values,
  PhysicalType type, {
  int? typeLength,
}) {
  final n = values.length;
  switch (type) {
    case PhysicalType.boolean:
      final bits = Uint8List((n + 7) >> 3);
      for (var i = 0; i < n; i++) {
        if (values[i] as bool) bits[i >> 3] |= 1 << (i & 7);
      }
      return bits;
    case PhysicalType.int32:
      final out = Uint8List(n * 4);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        data.setInt32(i * 4, values[i] as int, Endian.little);
      }
      return out;
    case PhysicalType.int64:
      final out = Uint8List(n * 8);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        final v = values[i] as Int64Value;
        data.setUint32(i * 8, v.lo, Endian.little);
        data.setUint32(i * 8 + 4, v.hi, Endian.little);
      }
      return out;
    case PhysicalType.int96:
      final out = Uint8List(n * 12);
      for (var i = 0; i < n; i++) {
        final v = values[i] as Uint8List;
        if (v.length != 12) {
          throw ArgumentError.value(v.length, 'INT96 length', 'expected 12');
        }
        out.setRange(i * 12, i * 12 + 12, v);
      }
      return out;
    case PhysicalType.float:
      final out = Uint8List(n * 4);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        data.setFloat32(i * 4, values[i] as double, Endian.little);
      }
      return out;
    case PhysicalType.double:
      final out = Uint8List(n * 8);
      final data = ByteData.sublistView(out);
      for (var i = 0; i < n; i++) {
        data.setFloat64(i * 8, values[i] as double, Endian.little);
      }
      return out;
    case PhysicalType.byteArray:
      var total = 0;
      for (var i = 0; i < n; i++) {
        total += 4 + (values[i] as Uint8List).length;
      }
      final out = Uint8List(total);
      final data = ByteData.sublistView(out);
      var pos = 0;
      for (var i = 0; i < n; i++) {
        final v = values[i] as Uint8List;
        data.setUint32(pos, v.length, Endian.little);
        pos += 4;
        out.setRange(pos, pos + v.length, v);
        pos += v.length;
      }
      return out;
    case PhysicalType.fixedLenByteArray:
      final width = typeLength;
      if (width == null || width <= 0) {
        throw ParquetFormatException(
          'FIXED_LEN_BYTE_ARRAY requires a positive type_length',
        );
      }
      final out = Uint8List(n * width);
      var pos = 0;
      for (var i = 0; i < n; i++) {
        final v = values[i] as Uint8List;
        if (v.length != width) {
          throw ArgumentError(
            'FIXED_LEN_BYTE_ARRAY value has ${v.length} bytes, expected $width',
          );
        }
        out.setRange(pos, pos + width, v);
        pos += width;
      }
      return out;
  }
}

/// Encodes a dictionary index stream: one bit-width byte plus an RLE /
/// bit-packed hybrid payload.
Uint8List encodeDictionaryIndices(List<int> indices, int bitWidth) {
  final out = ByteWriter()..writeUint8(bitWidth);
  out.writeUint8List(encodeRleBitPacked(indices, bitWidth));
  return out.bytes.asUint8List();
}

/// Raw PLAIN bytes of a single value, without a BYTE_ARRAY length prefix.
///
/// This is the representation used by Parquet `Statistics.min_value` /
/// `max_value`.
Uint8List encodeStatisticsValue(
  Object? value,
  PhysicalType type, {
  int? typeLength,
}) {
  switch (type) {
    case PhysicalType.boolean:
      return Uint8List(1)..[0] = (value as bool) ? 1 : 0;
    case PhysicalType.int32:
      final v = value as int;
      return Uint8List(4)
        ..[0] = v & 0xFF
        ..[1] = (v >> 8) & 0xFF
        ..[2] = (v >> 16) & 0xFF
        ..[3] = (v >> 24) & 0xFF;
    case PhysicalType.int64:
      final v = value as Int64Value;
      return Uint8List(8)
        ..[0] = v.lo & 0xFF
        ..[1] = (v.lo >> 8) & 0xFF
        ..[2] = (v.lo >> 16) & 0xFF
        ..[3] = (v.lo >> 24) & 0xFF
        ..[4] = v.hi & 0xFF
        ..[5] = (v.hi >> 8) & 0xFF
        ..[6] = (v.hi >> 16) & 0xFF
        ..[7] = (v.hi >> 24) & 0xFF;
    case PhysicalType.float:
      final data = ByteData(4)..setFloat32(0, value as double, Endian.little);
      return data.buffer.asUint8List();
    case PhysicalType.double:
      final data = ByteData(8)..setFloat64(0, value as double, Endian.little);
      return data.buffer.asUint8List();
    case PhysicalType.byteArray:
    case PhysicalType.fixedLenByteArray:
      return value as Uint8List;
    case PhysicalType.int96:
      throw ParquetUnsupportedException(
        'statistics are not written for INT96 columns',
      );
  }
}

/// Encoded size of [value] in the PLAIN encoding.
int plainValueSize(Object? value, PhysicalType type) {
  switch (type) {
    case PhysicalType.boolean:
      return 1;
    case PhysicalType.int32:
    case PhysicalType.float:
      return 4;
    case PhysicalType.int64:
    case PhysicalType.double:
      return 8;
    case PhysicalType.int96:
      return 12;
    case PhysicalType.byteArray:
      return 4 + (value as Uint8List).length;
    case PhysicalType.fixedLenByteArray:
      return (value as Uint8List).length;
  }
}
