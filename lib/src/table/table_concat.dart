/// Concatenates row-group tables into one table.
library;

import 'dart:typed_data';

import '../schema/schema.dart';
import '../util/errors.dart';
import '../util/int64.dart';
import 'column.dart';
import 'table.dart';

/// Concatenates [tables] (same schema/projection) into one table.
ParquetTable concatTables(ParquetSchema schema, List<ParquetTable> tables) {
  if (tables.isEmpty) {
    return ParquetTable(schema: schema, columns: const [], numRows: 0);
  }
  if (tables.length == 1) return tables.first;
  final names = [for (final c in tables.first.columns) c.name];
  final out = <ParquetColumn>[];
  for (final name in names) {
    final columns = [for (final t in tables) t[name]];
    out.add(_concatColumn(name, columns));
  }
  final rows = tables.fold<int>(0, (a, t) => a + t.numRows);
  return ParquetTable(schema: schema, columns: out, numRows: rows);
}

/// Slices a flat column to [length] rows.
ParquetColumn sliceColumn(ParquetColumn column, int length) {
  if (column.length < length) {
    throw ArgumentError.value(length, 'length', 'larger than column');
  }
  final validity = Validity(length);
  for (var i = 0; i < length; i++) {
    if (column.isNull(i)) validity.setNull(i);
  }
  if (column is NullColumn) {
    return NullColumn(name: column.name, length: length);
  }
  if (column is Int32Column) {
    return Int32Column(
      name: column.name,
      values: Int32List.fromList(column.values.sublist(0, length)),
      validity: validity,
    );
  }
  if (column is Int64Column) {
    final values = Int64Buffer(length);
    for (var i = 0; i < length; i++) {
      final limbs = column.values.getLimbs(i);
      values.setLimbs(i, limbs.$1, limbs.$2);
    }
    return Int64Column(name: column.name, values: values, validity: validity);
  }
  if (column is Float32Column) {
    return Float32Column(
      name: column.name,
      values: Float32List.fromList(column.values.sublist(0, length)),
      validity: validity,
    );
  }
  if (column is Float64Column) {
    return Float64Column(
      name: column.name,
      values: Float64List.fromList(column.values.sublist(0, length)),
      validity: validity,
    );
  }
  if (column is BooleanColumn) {
    final bits = Uint8List((length + 7) >> 3);
    for (var i = 0; i < length; i++) {
      if (column.getBool(i)) bits[i >> 3] |= 1 << (i & 7);
    }
    return BooleanColumn(name: column.name, bits: bits, validity: validity);
  }
  if (column is BinaryColumn) {
    final end = column.offsets[length];
    final data = Uint8List.fromList(column.data.sublist(0, end));
    final offsets = Uint32List(length + 1);
    for (var i = 0; i <= length; i++) {
      offsets[i] = column.offsets[i];
    }
    return BinaryColumn(
      name: column.name,
      data: data,
      offsets: offsets,
      validity: validity,
      isUtf8: column.isUtf8,
    );
  }
  if (column is FixedLenColumn) {
    final end = length * column.width;
    return FixedLenColumn(
      name: column.name,
      data: Uint8List.fromList(column.data.sublist(0, end)),
      width: column.width,
      validity: validity,
    );
  }
  if (column is Int96Column) {
    final end = length * 12;
    return Int96Column(
      name: column.name,
      data: Uint8List.fromList(column.data.sublist(0, end)),
      validity: validity,
    );
  }
  throw ParquetUnsupportedException(
    'slicing ${column.runtimeType} is not supported in v0.1.0',
  );
}

ParquetColumn _concatColumn(String name, List<ParquetColumn> columns) {
  final total = columns.fold<int>(0, (a, c) => a + c.length);
  final validity = Validity(total);
  var offset = 0;
  for (final c in columns) {
    for (var i = 0; i < c.length; i++) {
      if (c.isNull(i)) validity.setNull(offset + i);
    }
    offset += c.length;
  }

  final first = columns.first;
  if (first is NullColumn) {
    return NullColumn(name: name, length: total);
  }
  if (first is Int32Column) {
    final values = Int32List(total);
    var pos = 0;
    for (final c in columns) {
      final v = c as Int32Column;
      values.setRange(pos, pos + v.length, v.values);
      pos += v.length;
    }
    return Int32Column(name: name, values: values, validity: validity);
  }
  if (first is Int64Column) {
    final values = Int64Buffer(total);
    var pos = 0;
    for (final c in columns) {
      final v = c as Int64Column;
      for (var i = 0; i < v.length; i++) {
        final limbs = v.values.getLimbs(i);
        values.setLimbs(pos + i, limbs.$1, limbs.$2);
      }
      pos += v.length;
    }
    return Int64Column(name: name, values: values, validity: validity);
  }
  if (first is Float32Column) {
    final values = Float32List(total);
    var pos = 0;
    for (final c in columns) {
      final v = c as Float32Column;
      values.setRange(pos, pos + v.length, v.values);
      pos += v.length;
    }
    return Float32Column(name: name, values: values, validity: validity);
  }
  if (first is Float64Column) {
    final values = Float64List(total);
    var pos = 0;
    for (final c in columns) {
      final v = c as Float64Column;
      values.setRange(pos, pos + v.length, v.values);
      pos += v.length;
    }
    return Float64Column(name: name, values: values, validity: validity);
  }
  if (first is BooleanColumn) {
    final bits = Uint8List((total + 7) >> 3);
    var pos = 0;
    for (final c in columns) {
      final v = c as BooleanColumn;
      for (var i = 0; i < v.length; i++) {
        if (v.getBool(i)) bits[(pos + i) >> 3] |= 1 << ((pos + i) & 7);
      }
      pos += v.length;
    }
    return BooleanColumn(name: name, bits: bits, validity: validity);
  }
  if (first is BinaryColumn) {
    var dataLength = 0;
    for (final c in columns) {
      dataLength += (c as BinaryColumn).data.length;
    }
    final data = Uint8List(dataLength);
    final offsets = Uint32List(total + 1);
    var dataPos = 0;
    var rowPos = 0;
    for (final c in columns) {
      final v = c as BinaryColumn;
      if (v.data.isNotEmpty) {
        data.setRange(dataPos, dataPos + v.data.length, v.data);
      }
      for (var i = 0; i < v.length; i++) {
        offsets[rowPos + i + 1] = dataPos + v.offsets[i + 1];
      }
      dataPos += v.data.length;
      rowPos += v.length;
    }
    return BinaryColumn(
      name: name,
      data: data,
      offsets: offsets,
      validity: validity,
      isUtf8: first.isUtf8,
    );
  }
  if (first is FixedLenColumn) {
    final width = first.width;
    final data = Uint8List(total * width);
    var pos = 0;
    for (final c in columns) {
      final v = c as FixedLenColumn;
      if (v.width != width) {
        throw ParquetFormatException('fixed length mismatch');
      }
      if (v.data.isNotEmpty) {
        data.setRange(pos, pos + v.data.length, v.data);
      }
      pos += v.data.length;
    }
    return FixedLenColumn(
      name: name,
      data: data,
      width: width,
      validity: validity,
    );
  }
  if (first is Int96Column) {
    final data = Uint8List(total * 12);
    var pos = 0;
    for (final c in columns) {
      final v = c as Int96Column;
      if (v.data.isNotEmpty) {
        data.setRange(pos, pos + v.data.length, v.data);
      }
      pos += v.data.length;
    }
    return Int96Column(name: name, data: data, validity: validity);
  }
  if (first is ListColumn) {
    final offsets = Uint32List(total + 1);
    var row = 0;
    var childOffset = 0;
    for (final c in columns) {
      final v = c as ListColumn;
      for (var i = 0; i < v.length; i++) {
        offsets[row + i + 1] = childOffset + v.offsets[i + 1];
      }
      childOffset += v.child.length;
      row += v.length;
    }
    final child = _concatColumn(first.child.name, [
      for (final c in columns) (c as ListColumn).child,
    ]);
    return ListColumn(
      name: name,
      offsets: offsets,
      child: child,
      validity: validity,
    );
  }
  if (first is StructColumn) {
    final fields = <String, ParquetColumn>{};
    for (final fieldName in first.fields.keys) {
      fields[fieldName] = _concatColumn(fieldName, [
        for (final c in columns) (c as StructColumn).fields[fieldName]!,
      ]);
    }
    return StructColumn(
      name: name,
      length: total,
      fields: fields,
      validity: validity,
    );
  }
  if (first is MapColumn) {
    final offsets = Uint32List(total + 1);
    var row = 0;
    var entryOffset = 0;
    for (final c in columns) {
      final v = c as MapColumn;
      for (var i = 0; i < v.length; i++) {
        offsets[row + i + 1] = entryOffset + v.offsets[i + 1];
      }
      entryOffset += v.keys.length;
      row += v.length;
    }
    final keys = _concatColumn(first.keys.name, [
      for (final c in columns) (c as MapColumn).keys,
    ]);
    final values = _concatColumn(first.values.name, [
      for (final c in columns) (c as MapColumn).values,
    ]);
    return MapColumn(
      name: name,
      offsets: offsets,
      keys: keys,
      values: values,
      validity: validity,
    );
  }
  throw ParquetUnsupportedException('cannot concatenate ${first.runtimeType}');
}
