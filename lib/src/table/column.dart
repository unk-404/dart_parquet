/// Columnar data model exposed by the reader.
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/int64.dart';

/// Bit-packed validity bitmap (1 = valid, 0 = null).
final class Validity {
  Validity(int length, {bool allValid = true})
    : length = length,
      _bits = Uint8List((length + 7) >> 3),
      _nullCount = 0 {
    if (allValid && length > 0) {
      final full = length >> 3;
      for (var i = 0; i < full; i++) {
        _bits[i] = 0xFF;
      }
      final rem = length & 7;
      if (rem != 0) _bits[full] = (1 << rem) - 1;
    } else {
      _nullCount = length;
    }
  }

  final int length;
  final Uint8List _bits;
  int _nullCount;

  int get nullCount => _nullCount;
  bool get hasNulls => _nullCount != 0;

  bool isNull(int index) => (_bits[index >> 3] & (1 << (index & 7))) == 0;

  bool isValid(int index) => !isNull(index);

  void setNull(int index) {
    final mask = 1 << (index & 7);
    final b = _bits[index >> 3];
    if (b & mask != 0) {
      _bits[index >> 3] = b & ~mask;
      _nullCount++;
    }
  }

  void setValid(int index) {
    final mask = 1 << (index & 7);
    final b = _bits[index >> 3];
    if (b & mask == 0) {
      _bits[index >> 3] = b | mask;
      _nullCount--;
    }
  }
}

/// Read-only typed column.
abstract class ParquetColumn {
  ParquetColumn({
    required this.name,
    required this.length,
    required this.validity,
  });

  final String name;
  final int length;
  final Validity validity;

  bool isNull(int index) => validity.isNull(index);

  /// Materialises a row value. Null for null entries.
  ///
  /// Binary columns return [String] for UTF-8 logical values and [Uint8List]
  /// otherwise. Use [BinaryColumn.bytesAt] for zero-copy access.
  Object? value(int index);

  List<Object?> toList() =>
      List<Object?>.generate(length, value, growable: false);
}

final class Int32Column extends ParquetColumn {
  Int32Column({
    required super.name,
    required this.values,
    required super.validity,
  }) : super(length: values.length);

  final Int32List values;

  int getInt(int index) => values[index];

  @override
  Object? value(int index) => isNull(index) ? null : values[index];
}

final class Int64Column extends ParquetColumn {
  Int64Column({
    required super.name,
    required this.values,
    required super.validity,
  }) : super(length: values.length);

  final Int64Buffer values;

  int getInt(int index) => values.getInt(index);
  BigInt getBigInt(int index) => values.getBigInt(index);

  @override
  Object? value(int index) {
    if (isNull(index)) return null;
    return values.isNative ? values.getInt(index) : values.getBigInt(index);
  }
}

final class Float32Column extends ParquetColumn {
  Float32Column({
    required super.name,
    required this.values,
    required super.validity,
  }) : super(length: values.length);

  final Float32List values;

  double getDouble(int index) => values[index];

  @override
  Object? value(int index) => isNull(index) ? null : values[index];
}

final class Float64Column extends ParquetColumn {
  Float64Column({
    required super.name,
    required this.values,
    required super.validity,
  }) : super(length: values.length);

  final Float64List values;

  double getDouble(int index) => values[index];

  @override
  Object? value(int index) => isNull(index) ? null : values[index];
}

final class BooleanColumn extends ParquetColumn {
  BooleanColumn({
    required super.name,
    required this.bits,
    required super.validity,
  }) : super(length: validity.length);

  final Uint8List bits;

  bool getBool(int index) => (bits[index >> 3] & (1 << (index & 7))) != 0;

  @override
  Object? value(int index) => isNull(index) ? null : getBool(index);
}

final class BinaryColumn extends ParquetColumn {
  BinaryColumn({
    required super.name,
    required this.data,
    required this.offsets,
    required super.validity,
    this.isUtf8 = false,
  }) : super(length: offsets.length - 1) {
    if (offsets.isEmpty) {
      throw ArgumentError('offsets must contain at least one element');
    }
    if (offsets.last > data.length) {
      throw ArgumentError('offsets exceed data length');
    }
  }

  final Uint8List data;
  final Uint32List offsets;
  final bool isUtf8;
  Bytes? _view;

  Bytes get bytes => _view ??= Bytes.fromUint8List(data);

  /// Zero-copy view of value [index]; empty view for null entries.
  Bytes bytesAt(int index) => bytes.slice(offsets[index], offsets[index + 1]);

  Uint8List getBytes(int index) => bytesAt(index).toUint8List();

  String getString(int index) => utf8.decode(bytesAt(index).asUint8List());

  int lengthInBytes(int index) => offsets[index + 1] - offsets[index];

  @override
  Object? value(int index) {
    if (isNull(index)) return null;
    if (isUtf8) return getString(index);
    return getBytes(index);
  }
}

final class FixedLenColumn extends ParquetColumn {
  FixedLenColumn({
    required super.name,
    required this.data,
    required this.width,
    required super.validity,
  }) : super(length: width == 0 ? 0 : data.length ~/ width) {
    if (width <= 0) throw ArgumentError.value(width, 'width');
    if (data.length % width != 0) {
      throw ArgumentError('data length is not a multiple of width');
    }
  }

  final Uint8List data;
  final int width;
  Bytes? _view;

  Bytes get bytes => _view ??= Bytes.fromUint8List(data);

  Bytes bytesAt(int index) => bytes.slice(index * width, (index + 1) * width);

  Uint8List getBytes(int index) => bytesAt(index).toUint8List();

  @override
  Object? value(int index) => isNull(index)
      ? null
      : Uint8List.sublistView(data, index * width, (index + 1) * width);
}

/// INT96 (legacy) raw 12-byte values.
final class Int96Column extends ParquetColumn {
  Int96Column({
    required super.name,
    required this.data,
    required super.validity,
  }) : super(length: data.length ~/ 12) {
    if (data.length % 12 != 0) {
      throw ArgumentError('INT96 data length must be a multiple of 12');
    }
  }

  final Uint8List data;

  Uint8List getBytes(int index) =>
      Uint8List.sublistView(data, index * 12, (index + 1) * 12);

  @override
  Object? value(int index) => isNull(index) ? null : getBytes(index);
}

/// Repeated field (LIST). `offsets` has `length + 1` entries.
final class ListColumn extends ParquetColumn {
  ListColumn({
    required super.name,
    required this.offsets,
    required this.child,
    required super.validity,
  }) : super(length: offsets.length - 1) {
    if (offsets.isEmpty) throw ArgumentError('offsets must not be empty');
    if (offsets.last != child.length) {
      throw ArgumentError('list offsets do not match child length');
    }
  }

  final Uint32List offsets;
  final ParquetColumn child;

  List<Object?> getList(int index) {
    final out = <Object?>[];
    for (var i = offsets[index]; i < offsets[index + 1]; i++) {
      out.add(child.value(i));
    }
    return out;
  }

  @override
  Object? value(int index) => isNull(index) ? null : getList(index);
}

/// Struct group.
final class StructColumn extends ParquetColumn {
  StructColumn({
    required super.name,
    required super.length,
    required this.fields,
    required super.validity,
  });

  final Map<String, ParquetColumn> fields;

  Map<String, Object?> getStruct(int index) {
    final out = <String, Object?>{};
    for (final e in fields.entries) {
      out[e.key] = e.value.value(index);
    }
    return out;
  }

  @override
  Object? value(int index) => isNull(index) ? null : getStruct(index);
}

/// MAP logical type: repeated key/value entries.
final class MapColumn extends ParquetColumn {
  MapColumn({
    required super.name,
    required this.offsets,
    required this.keys,
    required this.values,
    required super.validity,
  }) : super(length: offsets.length - 1) {
    if (offsets.isEmpty) throw ArgumentError('offsets must not be empty');
    if (offsets.last != keys.length || offsets.last != values.length) {
      throw ArgumentError('map offsets do not match key/value lengths');
    }
  }

  final Uint32List offsets;
  final ParquetColumn keys;
  final ParquetColumn values;

  Map<Object?, Object?> getMap(int index) {
    final out = <Object?, Object?>{};
    for (var i = offsets[index]; i < offsets[index + 1]; i++) {
      out[keys.value(i)] = values.value(i);
    }
    return out;
  }

  @override
  Object? value(int index) => isNull(index) ? null : getMap(index);
}

/// All-null column (NULL logical type / no physical values).
final class NullColumn extends ParquetColumn {
  NullColumn({required super.name, required super.length})
    : super(validity: Validity(length, allValid: false));

  @override
  Object? value(int index) => null;
}
