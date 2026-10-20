/// Apache Thrift compact protocol support for Parquet metadata.
///
/// Parquet stores its metadata and page headers using Thrift's *compact*
/// protocol. This library provides a small, dependency-free reader (and a
/// companion writer used by tests) over [ByteReader].
///
/// Structs are represented as `Map<int, Object?>` keyed by Thrift field id;
/// lists/sets become `List<Object?>`, maps become `Map<Object?, Object?>`,
/// binary values become [Uint8List] views and bools become [bool]. Strings are
/// not distinguishable from binary on the wire, so they are returned as bytes
/// too; callers decode UTF-8 where the schema says `string`.
library;

import 'dart:convert';
import 'dart:typed_data';

import '../util/bytes.dart';
import '../util/errors.dart';

/// Compact-protocol type identifiers.
abstract final class TCompactType {
  /// End of a struct.
  static const int stop = 0;

  /// Boolean `true` (field value is carried in the type, not a payload).
  static const int boolTrue = 1;

  /// Boolean `false`.
  static const int boolFalse = 2;

  /// Signed 8-bit integer.
  static const int byte = 3;

  /// Signed 16-bit integer (zigzag varint on the wire).
  static const int i16 = 4;

  /// Signed 32-bit integer (zigzag varint on the wire).
  static const int i32 = 5;

  /// Signed 64-bit integer (zigzag varint on the wire).
  static const int i64 = 6;

  /// IEEE-754 double, little-endian.
  static const int doubleValue = 7;

  /// Length-prefixed bytes.
  static const int binary = 8;

  /// List.
  static const int list = 9;

  /// Set (wire-identical to a list).
  static const int set = 10;

  /// Map.
  static const int map = 11;

  /// Nested struct.
  static const int struct = 12;

  /// Whether [type] is a valid non-stop compact type.
  static bool isValid(int type) => type >= boolTrue && type <= struct;
}

/// Reads Thrift compact protocol values from a [ByteReader].
final class ThriftCompactReader {
  /// Creates a reader over [reader]. The reader's position is mutated.
  ThriftCompactReader(this.reader);

  /// Creates a reader over [bytes], optionally starting at [offset] and
  /// bounded by [length] bytes.
  factory ThriftCompactReader.fromBytes(
    Bytes bytes, {
    int offset = 0,
    int? length,
  }) {
    final end = length == null ? bytes.length : offset + length;
    return ThriftCompactReader(ByteReader(bytes, position: offset, limit: end));
  }

  /// The underlying byte reader; its [ByteReader.position] tracks consumption.
  final ByteReader reader;

  /// Current read position.
  int get position => reader.position;

  /// Reads a complete struct (through its `STOP` field) as a field-id map.
  ///
  /// Unknown fields are fully parsed and stored in the returned map, so the
  /// caller can ignore them without desynchronising the stream.
  Map<int, Object?> readStruct() {
    final fields = <int, Object?>{};
    var lastFieldId = 0;
    while (true) {
      final header = reader.readUint8();
      if (header == TCompactType.stop) return fields;

      final type = header & 0x0F;
      if (!TCompactType.isValid(type)) {
        throw ParquetFormatException(
          'invalid Thrift field type $type at ${reader.position - 1}',
        );
      }

      final delta = header >> 4;
      final fieldId = delta == 0 ? reader.readZigZag32() : lastFieldId + delta;
      if (fieldId <= 0) {
        throw ParquetFormatException(
          'invalid Thrift field id $fieldId at ${reader.position}',
        );
      }
      lastFieldId = fieldId;
      fields[fieldId] = _readFieldValue(type);
    }
  }

  /// Reads one value of compact [type] from the stream.
  ///
  /// For booleans this is only valid for a *field* header (no payload byte);
  /// collection elements are handled internally.
  Object? readValue(int type) {
    if (!TCompactType.isValid(type)) {
      throw ParquetFormatException('invalid Thrift type $type');
    }
    return _readFieldValue(type);
  }

  Object? _readFieldValue(int type) {
    switch (type) {
      case TCompactType.boolTrue:
        return true;
      case TCompactType.boolFalse:
        return false;
      case TCompactType.byte:
        final raw = reader.readUint8();
        return raw >= 0x80 ? raw - 0x100 : raw;
      case TCompactType.i16:
      case TCompactType.i32:
        return reader.readZigZag32();
      case TCompactType.i64:
        return reader.readZigZag64();
      case TCompactType.doubleValue:
        return _readDouble();
      case TCompactType.binary:
        return _readBinary();
      case TCompactType.list:
      case TCompactType.set:
        return _readList();
      case TCompactType.map:
        return _readMap();
      case TCompactType.struct:
        return readStruct();
      default:
        throw ParquetFormatException('unsupported Thrift type $type');
    }
  }

  double _readDouble() {
    final lo = reader.readUint32LE();
    final hi = reader.readUint32LE();
    final bytes = ByteData(8)
      ..setUint32(0, lo, Endian.little)
      ..setUint32(4, hi, Endian.little);
    return bytes.getFloat64(0, Endian.little);
  }

  Uint8List _readBinary() {
    final length = reader.readVarint32();
    if (length < 0 || length > reader.remaining) {
      throw ParquetFormatException(
        'Thrift binary length $length exceeds remaining ${reader.remaining}',
      );
    }
    final start = reader.position;
    reader.skip(length);
    // Zero-copy view over the backing buffer.
    return Uint8List.sublistView(
      reader.bytes.asUint8List(),
      start,
      start + length,
    );
  }

  List<Object?> _readList() {
    final header = reader.readUint8();
    var size = header >> 4;
    final elementType = header & 0x0F;
    if (size == 15) size = reader.readVarint32();

    if (!TCompactType.isValid(elementType)) {
      throw ParquetFormatException(
        'invalid Thrift list element type $elementType',
      );
    }
    if (size < 0 || size > reader.remaining) {
      throw ParquetFormatException(
        'Thrift list size $size exceeds remaining ${reader.remaining}',
      );
    }

    final values = <Object?>[];
    for (var i = 0; i < size; i++) {
      values.add(_readElement(elementType));
    }
    return values;
  }

  Map<Object?, Object?> _readMap() {
    final size = reader.readVarint32();
    if (size < 0 || size > reader.remaining) {
      throw ParquetFormatException(
        'Thrift map size $size exceeds remaining ${reader.remaining}',
      );
    }
    if (size == 0) return <Object?, Object?>{};

    final types = reader.readUint8();
    final keyType = types >> 4;
    final valueType = types & 0x0F;
    if (!TCompactType.isValid(keyType) || !TCompactType.isValid(valueType)) {
      throw ParquetFormatException(
        'invalid Thrift map entry types $keyType/$valueType',
      );
    }

    final result = <Object?, Object?>{};
    for (var i = 0; i < size; i++) {
      final key = _readElement(keyType);
      result[key] = _readElement(valueType);
    }
    return result;
  }

  Object? _readElement(int type) {
    // Inside collections booleans are materialised as one byte each rather
    // than being carried in a field header.
    if (type == TCompactType.boolTrue || type == TCompactType.boolFalse) {
      final raw = reader.readUint8();
      if (raw == TCompactType.boolTrue) return true;
      if (raw == TCompactType.boolFalse) return false;
      throw ParquetFormatException(
        'invalid Thrift boolean byte $raw at ${reader.position - 1}',
      );
    }
    return _readFieldValue(type);
  }
}

/// A list with an explicit Thrift element type, for [ThriftCompactWriter].
final class TList {
  /// Creates a list value encoded with [elementType] (a [TCompactType]).
  const TList(this.elementType, this.values);

  /// Compact type of every element.
  final int elementType;

  /// Values to encode.
  final List<Object?> values;
}

/// A map with explicit Thrift key/value types, for [ThriftCompactWriter].
final class TMap {
  /// Creates a map value encoded with [keyType] and [valueType].
  const TMap(this.keyType, this.valueType, this.entries);

  /// Compact type of keys.
  final int keyType;

  /// Compact type of values.
  final int valueType;

  /// Entries to encode.
  final Map<Object?, Object?> entries;
}

/// Minimal Thrift compact-protocol writer.
///
/// It exists so tests can round-trip handcrafted metadata and page headers;
/// the package itself is read-only. Struct fields are written in ascending
/// field-id order, using short field-id deltas where possible.
final class ThriftCompactWriter {
  /// Creates an empty writer.
  ThriftCompactWriter();

  final ByteWriter _out = ByteWriter();

  /// Encodes [fields] as a struct and returns the accumulated bytes.
  ///
  /// This may only be called once per writer instance.
  Bytes toBytes() => _out.bytes;

  /// Writes one struct (including the terminating `STOP` field).
  void writeStruct(Map<int, Object?> fields) {
    var lastFieldId = 0;
    final ids = fields.keys.toList()..sort();
    for (final id in ids) {
      final value = fields[id];
      if (value == null) continue; // optional fields are simply omitted
      final type = _typeOf(value);
      final delta = id - lastFieldId;
      if (delta > 0 && delta < 16) {
        _out.writeUint8((delta << 4) | type);
      } else {
        _out.writeUint8(type);
        _out.writeZigZag32(id);
      }
      lastFieldId = id;
      _writeFieldValue(type, value);
    }
    _out.writeUint8(TCompactType.stop);
  }

  static int _typeOf(Object value) {
    if (value is bool) {
      return value ? TCompactType.boolTrue : TCompactType.boolFalse;
    }
    if (value is int) return TCompactType.i64;
    if (value is double) return TCompactType.doubleValue;
    if (value is Uint8List || value is String || value is Bytes) {
      return TCompactType.binary;
    }
    if (value is List<int>) return TCompactType.binary;
    if (value is TList) return TCompactType.list;
    if (value is TMap) return TCompactType.map;
    if (value is Map<int, Object?>) return TCompactType.struct;
    throw ArgumentError.value(value, 'value', 'unsupported Thrift value type');
  }

  void _writeFieldValue(int type, Object value) {
    switch (type) {
      case TCompactType.boolTrue:
      case TCompactType.boolFalse:
        return; // value encoded in the field header
      case TCompactType.byte:
        _out.writeUint8((value as int) & 0xFF);
      case TCompactType.i16:
      case TCompactType.i32:
        _out.writeZigZag32(value as int);
      case TCompactType.i64:
        _writeZigZag64(value as int);
      case TCompactType.doubleValue:
        final data = ByteData(8)..setFloat64(0, value as double, Endian.little);
        for (var i = 0; i < 8; i++) {
          _out.writeUint8(data.getUint8(i));
        }
      case TCompactType.binary:
        _writeBinary(value);
      case TCompactType.list:
      case TCompactType.set:
        _writeList(value);
      case TCompactType.map:
        _writeMap(value);
      case TCompactType.struct:
        writeStruct(value as Map<int, Object?>);
      default:
        throw ArgumentError.value(type, 'type', 'unsupported Thrift type');
    }
  }

  void _writeZigZag64(int value) {
    // Zigzag: non-negative 2n, negative -2n-1. Implemented locally because
    // ByteWriter.writeZigZag64 encodes negative values with the wrong offset.
    var encoded = value < 0 ? (-value * 2) - 1 : value * 2;
    while (encoded >= 0x80) {
      _out.writeUint8((encoded & 0x7F) | 0x80);
      encoded ~/= 128;
    }
    _out.writeUint8(encoded);
  }

  void _writeBinary(Object value) {
    final List<int> bytes;
    if (value is Uint8List) {
      bytes = value;
    } else if (value is String) {
      bytes = utf8.encode(value);
    } else if (value is Bytes) {
      bytes = value.asUint8List();
    } else {
      bytes = value as List<int>;
    }
    _out.writeVarint32(bytes.length);
    _out.writeUint8List(bytes);
  }

  void _writeList(Object value) {
    if (value is! TList) {
      throw ArgumentError.value(value, 'value', 'expected TList');
    }
    final size = value.values.length;
    if (size < 15) {
      _out.writeUint8((size << 4) | value.elementType);
    } else {
      _out.writeUint8(0xF0 | value.elementType);
      _out.writeVarint32(size);
    }
    for (final element in value.values) {
      _writeElement(value.elementType, element);
    }
  }

  void _writeMap(Object value) {
    if (value is! TMap) {
      throw ArgumentError.value(value, 'value', 'expected TMap');
    }
    final size = value.entries.length;
    _out.writeVarint32(size);
    if (size == 0) return;
    _out.writeUint8((value.keyType << 4) | value.valueType);
    for (final entry in value.entries.entries) {
      _writeElement(value.keyType, entry.key);
      _writeElement(value.valueType, entry.value);
    }
  }

  void _writeElement(int type, Object? value) {
    if (type == TCompactType.boolTrue || type == TCompactType.boolFalse) {
      _out.writeUint8(
        (value as bool) ? TCompactType.boolTrue : TCompactType.boolFalse,
      );
      return;
    }
    if (value == null) {
      throw ArgumentError.value(value, 'value', 'null collection element');
    }
    _writeFieldValue(type, value);
  }
}
