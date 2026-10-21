/// Dremel-style shredding: turns assembled [ParquetTable] columns back into
/// per-leaf definition/repetition level streams.
///
/// This is the inverse of `lib/src/table/nested_assembler.dart` and supports
/// arbitrary nesting: optional/required/repeated groups and leaves, LIST of
/// LIST/MAP/STRUCT, MAP of LIST/MAP/STRUCT, bare repeated leaves/groups,
/// legacy two-level LIST encodings (all `parquet-format` backward-compatibility
/// rules) and MAP without a value field.
library;

import 'dart:typed_data';

import '../schema/schema.dart';
import '../table/column.dart';
import '../table/table.dart';
import '../util/errors.dart';
import 'encoders.dart';

/// Shredded level and value streams for one leaf column.
final class LeafData {
  /// Creates an empty stream for [descriptor].
  LeafData(this.descriptor);

  /// Leaf descriptor (physical type, levels and path).
  final ColumnDescriptor descriptor;

  /// Definition level for every entry (including null/empty markers).
  List<int> defs = <int>[];

  /// Repetition level for every entry.
  List<int> reps = <int>[];

  /// Present values in entry order (only entries with `def == maxDefLevel`).
  List<Object?> values = <Object?>[];

  int? _entryCount;

  /// Set for fully-valid flat columns: the writer can take values directly
  /// from [sourceColumn] instead of the boxed [values] list.
  ParquetColumn? sourceColumn;

  /// Whether every entry is present (no nulls/empty markers).
  bool allPresent = false;

  /// Number of level entries; this is `ColumnMetaData.num_values`.
  int get numEntries => _entryCount ?? defs.length;

  /// Sets the entry count for flat fast paths that omit level arrays.
  void setEntryCount(int count) {
    _entryCount = count;
  }

  /// Number of actual values stored in [values].
  int get presentCount => values.length;
}

/// Validates that [schema] is a schema this writer can shred and the matching
/// reader can reconstruct.
void validateSchemaShape(ParquetSchema schema) {
  if (schema.root.children.isEmpty) {
    throw ParquetUnsupportedException(
      'cannot write a Parquet file with an empty schema',
    );
  }
  for (final node in schema.root.children) {
    _validateNode(node);
  }
}

void _validateNode(SchemaNode node) {
  if (node.maxDefLevel > 0xFF || node.maxRepLevel > 0xFF) {
    throw ParquetUnsupportedException(
      'schema ${node.dotPath} has more than 255 definition/repetition levels',
    );
  }
  if (node.isLeaf) {
    if (node.physicalType == PhysicalType.fixedLenByteArray &&
        (node.typeLength == null || node.typeLength! <= 0)) {
      throw ParquetUnsupportedException(
        'FIXED_LEN_BYTE_ARRAY ${node.dotPath} needs a positive type_length',
      );
    }
    return;
  }
  if (node.logicalType?.kind == LogicalTypeKind.variant) {
    final names = <String>{for (final child in node.children) child.name};
    if (node.children.length != 2 ||
        !names.contains('metadata') ||
        !names.contains('value') ||
        node.children.any((child) => !child.isLeaf)) {
      throw ParquetUnsupportedException(
        'writing shredded VARIANT ${node.dotPath} is not supported; '
        'only unshredded metadata/value groups can be written',
      );
    }
    for (final child in node.children) {
      _validateNode(child);
    }
    return;
  }
  if (!_hasLeaf(node)) {
    throw ParquetFormatException('group ${node.dotPath} has no leaf columns');
  }
  // Throws a typed format error for malformed LIST/MAP shapes.
  _resolveList(node);
  _resolveMap(node);
  for (final child in node.children) {
    _validateNode(child);
  }
}

bool _hasLeaf(SchemaNode node) {
  if (node.isLeaf) return true;
  for (final child in node.children) {
    if (_hasLeaf(child)) return true;
  }
  return false;
}

/// Whether [node] is a LIST-annotated group.
bool isListNode(SchemaNode node) =>
    !node.isLeaf && node.logicalType?.kind == LogicalTypeKind.list;

/// Validates that [table] matches [schema] structurally.
void validateTableShape(ParquetTable table, ParquetSchema schema) {
  if (table.numRows < 0) {
    throw ArgumentError.value(table.numRows, 'numRows', 'must not be negative');
  }
  final topLevel = schema.root.children;
  if (table.columns.length != topLevel.length) {
    throw ArgumentError(
      'table has ${table.columns.length} columns, schema expects '
      '${topLevel.length}',
    );
  }
  final names = <String>{};
  for (final column in table.columns) {
    if (!names.add(column.name)) {
      throw ArgumentError('duplicate column name "${column.name}"');
    }
  }
  for (final node in topLevel) {
    final column = table.column(node.name);
    if (column == null) {
      throw ArgumentError('table is missing column "${node.name}"');
    }
    if (column.length != table.numRows) {
      throw ArgumentError(
        'column "${node.name}" has length ${column.length}, expected '
        '${table.numRows}',
      );
    }
    _validatePosition(node, column, consumed: false);
  }
}

void _validatePosition(
  SchemaNode node,
  ParquetColumn column, {
  required bool consumed,
}) {
  if (!node.isLeaf && node.logicalType?.kind == LogicalTypeKind.variant) {
    if (column is! VariantColumn) {
      throw ArgumentError(
        'VARIANT column "${node.name}" is ${column.runtimeType}, expected '
        'VariantColumn',
      );
    }
    return;
  }
  if (node.isLeaf) {
    if (node.isRepeated && !consumed) {
      // A bare repeated leaf is represented by a [ListColumn] of its type.
      if (column is! ListColumn) {
        throw ArgumentError(
          'repeated column "${node.name}" is ${column.runtimeType}, expected '
          'ListColumn',
        );
      }
      _requireLeafType(node, column.child);
      return;
    }
    _requireLeafType(node, column);
    return;
  }

  if (node.isRepeated && !consumed) {
    // A bare repeated group is represented by a [ListColumn] of its struct.
    if (column is! ListColumn) {
      throw ArgumentError(
        'repeated column "${node.name}" is ${column.runtimeType}, expected '
        'ListColumn',
      );
    }
    _validatePosition(node, column.child, consumed: true);
    return;
  }

  final listShape = _resolveList(node);
  if (listShape != null) {
    if (column is! ListColumn) {
      throw ArgumentError(
        'column "${node.name}" is ${column.runtimeType}, expected ListColumn',
      );
    }
    _validatePosition(
      listShape.element,
      column.child,
      consumed: listShape.consumed,
    );
    return;
  }

  final mapShape = _resolveMap(node);
  if (mapShape != null) {
    if (column is! MapColumn) {
      throw ArgumentError(
        'column "${node.name}" is ${column.runtimeType}, expected MapColumn',
      );
    }
    _validatePosition(mapShape.key, column.keys, consumed: false);
    final valueNode = mapShape.value;
    if (valueNode != null) {
      _validatePosition(valueNode, column.values, consumed: false);
    }
    return;
  }

  if (column is! StructColumn) {
    throw ArgumentError(
      'column "${node.name}" is ${column.runtimeType}, expected StructColumn',
    );
  }
  for (final child in node.children) {
    final field = column.fields[child.name];
    if (field == null) {
      throw ArgumentError(
        'struct column "${node.name}" is missing field "${child.name}"',
      );
    }
    if (field.length != column.length) {
      throw ArgumentError(
        'struct column "${node.name}" field "${child.name}" has length '
        '${field.length}, expected ${column.length}',
      );
    }
    _validatePosition(child, field, consumed: false);
  }
  for (final name in column.fields.keys) {
    if (!node.children.any((child) => child.name == name)) {
      throw ArgumentError(
        'struct column "${node.name}" has unexpected field "$name"',
      );
    }
  }
}

void _requireLeafType(SchemaNode node, ParquetColumn column) {
  final ok = switch (node.physicalType!) {
    PhysicalType.boolean => column is BooleanColumn,
    PhysicalType.int32 => column is Int32Column,
    PhysicalType.int64 => column is Int64Column,
    PhysicalType.int96 => column is Int96Column,
    PhysicalType.float => column is Float32Column,
    PhysicalType.double => column is Float64Column,
    PhysicalType.byteArray => column is BinaryColumn,
    PhysicalType.fixedLenByteArray =>
      column is FixedLenColumn &&
          (node.typeLength == null || column.width == node.typeLength),
  };
  if (!ok) {
    throw ArgumentError(
      'column "${node.dotPath}" is ${column.runtimeType}, which does not match '
      'physical type ${node.physicalType!.name}',
    );
  }
}

/// Shreds [table] into one [LeafData] per schema leaf, in schema leaf order.
List<LeafData> shredTable(
  ParquetTable table,
  ParquetSchema schema, {
  bool fastFlat = true,
}) {
  final leaves = <int, LeafData>{
    for (final descriptor in schema.columns)
      descriptor.leafIndex: LeafData(descriptor),
  };
  final shredder = _Shredder(leaves, fastFlat: fastFlat);
  for (final node in schema.root.children) {
    final column = table.column(node.name);
    if (column == null) {
      throw ArgumentError('table is missing column "${node.name}"');
    }
    shredder.shredRoot(node, column, table.numRows);
  }
  return [
    for (final descriptor in schema.columns) leaves[descriptor.leafIndex]!,
  ];
}

final class _Shredder {
  _Shredder(this._leaves, {required this.fastFlat});

  final Map<int, LeafData> _leaves;
  final bool fastFlat;

  void shredRoot(SchemaNode node, ParquetColumn column, int numRows) {
    if (!node.isLeaf && node.logicalType?.kind == LogicalTypeKind.variant) {
      _shredVariant(node, column as VariantColumn, numRows);
      return;
    }
    // Fast path for a flat leaf: no slot lists required.
    if (node.isLeaf && !node.isRepeated) {
      final data = _leaf(node)..setEntryCount(numRows);
      final maxDef = node.maxDefLevel;
      if (!node.isOptional) {
        data.values = List<Object?>.generate(numRows, (row) {
          if (column.isNull(row)) {
            throw ArgumentError(
              'required column "${node.dotPath}" contains a null at row '
              '$row',
            );
          }
          return canonicalValue(column, row);
        }, growable: false);
        return;
      }
      final nullCount = column.validity.nullCount;
      // Keep the legacy shredding path for small tables so default output and
      // byte-compatibility fixtures stay stable; large flat analyses use the
      // zero-copy column path.
      if (fastFlat && nullCount == 0 && numRows >= 4096) {
        data.allPresent = true;
        // Fixed-width values are read from the column by the chunk writer.
        // Byte arrays keep the boxed list for dictionary planning.
        final type = node.physicalType;
        data.sourceColumn = column;
        if (type != PhysicalType.byteArray &&
            type != PhysicalType.fixedLenByteArray) {
          return;
        }
        data.defs = Uint8List(numRows)..fillRange(0, numRows, maxDef);
        data.reps = Uint8List(numRows);
        data.values = List<Object?>.generate(
          numRows,
          (row) => canonicalValue(column, row),
          growable: false,
        );
        return;
      }
      final defs = Uint8List(numRows);
      final reps = Uint8List(numRows);
      final values = List<Object?>.filled(numRows - nullCount, null);
      var present = 0;
      for (var row = 0; row < numRows; row++) {
        if (column.isNull(row)) {
          defs[row] = maxDef - 1;
        } else {
          defs[row] = maxDef;
          values[present++] = canonicalValue(column, row);
        }
      }
      data
        ..defs = defs
        ..reps = reps
        ..values = values;
      return;
    }
    final slots = List<int>.generate(numRows, (i) => i);
    final defs = List<int>.filled(numRows, 0);
    final reps = List<int>.filled(numRows, 0);
    _shred(node, column, slots, defs, reps);
  }

  void _shredVariant(SchemaNode node, VariantColumn column, int numRows) {
    SchemaNode? metadataNode;
    SchemaNode? valueNode;
    for (final child in node.children) {
      if (child.name == 'metadata') metadataNode = child;
      if (child.name == 'value') valueNode = child;
    }
    if (metadataNode == null ||
        valueNode == null ||
        !metadataNode.isLeaf ||
        !valueNode.isLeaf) {
      throw ParquetUnsupportedException(
        'VARIANT ${node.dotPath} must be an unshredded metadata/value group',
      );
    }
    final metadataData = _leaf(metadataNode)..setEntryCount(numRows);
    final valueData = _leaf(valueNode)..setEntryCount(numRows);
    final metadataDefs = Uint8List(numRows);
    final valueDefs = Uint8List(numRows);
    final metadataValues = <Object?>[];
    final valueValues = <Object?>[];
    final groupDef = node.maxDefLevel;
    final nullDef = groupDef > 0 ? groupDef - 1 : 0;
    for (var row = 0; row < numRows; row++) {
      final variant = column.getVariant(row);
      if (variant == null) {
        if (!node.isOptional) {
          throw ArgumentError(
            'required VARIANT "${node.dotPath}" contains a null at row $row',
          );
        }
        metadataDefs[row] = nullDef;
        valueDefs[row] = nullDef;
        continue;
      }
      metadataDefs[row] = groupDef;
      valueDefs[row] = groupDef;
      metadataValues.add(variant.metadataBytes);
      valueValues.add(variant.valueBytes);
    }
    metadataData
      ..defs = metadataDefs
      ..reps = Uint8List(numRows)
      ..values = metadataValues;
    valueData
      ..defs = valueDefs
      ..reps = Uint8List(numRows)
      ..values = valueValues;
  }

  LeafData _leaf(SchemaNode node) {
    final data = _leaves[node.leafIndex!];
    if (data == null) {
      throw ParquetFormatException('missing leaf ${node.dotPath}');
    }
    return data;
  }

  void _shred(
    SchemaNode node,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps, {
    bool consumed = false,
  }) {
    if (slots.isEmpty) return;

    if (node.isRepeated && !consumed) {
      _shredRepeated(node, column, slots, defs, reps);
      return;
    }

    if (node.isOptional) {
      // Emit in entry order: batch consecutive present slots, flushing before
      // every null marker (levels must follow row/element order).
      final runSlots = <int>[];
      final runDefs = <int>[];
      final runReps = <int>[];
      void flush() {
        if (runSlots.isEmpty) return;
        _shredPresent(node, column, runSlots, runDefs, runReps);
        runSlots.clear();
        runDefs.clear();
        runReps.clear();
      }

      for (var i = 0; i < slots.length; i++) {
        if (column.isNull(slots[i])) {
          flush();
          _emitMissing(node, defs[i], reps[i]);
        } else {
          runSlots.add(slots[i]);
          runDefs.add(defs[i] + 1);
          runReps.add(reps[i]);
        }
      }
      flush();
      return;
    }

    // Required node (including a repeated node consumed as a LIST element):
    // nulls cannot be represented.
    for (var i = 0; i < slots.length; i++) {
      if (column.isNull(slots[i])) {
        throw ArgumentError(
          'required column "${node.dotPath}" contains a null value',
        );
      }
    }
    _shredPresent(node, column, slots, defs, reps);
  }

  void _shredPresent(
    SchemaNode node,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    if (node.isLeaf) {
      final data = _leaf(node);
      for (var i = 0; i < slots.length; i++) {
        data.defs.add(defs[i]);
        data.reps.add(reps[i]);
        data.values.add(canonicalValue(column, slots[i]));
      }
      return;
    }

    final listShape = _resolveList(node);
    if (listShape != null) {
      _shredList(node, listShape, column, slots, defs, reps);
      return;
    }

    final mapShape = _resolveMap(node);
    if (mapShape != null) {
      _shredMap(node, mapShape, column, slots, defs, reps);
      return;
    }

    final struct = column as StructColumn;
    for (final child in node.children) {
      final field = struct.fields[child.name];
      if (field == null) {
        throw ArgumentError(
          'struct column "${node.name}" is missing field "${child.name}"',
        );
      }
      _shred(child, field, slots, defs, reps);
    }
  }

  void _shredList(
    SchemaNode node,
    _ListShape shape,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    final list = column as ListColumn;
    final repeated = shape.repeated;
    for (var i = 0; i < slots.length; i++) {
      final start = list.offsets[slots[i]];
      final end = list.offsets[slots[i] + 1];
      if (start == end) {
        _emitMissing(repeated, defs[i], reps[i]);
        continue;
      }
      final def = defs[i] + 1; // the repeated group/leaf is defined
      for (var k = start; k < end; k++) {
        final rep = k == start ? reps[i] : repeated.repLevel;
        _shred(shape.element, list.child, <int>[k], <int>[def], <int>[
          rep,
        ], consumed: shape.consumed);
      }
    }
  }

  void _shredMap(
    SchemaNode node,
    _MapShape shape,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    final map = column as MapColumn;
    final repeated = shape.repeated;
    final valueNode = shape.value;
    for (var i = 0; i < slots.length; i++) {
      final start = map.offsets[slots[i]];
      final end = map.offsets[slots[i] + 1];
      if (start == end) {
        _emitMissing(repeated, defs[i], reps[i]);
        continue;
      }
      final def = defs[i] + 1; // the key/value group is defined
      for (var k = start; k < end; k++) {
        final rep = k == start ? reps[i] : repeated.repLevel;
        _shred(shape.key, map.keys, <int>[k], <int>[def], <int>[rep]);
        if (valueNode != null) {
          _shred(valueNode, map.values, <int>[k], <int>[def], <int>[rep]);
        }
      }
    }
  }

  void _shredRepeated(
    SchemaNode node,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    final list = column as ListColumn;
    for (var i = 0; i < slots.length; i++) {
      final start = list.offsets[slots[i]];
      final end = list.offsets[slots[i] + 1];
      if (start == end) {
        // Empty repeated field: emit the parent's definition level.
        _emitMissing(node, defs[i], reps[i]);
        continue;
      }
      final def = defs[i] + 1; // the repeated element is defined
      for (var k = start; k < end; k++) {
        final rep = k == start ? reps[i] : node.repLevel;
        _shred(node, list.child, <int>[k], <int>[def], <int>[
          rep,
        ], consumed: true);
      }
    }
  }

  void _emitMissing(SchemaNode node, int def, int rep) {
    for (final leaf in _leavesOf(node)) {
      final data = _leaf(leaf);
      data.defs.add(def);
      data.reps.add(rep);
    }
  }
}

Iterable<SchemaNode> _leavesOf(SchemaNode node) sync* {
  if (node.isLeaf) {
    yield node;
    return;
  }
  for (final child in node.children) {
    yield* _leavesOf(child);
  }
}

/// Resolved LIST shape (see the assembler for the backward-compatibility rules).
final class _ListShape {
  const _ListShape({
    required this.repeated,
    required this.element,
    required this.consumed,
  });

  final SchemaNode repeated;
  final SchemaNode element;
  final bool consumed;
}

/// Resolved MAP shape.
final class _MapShape {
  const _MapShape({required this.repeated, required this.key, this.value});

  final SchemaNode repeated;
  final SchemaNode key;
  final SchemaNode? value;
}

_ListShape? _resolveList(SchemaNode node) {
  if (node.isLeaf || node.logicalType?.kind != LogicalTypeKind.list) {
    return null;
  }
  if (node.children.length != 1) {
    throw ParquetFormatException(
      'LIST group ${node.dotPath} must have exactly one child',
    );
  }
  final repeated = node.children.single;
  if (!repeated.isRepeated) {
    throw ParquetFormatException(
      'LIST group ${node.dotPath} must contain a repeated child',
    );
  }
  if (repeated.isLeaf) {
    return _ListShape(repeated: repeated, element: repeated, consumed: true);
  }
  if (repeated.children.isEmpty) {
    throw ParquetFormatException(
      'LIST group ${node.dotPath} has a repeated group without children',
    );
  }
  if (repeated.children.length > 1 ||
      repeated.name == 'array' ||
      repeated.name == '${node.name}_tuple') {
    return _ListShape(repeated: repeated, element: repeated, consumed: true);
  }
  return _ListShape(
    repeated: repeated,
    element: repeated.children.single,
    consumed: false,
  );
}

bool _isMapNode(SchemaNode node) {
  if (node.isLeaf) return false;
  final kind = node.logicalType?.kind;
  if (kind == LogicalTypeKind.map) return true;
  if (node.convertedType == ConvertedType.map) return true;
  if (node.convertedType == ConvertedType.mapKeyValue &&
      _looksLikeMapWrapper(node)) {
    return true;
  }
  if (node.children.length == 1) {
    final child = node.children.single;
    if (child.isRepeated &&
        child.convertedType == ConvertedType.mapKeyValue &&
        child.children.isNotEmpty &&
        child.children.length <= 2) {
      return true;
    }
  }
  return false;
}

bool _looksLikeMapWrapper(SchemaNode node) {
  if (node.children.length != 1) return false;
  final child = node.children.single;
  return child.isRepeated &&
      child.children.isNotEmpty &&
      child.children.length <= 2;
}

_MapShape? _resolveMap(SchemaNode node) {
  if (!_isMapNode(node)) return null;
  if (node.children.length != 1) {
    throw ParquetFormatException(
      'MAP group ${node.dotPath} must have exactly one child',
    );
  }
  final repeated = node.children.single;
  if (!repeated.isRepeated) {
    throw ParquetFormatException(
      'MAP group ${node.dotPath} must contain a repeated key/value child',
    );
  }
  if (repeated.children.isEmpty) {
    throw ParquetFormatException('MAP group ${node.dotPath} has no key field');
  }
  if (repeated.children.length > 2) {
    throw ParquetFormatException(
      'MAP group ${node.dotPath} has more than key and value fields',
    );
  }
  return _MapShape(
    repeated: repeated,
    key: repeated.children.first,
    value: repeated.children.length == 2 ? repeated.children[1] : null,
  );
}

/// Materialises the canonical writer representation of value [index].
///
/// * INT64 becomes an exact [Int64Value] limb pair.
/// * BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY / INT96 become zero-copy byte views.
Object? canonicalValue(ParquetColumn column, int index) {
  if (column is Int32Column) return column.values[index];
  if (column is Int64Column) {
    final limbs = column.values.getLimbs(index);
    return Int64Value(limbs.$1, limbs.$2);
  }
  if (column is Float32Column) return column.values[index];
  if (column is Float64Column) return column.values[index];
  if (column is BooleanColumn) return column.getBool(index);
  if (column is BinaryColumn) {
    final start = column.offsets[index];
    final end = column.offsets[index + 1];
    return Uint8List.sublistView(column.data, start, end);
  }
  if (column is FixedLenColumn) {
    final start = index * column.width;
    return Uint8List.sublistView(column.data, start, start + column.width);
  }
  if (column is Int96Column) {
    final start = index * 12;
    return Uint8List.sublistView(column.data, start, start + 12);
  }
  throw ParquetUnsupportedException(
    'unsupported column type ${column.runtimeType} for ${column.name}',
  );
}
