/// Dremel-style shredding: turns assembled [ParquetTable] columns back into
/// per-leaf definition/repetition level streams.
///
/// This is the inverse of `lib/src/table/nested_assembler.dart` for the shapes
/// the reader supports:
///
///  * flat leaves (required or optional);
///  * structs of leaves, including one nested struct level;
///  * LIST of a leaf or of a struct of leaves;
///  * MAP with a leaf key and a leaf value;
///  * legacy bare repeated leaves at the top level.
///
/// Other shapes (list of list, map of struct, list/map inside a struct, ...)
/// are rejected with a [ParquetUnsupportedException] so the writer never emits
/// a file the reader cannot reconstruct.
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

/// Validates that [schema] only contains shapes this writer (and the matching
/// reader) supports.
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
    if (node.isRepeated) {
      throw ParquetUnsupportedException(
        'bare repeated leaf ${node.dotPath} is not supported; use a LIST '
        'annotation instead',
      );
    }
    if (node.physicalType == PhysicalType.fixedLenByteArray &&
        (node.typeLength == null || node.typeLength! <= 0)) {
      throw ParquetUnsupportedException(
        'FIXED_LEN_BYTE_ARRAY ${node.dotPath} needs a positive type_length',
      );
    }
    return;
  }

  if (node.isRepeated) {
    throw ParquetUnsupportedException(
      'bare repeated group ${node.dotPath} is not supported',
    );
  }

  if (isListNode(node)) {
    if (node.children.length != 1) {
      throw ParquetUnsupportedException(
        'LIST group ${node.dotPath} must have exactly one child',
      );
    }
    final repeated = node.children.single;
    if (!repeated.isRepeated || repeated.children.length != 1) {
      throw ParquetUnsupportedException(
        'LIST group ${node.dotPath} must wrap a repeated group with one child',
      );
    }
    final element = repeated.children.single;
    if (element.isRepeated) {
      throw ParquetUnsupportedException(
        'LIST element ${element.dotPath} must not be repeated',
      );
    }
    if (element.isLeaf) return;
    final elementKind = element.logicalType?.kind;
    if (elementKind == LogicalTypeKind.list ||
        elementKind == LogicalTypeKind.map ||
        isListNode(element)) {
      throw ParquetUnsupportedException(
        'list of ${elementKind?.name ?? 'list'} is not supported '
        '(${element.dotPath})',
      );
    }
    for (final child in element.children) {
      if (!child.isLeaf || child.isRepeated) {
        throw ParquetUnsupportedException(
          'list of nested structs deeper than one level is not supported '
          '(${child.dotPath})',
        );
      }
    }
    return;
  }

  if (node.logicalType?.kind == LogicalTypeKind.map) {
    if (node.children.length != 1) {
      throw ParquetUnsupportedException(
        'MAP group ${node.dotPath} must have exactly one child',
      );
    }
    final repeated = node.children.single;
    if (!repeated.isRepeated || repeated.children.length < 2) {
      throw ParquetUnsupportedException(
        'MAP group ${node.dotPath} must wrap a repeated key/value group',
      );
    }
    final keyNode = repeated.children[0];
    final valueNode = repeated.children[1];
    if (!keyNode.isLeaf || !valueNode.isLeaf) {
      throw ParquetUnsupportedException(
        'MAP with non-leaf key/value is not supported (${node.dotPath})',
      );
    }
    if (keyNode.isRepeated || valueNode.isRepeated) {
      throw ParquetUnsupportedException(
        'MAP key/value must not be repeated (${node.dotPath})',
      );
    }
    return;
  }

  // Struct.
  for (final child in node.children) {
    if (child.isLeaf) {
      if (child.isRepeated) {
        throw ParquetUnsupportedException(
          'repeated leaf ${child.dotPath} inside a struct is not supported',
        );
      }
      continue;
    }
    if (child.isRepeated) {
      throw ParquetUnsupportedException(
        'repeated group ${child.dotPath} inside a struct is not supported',
      );
    }
    final childKind = child.logicalType?.kind;
    if (childKind == LogicalTypeKind.list ||
        childKind == LogicalTypeKind.map ||
        isListNode(child)) {
      throw ParquetUnsupportedException(
        'nested ${childKind?.name ?? 'list'} inside a struct is not supported '
        '(${child.dotPath})',
      );
    }
    for (final grand in child.children) {
      if (!grand.isLeaf || grand.isRepeated) {
        throw ParquetUnsupportedException(
          'struct nesting deeper than one level is not supported '
          '(${grand.dotPath})',
        );
      }
    }
  }
}

/// Whether [node] is a LIST group (modern annotation or the legacy shape the
/// reader recognises).
bool isListNode(SchemaNode node) {
  if (node.isLeaf) return false;
  if (node.logicalType?.kind == LogicalTypeKind.list) return true;
  return node.children.length == 1 &&
      node.children.first.isRepeated &&
      node.children.first.name == 'list';
}

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
    _validateColumn(node, column);
  }
}

void _validateColumn(SchemaNode node, ParquetColumn column) {
  if (node.isLeaf) {
    _requireLeafColumn(node, column);
    return;
  }
  if (isListNode(node)) {
    if (column is! ListColumn) {
      throw ArgumentError(
        'column "${node.name}" is ${column.runtimeType}, expected ListColumn',
      );
    }
    final element = node.children.single.children.single;
    _validateColumn(element, column.child);
    return;
  }
  if (node.logicalType?.kind == LogicalTypeKind.map) {
    if (column is! MapColumn) {
      throw ArgumentError(
        'column "${node.name}" is ${column.runtimeType}, expected MapColumn',
      );
    }
    final repeated = node.children.single;
    _validateColumn(repeated.children[0], column.keys);
    _validateColumn(repeated.children[1], column.values);
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
    _validateColumn(child, field);
  }
  for (final name in column.fields.keys) {
    if (!node.children.any((child) => child.name == name)) {
      throw ArgumentError(
        'struct column "${node.name}" has unexpected field "$name"',
      );
    }
  }
}

void _requireLeafColumn(SchemaNode node, ParquetColumn column) {
  // A bare repeated leaf is represented by a [ListColumn] of its element type.
  if (node.isRepeated) {
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
    List<int> reps,
  ) {
    if (slots.isEmpty) return;

    if (node.isRepeated) {
      _shredBareRepeated(node, column, slots, defs, reps);
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

    // Required node: nulls cannot be represented.
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

    if (isListNode(node)) {
      _shredList(node, column, slots, defs, reps);
      return;
    }

    if (node.logicalType?.kind == LogicalTypeKind.map) {
      _shredMap(node, column, slots, defs, reps);
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
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    final list = column as ListColumn;
    final repeated = node.children.single;
    final element = repeated.children.single;
    for (var i = 0; i < slots.length; i++) {
      final start = list.offsets[slots[i]];
      final end = list.offsets[slots[i] + 1];
      if (start == end) {
        _emitMissing(repeated, defs[i], reps[i]);
        continue;
      }
      final def = defs[i] + 1; // the repeated group is defined
      for (var k = start; k < end; k++) {
        final rep = k == start ? reps[i] : repeated.repLevel;
        _shred(element, list.child, <int>[k], <int>[def], <int>[rep]);
      }
    }
  }

  void _shredMap(
    SchemaNode node,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    final map = column as MapColumn;
    final repeated = node.children.single;
    final keyNode = repeated.children[0];
    final valueNode = repeated.children[1];
    for (var i = 0; i < slots.length; i++) {
      final start = map.offsets[slots[i]];
      final end = map.offsets[slots[i] + 1];
      if (start == end) {
        _emitMissing(repeated, defs[i], reps[i]);
        continue;
      }
      final def = defs[i] + 1;
      for (var k = start; k < end; k++) {
        final rep = k == start ? reps[i] : repeated.repLevel;
        _shred(keyNode, map.keys, <int>[k], <int>[def], <int>[rep]);
        _shred(valueNode, map.values, <int>[k], <int>[def], <int>[rep]);
      }
    }
  }

  void _shredBareRepeated(
    SchemaNode node,
    ParquetColumn column,
    List<int> slots,
    List<int> defs,
    List<int> reps,
  ) {
    if (!node.isLeaf) {
      throw ParquetUnsupportedException(
        'bare repeated group ${node.dotPath} is not supported',
      );
    }
    final list = column as ListColumn;
    final data = _leaf(node);
    for (var i = 0; i < slots.length; i++) {
      final start = list.offsets[slots[i]];
      final end = list.offsets[slots[i] + 1];
      if (start == end) {
        data.defs.add(defs[i]);
        data.reps.add(reps[i]);
        continue;
      }
      for (var k = start; k < end; k++) {
        data.defs.add(defs[i] + 1);
        data.reps.add(k == start ? reps[i] : node.repLevel);
        data.values.add(canonicalValue(list.child, k));
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
