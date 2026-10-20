/// Dremel-style record assembly for LIST/STRUCT/MAP schemas.
///
/// v0.1.0 supports the common shapes:
///  * optional/required struct of leaves and nested structs;
///  * optional/required LIST of a leaf or a struct of leaves;
///  * optional/required MAP from a leaf key to a leaf value.
/// Deeper combinations (list of list, map inside list, ...) are reported as
/// unsupported and land in v0.1.1.
library;

import 'dart:typed_data';

import '../schema/schema.dart';
import '../util/errors.dart';
import '../table/column.dart';
import '../column/column_builder.dart';
import '../column/page_reader.dart';

/// Builds one top-level column from the decoded leaf chunks of a row group.
ParquetColumn buildNodeColumn(
  SchemaNode node,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  if (node.isLeaf) {
    final descriptor = schema.column(node.leafIndex!);
    final decoded = leaves[node.leafIndex!];
    if (decoded == null) {
      throw ParquetFormatException('missing leaf ${descriptor.dotPath}');
    }
    return buildFlatColumn(descriptor, decoded);
  }
  final kind = node.logicalType?.kind;
  if (kind == LogicalTypeKind.list || _isLegacyList(node)) {
    return _buildList(node, schema, leaves, numRows);
  }
  if (kind == LogicalTypeKind.map) {
    return _buildMap(node, schema, leaves, numRows);
  }
  if (node.isRepeated && !node.isLeaf) {
    // Bare repeated group without LIST annotation is a list of structs.
    return _buildBareList(node, schema, leaves, numRows);
  }
  return _buildStruct(node, schema, leaves, numRows);
}

bool _isLegacyList(SchemaNode node) =>
    node.isGroup &&
    node.children.length == 1 &&
    node.children.first.isRepeated &&
    node.children.first.name == 'list';

/// Builds the struct column for [node] with one slot per row.
ParquetColumn _buildStruct(
  SchemaNode node,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  final fields = <String, ParquetColumn>{};
  for (final child in node.children) {
    if (child.isLeaf) {
      final descriptor = schema.column(child.leafIndex!);
      final decoded = leaves[child.leafIndex!];
      if (decoded == null) {
        throw ParquetFormatException('missing leaf ${descriptor.dotPath}');
      }
      fields[child.name] = buildFlatColumn(descriptor, decoded);
    } else if (child.isGroup && !child.isRepeated) {
      final childKind = child.logicalType?.kind;
      if (childKind == LogicalTypeKind.list ||
          childKind == LogicalTypeKind.map) {
        throw ParquetUnsupportedException(
          'nested ${childKind?.name} inside a struct is not supported in '
          'v0.1.0 (${child.name})',
        );
      }
      final childFields = <String, ParquetColumn>{};
      for (final grand in child.children) {
        if (!grand.isLeaf) {
          throw ParquetUnsupportedException(
            'struct nesting deeper than one level is not supported in v0.1.0',
          );
        }
        final descriptor = schema.column(grand.leafIndex!);
        final decoded = leaves[grand.leafIndex!];
        if (decoded == null) {
          throw ParquetFormatException('missing leaf ${descriptor.dotPath}');
        }
        childFields[grand.name] = buildFlatColumn(descriptor, decoded);
      }
      final childValidity = _structValidity(child, leaves, numRows);
      fields[child.name] = StructColumn(
        name: child.name,
        length: numRows,
        fields: childFields,
        validity: childValidity,
      );
    } else {
      throw ParquetUnsupportedException(
        'unsupported struct child ${child.name} in v0.1.0',
      );
    }
  }
  return StructColumn(
    name: node.name,
    length: numRows,
    fields: fields,
    validity: _structValidity(node, leaves, numRows),
  );
}

Validity _structValidity(
  SchemaNode node,
  Map<int, DecodedColumnChunk> leaves,
  int length,
) {
  final validity = Validity(length);
  if (node.repetition != FieldRepetitionType.optional) return validity;
  final leaf = _firstLeaf(node);
  if (leaf == null) return validity;
  final decoded = leaves[leaf.leafIndex!];
  if (decoded == null || decoded.defLevels == null) return validity;
  final defs = decoded.defLevels!;
  if (defs.length != length) {
    throw ParquetFormatException(
      'struct ${node.name} has ${defs.length} definition levels, expected '
      '$length',
    );
  }
  for (var i = 0; i < length; i++) {
    if (defs[i] < node.defLevel) validity.setNull(i);
  }
  return validity;
}

/// Builds a LIST column. The outer node is the annotated group.
ParquetColumn _buildList(
  SchemaNode node,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  if (node.children.length != 1) {
    throw ParquetFormatException('LIST group must have exactly one child');
  }
  final repeated = node.children.first;
  if (repeated.repetition != FieldRepetitionType.repeated ||
      repeated.children.length != 1) {
    throw ParquetFormatException('LIST repeated group must have one child');
  }
  return _buildListForRepeated(
    outer: node,
    repeated: repeated,
    element: repeated.children.first,
    schema: schema,
    leaves: leaves,
    numRows: numRows,
  );
}

ParquetColumn _buildBareList(
  SchemaNode repeated,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  // Legacy repeated field: the repeated node may be a leaf or a group.
  if (repeated.isLeaf) {
    final descriptor = schema.column(repeated.leafIndex!);
    final decoded = leaves[repeated.leafIndex!];
    if (decoded == null) {
      throw ParquetFormatException('missing leaf ${descriptor.dotPath}');
    }
    final rows = _splitRows(decoded, numRows);
    final offsets = Uint32List(numRows + 1);
    final entries = <int>[];
    for (var r = 0; r < numRows; r++) {
      entries.addAll(rows[r]);
      offsets[r + 1] = entries.length;
    }
    final child = _buildPrimitiveFromEntries(
      descriptor,
      decoded,
      entries,
      entries.length,
    );
    return ListColumn(
      name: repeated.name,
      offsets: offsets,
      child: child,
      validity: Validity(numRows),
    );
  }
  throw ParquetUnsupportedException(
    'bare repeated group ${repeated.name} is not supported in v0.1.0',
  );
}

ParquetColumn _buildListForRepeated({
  required SchemaNode outer,
  required SchemaNode repeated,
  required SchemaNode element,
  required ParquetSchema schema,
  required Map<int, DecodedColumnChunk> leaves,
  required int numRows,
}) {
  final repLeaf = _firstLeaf(repeated);
  if (repLeaf == null) {
    throw ParquetFormatException('LIST ${outer.name} has no leaf');
  }
  final repDecoded = leaves[repLeaf.leafIndex!];
  if (repDecoded == null) {
    throw ParquetFormatException('missing LIST leaf ${repLeaf.name}');
  }
  final rows = _splitRows(repDecoded, numRows);
  final offsets = Uint32List(numRows + 1);
  // Per-leaf element entry indices, in row-major element order.
  final entriesByLeaf = <int, List<int>>{};
  for (final leaf in _allLeaves(repeated)) {
    final decoded = leaves[leaf.leafIndex!];
    if (decoded == null) {
      throw ParquetFormatException('missing LIST leaf ${leaf.name}');
    }
    final leafRows = _splitRows(decoded, numRows);
    final entries = <int>[];
    for (var r = 0; r < numRows; r++) {
      for (final index in leafRows[r]) {
        final def = _defAt(decoded, index);
        if (def >= repeated.defLevel) entries.add(index);
      }
    }
    entriesByLeaf[leaf.leafIndex!] = entries;
  }
  var total = 0;
  for (var r = 0; r < numRows; r++) {
    var count = 0;
    for (final index in rows[r]) {
      if (_defAt(repDecoded, index) >= repeated.defLevel) count++;
    }
    total += count;
    offsets[r + 1] = total;
  }
  for (final entry in entriesByLeaf.entries) {
    if (entry.value.length != total) {
      throw ParquetFormatException(
        'LIST ${outer.name} leaf ${entry.key} has ${entry.value.length} '
        'elements, expected $total',
      );
    }
  }

  final child = _buildElement(
    element,
    schema,
    leaves,
    entriesByLeaf,
    total,
    repeated.defLevel,
  );
  return ListColumn(
    name: outer.name,
    offsets: offsets,
    child: child,
    validity: _listValidity(outer, offsets, rows, repDecoded, numRows),
  );
}

ParquetColumn _buildElement(
  SchemaNode element,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  Map<int, List<int>> entriesByLeaf,
  int length,
  int repeatedDef,
) {
  if (element.isLeaf) {
    final descriptor = schema.column(element.leafIndex!);
    final decoded = leaves[element.leafIndex!];
    if (decoded == null) {
      throw ParquetFormatException('missing element leaf');
    }
    return _buildPrimitiveFromEntries(
      descriptor,
      decoded,
      entriesByLeaf[element.leafIndex!]!,
      length,
    );
  }
  final kind = element.logicalType?.kind;
  if (kind == LogicalTypeKind.list || kind == LogicalTypeKind.map) {
    throw ParquetUnsupportedException(
      'list of ${kind?.name} is not supported in v0.1.0',
    );
  }
  final fields = <String, ParquetColumn>{};
  for (final child in element.children) {
    if (!child.isLeaf) {
      throw ParquetUnsupportedException(
        'list of nested structs deeper than one level is not supported in '
        'v0.1.0',
      );
    }
    final descriptor = schema.column(child.leafIndex!);
    final decoded = leaves[child.leafIndex!];
    if (decoded == null) throw ParquetFormatException('missing element leaf');
    fields[child.name] = _buildPrimitiveFromEntries(
      descriptor,
      decoded,
      entriesByLeaf[child.leafIndex!]!,
      length,
    );
  }
  return StructColumn(
    name: element.name,
    length: length,
    fields: fields,
    validity: _elementValidity(element, leaves, length),
  );
}

Validity _elementValidity(
  SchemaNode node,
  Map<int, DecodedColumnChunk> leaves,
  int length,
) {
  final validity = Validity(length);
  if (node.repetition != FieldRepetitionType.optional) return validity;
  final leaf = _firstLeaf(node);
  if (leaf == null) return validity;
  final decoded = leaves[leaf.leafIndex!];
  if (decoded == null || decoded.defLevels == null) return validity;
  final defs = decoded.defLevels!;
  // Entries are passed in for the element; the validity can only be computed
  // from the entries, so callers should use _buildPrimitiveFromEntries which
  // already handles nulls. This fallback leaves all valid.
  if (defs.length < length) return validity;
  return validity;
}

Validity _listValidity(
  SchemaNode outer,
  Uint32List offsets,
  List<List<int>> rows,
  DecodedColumnChunk repDecoded,
  int numRows,
) {
  final validity = Validity(numRows);
  if (outer.repetition != FieldRepetitionType.optional) return validity;
  for (var r = 0; r < numRows; r++) {
    final first = rows[r].first;
    if (_defAt(repDecoded, first) < outer.defLevel) {
      validity.setNull(r);
    }
  }
  return validity;
}

ParquetColumn _buildMap(
  SchemaNode node,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  if (node.children.length != 1) {
    throw ParquetFormatException('MAP group must have exactly one child');
  }
  final repeated = node.children.first;
  if (repeated.repetition != FieldRepetitionType.repeated ||
      repeated.children.length < 2) {
    throw ParquetFormatException('MAP repeated group must have key/value');
  }
  final keyNode = repeated.children[0];
  final valueNode = repeated.children[1];
  if (!keyNode.isLeaf || !valueNode.isLeaf) {
    throw ParquetUnsupportedException(
      'MAP with non-leaf key/value is not supported in v0.1.0',
    );
  }
  final keyLeaf = _firstLeaf(repeated)!;
  final keyDecoded = leaves[keyNode.leafIndex!]!;
  final valueDecoded = leaves[valueNode.leafIndex!]!;
  final rows = _splitRows(keyDecoded, numRows);
  final keyEntries = <int>[];
  final valueEntries = <int>[];
  final valueRows = _splitRows(valueDecoded, numRows);
  final offsets = Uint32List(numRows + 1);
  for (var r = 0; r < numRows; r++) {
    for (final index in rows[r]) {
      if (_defAt(keyDecoded, index) >= repeated.defLevel) {
        keyEntries.add(index);
      }
    }
    for (final index in valueRows[r]) {
      if (_defAt(valueDecoded, index) >= repeated.defLevel) {
        valueEntries.add(index);
      }
    }
    offsets[r + 1] = keyEntries.length;
  }
  if (keyEntries.length != valueEntries.length) {
    throw ParquetFormatException(
      'MAP ${node.name} key/value entry counts differ',
    );
  }
  final keyDesc = schema.column(keyLeaf.leafIndex!);
  final valueDesc = schema.column(valueNode.leafIndex!);
  final keyColumn = _buildPrimitiveFromEntries(
    keyDesc,
    keyDecoded,
    keyEntries,
    keyEntries.length,
  );
  final valueColumn = _buildPrimitiveFromEntries(
    valueDesc,
    valueDecoded,
    valueEntries,
    valueEntries.length,
  );
  return MapColumn(
    name: node.name,
    offsets: offsets,
    keys: keyColumn,
    values: valueColumn,
    validity: _listValidity(node, offsets, rows, keyDecoded, numRows),
  );
}

ParquetColumn _buildPrimitiveFromEntries(
  ColumnDescriptor descriptor,
  DecodedColumnChunk decoded,
  List<int> entries,
  int length,
) {
  final validity = Validity(length);
  final maxDef = descriptor.maxDefLevel;
  for (var slot = 0; slot < entries.length; slot++) {
    final def = _defAt(decoded, entries[slot]);
    if (def < maxDef) validity.setNull(slot);
  }
  return buildColumnFromCompact(descriptor, decoded.values, validity);
}

/// Splits the entries of [decoded] into top-level rows.
List<List<int>> _splitRows(DecodedColumnChunk decoded, int numRows) {
  final reps = decoded.repLevels;
  if (reps == null) {
    if (decoded.numValues != numRows) {
      throw ParquetFormatException(
        'flat leaf has ${decoded.numValues} values, expected $numRows rows',
      );
    }
    return List<List<int>>.generate(numRows, (i) => <int>[i], growable: false);
  }
  final rows = <List<int>>[];
  var current = <int>[];
  for (var i = 0; i < decoded.numValues; i++) {
    if (reps[i] == 0 && current.isNotEmpty) {
      rows.add(current);
      current = <int>[];
    }
    current.add(i);
  }
  if (current.isNotEmpty) rows.add(current);
  if (rows.length != numRows) {
    throw ParquetFormatException(
      'repetition levels describe ${rows.length} rows, expected $numRows',
    );
  }
  return rows;
}

int _defAt(DecodedColumnChunk decoded, int index) =>
    decoded.defLevels == null ? 0x7FFFFFFF : decoded.defLevels![index];

SchemaNode? _firstLeaf(SchemaNode node) {
  if (node.isLeaf) return node;
  for (final child in node.children) {
    final leaf = _firstLeaf(child);
    if (leaf != null) return leaf;
  }
  return null;
}

Iterable<SchemaNode> _allLeaves(SchemaNode node) sync* {
  if (node.isLeaf) {
    yield node;
    return;
  }
  for (final child in node.children) {
    yield* _allLeaves(child);
  }
}
