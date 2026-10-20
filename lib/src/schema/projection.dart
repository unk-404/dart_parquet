/// Schema projection (top-level or dotted leaf paths) for the reader.
library;

import '../format/parquet_format.dart';
import '../util/errors.dart';
import 'schema.dart';

/// A source schema pruned to a set of requested columns.
///
/// [sourceColumns] are the original leaf descriptors in projected leaf order;
/// [schema] is the reconstructed schema containing only those leaves and the
/// groups needed to reach them.
final class ProjectedSchema {
  ProjectedSchema({required this.schema, required this.sourceColumns});

  /// Projected schema tree.
  final ParquetSchema schema;

  /// Original leaf descriptors, in the same order as [schema]'s leaves.
  final List<ColumnDescriptor> sourceColumns;
}

/// Projects [source] to [names].
///
/// A name may be a top-level field (`struct`, `list_int`) or a dotted leaf
/// path (`struct.a`, `list_int.list.element`). An empty/null list keeps all
/// columns. Unknown names throw [ParquetFormatException].
ProjectedSchema projectSchema(ParquetSchema source, List<String>? names) {
  final selected = <String>{};
  if (names == null || names.isEmpty) {
    for (final column in source.columns) {
      selected.add(column.dotPath);
    }
  } else {
    final topLevel = {for (final node in source.root.children) node.name: node};
    final byPath = {
      for (final column in source.columns) column.dotPath: column,
    };
    for (final name in names) {
      final top = topLevel[name];
      if (top != null) {
        selected.addAll(_leafPaths(top));
        continue;
      }
      final column = byPath[name];
      if (column == null) {
        throw ParquetFormatException('no such column: $name');
      }
      selected.add(column.dotPath);
    }
  }
  if (selected.isEmpty) {
    throw ParquetFormatException('projection selected no columns');
  }

  // A map always needs both its key and value leaves to reconstruct entries.
  // If any map leaf is selected, include every leaf under that map.
  for (final node in _groups(source.root)) {
    if (node.logicalType?.kind != LogicalTypeKind.map) continue;
    final prefix = '${node.dotPath}.';
    if (selected.any((path) => path.startsWith(prefix))) {
      selected.addAll(_leafPaths(node));
    }
  }

  bool include(SchemaNode node) {
    if (node.isLeaf) return selected.contains(node.dotPath);
    return node.children.any(include);
  }

  List<SchemaNode> includedChildren(SchemaNode node) => [
    for (final child in node.children)
      if (include(child)) child,
  ];

  final elements = <SchemaElement>[];
  void emit(SchemaNode node, {required bool isRoot}) {
    final children = node.isLeaf
        ? const <SchemaNode>[]
        : includedChildren(node);
    elements.add(
      _cloneElement(node, isRoot: isRoot, childCount: children.length),
    );
    for (final child in children) {
      emit(child, isRoot: false);
    }
  }

  emit(source.root, isRoot: true);
  final projected = ParquetSchema.fromSchemaElements(elements);
  final sourceColumns = <ColumnDescriptor>[
    for (final column in source.columns)
      if (selected.contains(column.dotPath)) column,
  ];
  return ProjectedSchema(schema: projected, sourceColumns: sourceColumns);
}

Iterable<SchemaNode> _groups(SchemaNode node) sync* {
  if (node.isGroup) {
    yield node;
    for (final child in node.children) {
      yield* _groups(child);
    }
  }
}

Iterable<String> _leafPaths(SchemaNode node) sync* {
  if (node.isLeaf) {
    yield node.dotPath;
    return;
  }
  for (final child in node.children) {
    yield* _leafPaths(child);
  }
}

SchemaElement _cloneElement(
  SchemaNode node, {
  required bool isRoot,
  required int childCount,
}) {
  final logical = node.logicalType;
  return SchemaElement(
    type: isRoot ? null : node.physicalType,
    typeLength: node.typeLength,
    repetitionType: isRoot ? null : node.repetition,
    name: node.name,
    numChildren: node.isLeaf ? null : childCount,
    convertedType: node.convertedType,
    scale: node.scale,
    precision: node.precision,
    fieldId: node.fieldId,
    logicalType: logical,
  );
}
