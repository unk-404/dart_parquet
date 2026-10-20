/// Parquet schema tree, definition/repetition levels and leaf descriptors.
library;

import '../format/parquet_format.dart';
import '../util/errors.dart';

export '../format/enums.dart'
    show ConvertedType, FieldRepetitionType, PhysicalType;
export '../format/logical_type.dart';

/// One node of the Parquet schema tree.
///
/// The tree is built from the flattened `FileMetaData.schema` list. [defLevel]
/// and [repLevel] are the maximum definition/repetition levels at this node
/// (i.e. the number of optional/repeated nodes on the path from the root,
/// including this node).
final class SchemaNode {
  SchemaNode._({
    required this.name,
    required this.parent,
    required this.repetition,
    required this.physicalType,
    required this.typeLength,
    required this.numChildren,
    required this.convertedType,
    required this.declaredLogicalType,
    required this.logicalType,
    required this.scale,
    required this.precision,
    required this.fieldId,
    required this.defLevel,
    required this.repLevel,
    required this.leafIndex,
  });

  /// Field name.
  final String name;

  /// Parent node; `null` for the root.
  final SchemaNode? parent;

  /// Field repetition. The root is always [FieldRepetitionType.required].
  final FieldRepetitionType repetition;

  /// Physical type for leaves, `null` for groups.
  final PhysicalType? physicalType;

  /// Fixed byte length for `FIXED_LEN_BYTE_ARRAY` / maximum bit length.
  final int? typeLength;

  /// Number of children declared in the schema element (groups only).
  final int? numChildren;

  /// Legacy converted type annotation.
  final ConvertedType? convertedType;

  /// Logical type explicitly declared in the footer.
  final LogicalType? declaredLogicalType;

  /// Effective logical type: [declaredLogicalType] or the logical equivalent
  /// of [convertedType] when only the legacy annotation is present.
  final LogicalType? logicalType;

  /// Legacy decimal scale.
  final int? scale;

  /// Legacy decimal precision.
  final int? precision;

  /// Original field id, when present.
  final int? fieldId;

  /// Maximum definition level at this node.
  final int defLevel;

  /// Maximum repetition level at this node.
  final int repLevel;

  /// Zero-based index of this leaf among all leaves, or `null` for groups.
  final int? leafIndex;

  /// Child nodes in schema order.
  final List<SchemaNode> children = <SchemaNode>[];

  /// Whether this node has a physical type (is a column leaf).
  bool get isLeaf => physicalType != null;

  /// Whether this node is an inner (group) node.
  bool get isGroup => physicalType == null;

  /// Whether this field is optional.
  bool get isOptional => repetition == FieldRepetitionType.optional;

  /// Whether this field is repeated.
  bool get isRepeated => repetition == FieldRepetitionType.repeated;

  /// Whether this field is required.
  bool get isRequired => repetition == FieldRepetitionType.required;

  /// Alias for [defLevel].
  int get maxDefLevel => defLevel;

  /// Alias for [repLevel].
  int get maxRepLevel => repLevel;

  /// Path from the root's child down to this node (root excluded).
  List<String> get path {
    final parts = <String>[];
    SchemaNode? current = this;
    while (current != null && current.parent != null) {
      parts.add(current.name);
      current = current.parent;
    }
    return List<String>.unmodifiable(parts.reversed);
  }

  /// Dotted [path], e.g. `list_int.list.element`.
  String get dotPath => path.join('.');

  @override
  String toString() =>
      'SchemaNode($dotPath${isLeaf ? ', ${physicalType!.name}' : ', group'})';
}

/// A leaf column and its level information.
final class ColumnDescriptor {
  ColumnDescriptor._(this.node)
    : path = List<String>.unmodifiable(node.path),
      physicalType = node.physicalType!,
      typeLength = node.typeLength,
      logicalType = node.logicalType,
      maxDefLevel = node.defLevel,
      maxRepLevel = node.repLevel,
      leafIndex = node.leafIndex!;

  /// Schema node backing this column.
  final SchemaNode node;

  /// Path without the root name.
  final List<String> path;

  /// Leaf physical type.
  final PhysicalType physicalType;

  /// Fixed byte length (or maximum bit length), if declared.
  final int? typeLength;

  /// Effective logical type.
  final LogicalType? logicalType;

  /// Maximum definition level.
  final int maxDefLevel;

  /// Maximum repetition level.
  final int maxRepLevel;

  /// Zero-based leaf index.
  final int leafIndex;

  /// Field name (the last path segment).
  String get name => path.isEmpty ? node.name : path.last;

  /// Dotted path, e.g. `list_int.list.element`.
  String get dotPath => path.join('.');

  /// Repetition of the leaf field.
  FieldRepetitionType get repetition => node.repetition;

  /// Legacy converted type, if any.
  ConvertedType? get convertedType => node.convertedType;

  /// Legacy decimal scale.
  int? get scale => node.scale;

  /// Legacy decimal precision.
  int? get precision => node.precision;

  @override
  String toString() => 'ColumnDescriptor($dotPath, $physicalType)';
}

/// Parsed Parquet schema.
final class ParquetSchema {
  ParquetSchema._(this.root, List<ColumnDescriptor> columns)
    : columns = List<ColumnDescriptor>.unmodifiable(columns),
      _columnsByPath = <String, ColumnDescriptor>{
        for (final column in columns) _pathKey(column.path): column,
      };

  /// Builds a schema tree from the flattened `FileMetaData.schema` list.
  ///
  /// The first element is the root; children are listed depth-first.
  factory ParquetSchema.fromSchemaElements(List<SchemaElement> elements) =>
      _SchemaBuilder(elements).build();

  /// Builds a schema from already-parsed [FileMetaData].
  factory ParquetSchema.fromFileMetaData(FileMetaData metadata) =>
      ParquetSchema.fromSchemaElements(metadata.schema);

  /// Convenience alias for [ParquetSchema.fromSchemaElements].
  factory ParquetSchema.fromSchema(List<SchemaElement> elements) =>
      ParquetSchema.fromSchemaElements(elements);

  /// Root group node.
  final SchemaNode root;

  /// Leaf columns in depth-first order.
  final List<ColumnDescriptor> columns;

  final Map<String, ColumnDescriptor> _columnsByPath;

  /// Number of leaf columns.
  int get numColumns => columns.length;

  /// Number of leaf columns (alias).
  int get numLeaves => columns.length;

  /// Returns the leaf column at [index].
  ///
  /// Throws [ParquetRangeException] when [index] is out of bounds.
  ColumnDescriptor column(int index) {
    if (index < 0 || index >= columns.length) {
      throw ParquetRangeException(
        'column index $index out of range (0..${columns.length - 1})',
      );
    }
    return columns[index];
  }

  /// Looks up a leaf column by its exact path (root excluded).
  ColumnDescriptor? columnByPath(List<String> path) =>
      _columnsByPath[_pathKey(path)];

  /// Looks up a leaf column by a dotted path such as `struct.a`.
  ColumnDescriptor? columnByPathString(String path, {String separator = '.'}) {
    if (path.isEmpty) return null;
    return columnByPath(path.split(separator));
  }

  /// Looks up a leaf column by its field name; returns the first match.
  ColumnDescriptor? columnByName(String name) {
    for (final column in columns) {
      if (column.name == name) return column;
    }
    return null;
  }

  static String _pathKey(List<String> path) => path.join('\u0000');

  @override
  String toString() => 'ParquetSchema(${columns.length} columns)';
}

final class _SchemaBuilder {
  _SchemaBuilder(this.elements);

  final List<SchemaElement> elements;
  final List<ColumnDescriptor> _columns = <ColumnDescriptor>[];
  int _index = 0;

  ParquetSchema build() {
    if (elements.isEmpty) {
      throw ParquetFormatException('Parquet schema is empty');
    }
    if (elements.first.type != null) {
      throw ParquetFormatException(
        'root schema element must be a group, got physical type '
        '${elements.first.type}',
      );
    }
    final root = _parseNode(
      isRoot: true,
      parent: null,
      parentDefLevel: 0,
      parentRepLevel: 0,
    );
    if (_index != elements.length) {
      throw ParquetFormatException(
        'schema has ${elements.length - _index} trailing element(s) after '
        'the root tree',
      );
    }
    return ParquetSchema._(root, _columns);
  }

  SchemaNode _parseNode({
    required bool isRoot,
    required SchemaNode? parent,
    required int parentDefLevel,
    required int parentRepLevel,
  }) {
    if (_index >= elements.length) {
      throw ParquetFormatException(
        'schema ended while reading ${isRoot ? 'the root' : 'a child'}',
      );
    }
    final element = elements[_index++];

    final repetition = isRoot
        ? FieldRepetitionType.required
        : element.repetitionType ??
              (throw ParquetFormatException(
                'non-root schema element "${element.name}" has no repetition',
              ));

    final defLevel =
        parentDefLevel + (repetition == FieldRepetitionType.required ? 0 : 1);
    final repLevel =
        parentRepLevel + (repetition == FieldRepetitionType.repeated ? 1 : 0);

    final physicalType = element.type;
    final declaredChildren = element.numChildren;
    if (declaredChildren != null && declaredChildren < 0) {
      throw ParquetFormatException(
        'schema element "${element.name}" has negative num_children',
      );
    }
    if (physicalType != null &&
        declaredChildren != null &&
        declaredChildren != 0) {
      throw ParquetFormatException(
        'schema element "${element.name}" is both a leaf and a group',
      );
    }
    if (physicalType == null && declaredChildren == null) {
      throw ParquetFormatException(
        'group schema element "${element.name}" has no num_children',
      );
    }

    final isLeaf = physicalType != null;
    final leafIndex = isLeaf ? _columns.length : null;
    final logicalType =
        element.logicalType ??
        logicalTypeFromConverted(
          element.convertedType,
          scale: element.scale,
          precision: element.precision,
        );

    final node = SchemaNode._(
      name: element.name,
      parent: parent,
      repetition: repetition,
      physicalType: physicalType,
      typeLength: element.typeLength,
      numChildren: declaredChildren,
      convertedType: element.convertedType,
      declaredLogicalType: element.logicalType,
      logicalType: logicalType,
      scale: element.scale,
      precision: element.precision,
      fieldId: element.fieldId,
      defLevel: defLevel,
      repLevel: repLevel,
      leafIndex: leafIndex,
    );

    if (isLeaf) {
      _columns.add(ColumnDescriptor._(node));
      return node;
    }

    final childCount = declaredChildren ?? 0;
    final remaining = elements.length - _index;
    if (childCount > remaining) {
      throw ParquetFormatException(
        'schema element "${element.name}" declares $childCount children but '
        'only $remaining element(s) remain',
      );
    }
    for (var i = 0; i < childCount; i++) {
      node.children.add(
        _parseNode(
          isRoot: false,
          parent: node,
          parentDefLevel: defLevel,
          parentRepLevel: repLevel,
        ),
      );
    }
    return node;
  }
}
