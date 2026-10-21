/// Generic Dremel-style record assembly for LIST/STRUCT/MAP schemas.
///
/// The assembler is level-driven rather than shape-specific: every schema node
/// is rebuilt from the definition/repetition levels of the leaves below it.
/// This supports arbitrary combinations of optional/required/repeated groups
/// and leaves, including:
///
///  * LIST of LIST, MAP of LIST, LIST of MAP, MAP of MAP;
///  * STRUCT inside LIST/MAP and LIST/MAP inside STRUCT;
///  * bare repeated leaves/groups (legacy two-level encoding);
///  * legacy two-level LIST shapes, resolved with the backward-compatibility
///    rules from `parquet-format` (LogicalTypes.md);
///  * MAP without a value field (set semantics) and `MAP_KEY_VALUE`.
///
/// Output types stay the public [ParquetColumn] hierarchy: [StructColumn],
/// [ListColumn], [MapColumn] and the primitive column classes.
library;

import 'dart:typed_data';

import '../column/column_builder.dart';
import '../column/page_reader.dart';
import '../schema/schema.dart';
import '../table/column.dart';
import '../util/errors.dart';
import '../variant/variant.dart';

/// Builds one top-level column from the decoded leaf chunks of a row group.
ParquetColumn buildNodeColumn(
  SchemaNode node,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  if (numRows < 0) {
    throw ParquetFormatException(
      'cannot assemble a column for a negative row count ($numRows)',
    );
  }
  if (!node.isLeaf && node.logicalType?.kind == LogicalTypeKind.variant) {
    return _buildVariantColumn(node, schema, leaves, numRows);
  }

  // Flat leaves are by far the most common case; keep the direct path.
  if (node.isLeaf && !node.isRepeated) {
    final descriptor = schema.column(node.leafIndex!);
    final decoded = leaves[node.leafIndex!];
    if (decoded == null) {
      throw ParquetFormatException('missing leaf ${descriptor.dotPath}');
    }
    if (decoded.numValues != numRows) {
      throw ParquetFormatException(
        'leaf ${descriptor.dotPath} has ${decoded.numValues} values, expected '
        '$numRows rows',
      );
    }
    return buildFlatColumn(descriptor, decoded);
  }

  final assembler = _Assembler(schema, leaves);
  final streams = assembler.rootStreams(node);
  final column = assembler.assemble(_Position(node), streams, numRows);
  if (column.length != numRows) {
    throw ParquetFormatException(
      'assembled column ${node.dotPath} has length ${column.length}, expected '
      '$numRows',
    );
  }
  return column;
}

/// Builds a VARIANT group, reconstructing unshredded and simple shredded
/// `typed_value` fields. Shredded arrays (LIST typed_value) remain a future
/// extension; object/primitive shredding is supported.
ParquetColumn _buildVariantColumn(
  SchemaNode node,
  ParquetSchema schema,
  Map<int, DecodedColumnChunk> leaves,
  int numRows,
) {
  SchemaNode? metadataNode;
  SchemaNode? valueNode;
  SchemaNode? typedNode;
  for (final child in node.children) {
    switch (child.name) {
      case 'metadata':
        metadataNode = child;
      case 'value':
        valueNode = child;
      case 'typed_value':
        typedNode = child;
    }
  }
  if (metadataNode == null ||
      !metadataNode.isLeaf ||
      typedNode == null && valueNode == null) {
    throw ParquetFormatException(
      'malformed VARIANT ${node.dotPath}: metadata is required',
    );
  }
  final columns = <int, ParquetColumn>{};
  ParquetColumn columnFor(SchemaNode child) {
    if (!child.isLeaf || child.isRepeated) {
      throw ParquetUnsupportedException(
        'shredded VARIANT array/map ${child.dotPath} is not supported in v0.2.0',
      );
    }
    final cached = columns[child.leafIndex!];
    if (cached != null) return cached;
    final descriptor = schema.column(child.leafIndex!);
    final decoded = leaves[child.leafIndex!];
    if (decoded == null) {
      throw ParquetFormatException('missing VARIANT leaf ${child.dotPath}');
    }
    if (descriptor.maxRepLevel != 0) {
      throw ParquetUnsupportedException(
        'shredded VARIANT array ${child.dotPath} is not supported in v0.2.0',
      );
    }
    final built = buildFlatColumn(descriptor, decoded);
    columns[child.leafIndex!] = built;
    return built;
  }

  final metadata = columnFor(metadataNode);
  if (metadata is! BinaryColumn || metadata.length != numRows) {
    throw ParquetFormatException(
      'VARIANT ${node.dotPath} metadata must be a BYTE_ARRAY column',
    );
  }
  final rawValues = valueNode == null ? null : columnFor(valueNode);
  if (rawValues != null &&
      (rawValues is! BinaryColumn || rawValues.length != numRows)) {
    throw ParquetFormatException(
      'VARIANT ${node.dotPath} value must be a BYTE_ARRAY column',
    );
  }

  VariantValue? typedAt(int row) {
    if (typedNode == null) return null;
    return _variantTypedAt(typedNode, row, columnFor);
  }

  VariantValue? rawAt(int row) {
    if (rawValues == null || rawValues.isNull(row)) return null;
    return VariantCodec.decode(
      (rawValues as BinaryColumn).getBytes(row),
      VariantMetadata.fromBytes(metadata.getBytes(row)),
    );
  }

  final validity = Validity(numRows);
  final decoded = List<Variant?>.filled(numRows, null);
  for (var row = 0; row < numRows; row++) {
    if (metadata.isNull(row)) {
      validity.setNull(row);
      continue;
    }
    final metadataValue = VariantMetadata.fromBytes(metadata.getBytes(row));
    final typed = typedAt(row);
    final raw = rawAt(row);
    VariantValue? value;
    if (typed != null) {
      if (raw is VariantObject && typed is VariantObject) {
        value = VariantObject(<String, VariantValue>{
          ...raw.fields,
          ...typed.fields,
        });
      } else {
        value = typed;
      }
    } else {
      value = raw ?? VariantValue.nullValue;
    }
    decoded[row] = Variant(metadata: metadataValue, value: value);
  }
  return VariantColumn(name: node.name, values: decoded, validity: validity);
}

VariantValue? _variantTypedAt(
  SchemaNode node,
  int row,
  ParquetColumn Function(SchemaNode) columnFor,
) {
  if (node.isLeaf) {
    final column = columnFor(node);
    if (column.isNull(row)) return null;
    return _variantPrimitive(node, column.value(row), column, row);
  }
  if (node.isRepeated) {
    throw ParquetUnsupportedException(
      'shredded VARIANT array ${node.dotPath} is not supported in v0.2.0',
    );
  }
  final kind = node.logicalType?.kind;
  if (kind == LogicalTypeKind.list || kind == LogicalTypeKind.map) {
    throw ParquetUnsupportedException(
      'shredded VARIANT ${kind?.name} ${node.dotPath} is not supported in '
      'v0.2.0',
    );
  }
  final fields = <String, VariantValue>{};
  for (final field in node.children) {
    SchemaNode? valueChild;
    SchemaNode? typedChild;
    for (final child in field.children) {
      if (child.name == 'value') valueChild = child;
      if (child.name == 'typed_value') typedChild = child;
    }
    VariantValue? value;
    if (typedChild != null) value = _variantTypedAt(typedChild, row, columnFor);
    if (value == null && valueChild != null) {
      final valueColumn = columnFor(valueChild);
      if (!valueColumn.isNull(row)) {
        throw ParquetFormatException(
          'partially shredded VARIANT ${field.dotPath} value is not '
          'reconstructable yet',
        );
      }
    }
    if (value != null) fields[field.name] = value;
  }
  return VariantObject(fields);
}

VariantValue _variantPrimitive(
  SchemaNode node,
  Object? value,
  ParquetColumn column,
  int row,
) {
  final type = node.physicalType;
  final logical = node.logicalType;
  switch (type) {
    case PhysicalType.boolean:
      return VariantBoolean(value as bool);
    case PhysicalType.int32:
      final intValue = value as int;
      if (logical is DateLogicalType) return VariantDate(intValue);
      if (logical is DecimalLogicalType) {
        return VariantDecimal(BigInt.from(intValue), scale: logical.scale);
      }
      if (logical is TimeLogicalType) {
        return VariantTime(intValue);
      }
      if (logical is IntegerLogicalType) {
        return VariantInt(intValue, switch (logical.bitWidth) {
          8 => VariantIntWidth.int8,
          16 => VariantIntWidth.int16,
          _ => VariantIntWidth.int32,
        });
      }
      return VariantInt(intValue, VariantIntWidth.int32);
    case PhysicalType.int64:
      final intValue = _variantInt(value);
      if (logical is TimestampLogicalType) {
        return VariantTimestamp(
          value: intValue,
          unit: logical.unit == TimeUnit.nanos
              ? VariantTimeUnit.nanos
              : VariantTimeUnit.micros,
          isAdjustedToUtc: logical.isAdjustedToUtc,
        );
      }
      if (logical is TimeLogicalType) return VariantTime(intValue);
      if (logical is DecimalLogicalType) {
        return VariantDecimal(BigInt.from(intValue), scale: logical.scale);
      }
      return VariantInt(intValue, VariantIntWidth.int64);
    case PhysicalType.float:
      return VariantFloat((value as num).toDouble());
    case PhysicalType.double:
      return VariantDouble((value as num).toDouble());
    case PhysicalType.byteArray:
      if (value is String) {
        return logical?.kind == LogicalTypeKind.json ||
                logical?.kind == LogicalTypeKind.enumType
            ? VariantString(value)
            : logical?.kind == LogicalTypeKind.string
            ? VariantString(value)
            : VariantBinary(value.codeUnits);
      }
      final bytes = value is Uint8List ? value : Uint8List(0);
      if (logical is DecimalLogicalType) {
        return VariantDecimal(
          _bigIntFromBigEndian(bytes),
          scale: logical.scale,
        );
      }
      if (logical?.kind == LogicalTypeKind.uuid) return VariantUuid(bytes);
      if (logical?.kind == LogicalTypeKind.string ||
          logical?.kind == LogicalTypeKind.enumType ||
          logical?.kind == LogicalTypeKind.json) {
        return VariantString(String.fromCharCodes(bytes));
      }
      return VariantBinary(bytes);
    case PhysicalType.fixedLenByteArray:
      final bytes = value is Uint8List ? value : Uint8List(0);
      if (logical is DecimalLogicalType) {
        return VariantDecimal(
          _bigIntFromBigEndian(bytes),
          scale: logical.scale,
        );
      }
      if (logical?.kind == LogicalTypeKind.uuid) return VariantUuid(bytes);
      return VariantBinary(bytes);
    case PhysicalType.int96:
      throw ParquetUnsupportedException(
        'shredded VARIANT INT96 ${node.dotPath} is not supported',
      );
    case null:
      throw ParquetFormatException('VARIANT leaf ${node.dotPath} has no type');
  }
}

int _variantInt(Object? value) {
  if (value is int) return value;
  if (value is BigInt) return value.toInt();
  throw ParquetFormatException('VARIANT integer has type ${value.runtimeType}');
}

BigInt _bigIntFromBigEndian(Uint8List bytes) {
  var value = BigInt.zero;
  for (final byte in bytes) {
    value = (value << 8) | BigInt.from(byte);
  }
  if (bytes.isNotEmpty && bytes.first & 0x80 != 0) {
    value -= BigInt.one << (bytes.length * 8);
  }
  return value;
}

/// One schema position, optionally with its declared `repeated` repetition
/// already consumed by an enclosing LIST (backward-compatibility rules 1-3).
final class _Position {
  const _Position(this.node, {this.consumed = false});

  final SchemaNode node;

  /// Whether [node] is a `repeated` node whose repetition is represented by the
  /// enclosing LIST's offsets rather than by its own list column.
  final bool consumed;

  /// Whether this position itself is a repeated field to rebuild.
  bool get repeated => node.isRepeated && !consumed;

  /// Repetition level at which a new occurrence of this position starts.
  int get contextRep => node.repLevel - (repeated ? 1 : 0);
}

/// One decoded leaf plus the (global) level-entry indices that belong to the
/// schema subtree currently being assembled.
final class _LeafStream {
  _LeafStream(this.descriptor, this.decoded, this.entries);

  final ColumnDescriptor descriptor;
  final DecodedColumnChunk decoded;

  /// Entry indices into [decoded], in file order.
  final List<int> entries;

  int defAt(int entry) {
    final defs = decoded.defLevels;
    return defs == null ? descriptor.maxDefLevel : defs[entry];
  }

  int repAt(int entry) {
    final reps = decoded.repLevels;
    return reps == null ? 0 : reps[entry];
  }

  _LeafStream withEntries(List<int> newEntries) =>
      _LeafStream(descriptor, decoded, newEntries);
}

/// Resolved LIST shape (the LIST-annotated group and its element position).
final class _ListShape {
  const _ListShape({
    required this.repeated,
    required this.element,
    required this.consumed,
  });

  /// The single repeated child of the LIST group.
  final SchemaNode repeated;

  /// The element node.
  final SchemaNode element;

  /// Whether [element] is [repeated] itself (two-level encoding).
  final bool consumed;
}

/// Resolved MAP shape.
final class _MapShape {
  const _MapShape({required this.repeated, required this.key, this.value});

  /// The repeated key/value group (or the key-only group for set semantics).
  final SchemaNode repeated;

  /// The key node.
  final SchemaNode key;

  /// The value node, or `null` for a MAP without values (set semantics).
  final SchemaNode? value;
}

/// Assembles one schema subtree from its decoded leaf streams.
final class _Assembler {
  _Assembler(this.schema, this.leaves);

  final ParquetSchema schema;
  final Map<int, DecodedColumnChunk> leaves;
  final Map<int, _LeafStream> _streams = <int, _LeafStream>{};
  final Map<SchemaNode, List<int>> _leafIndexCache = <SchemaNode, List<int>>{};

  /// Full streams (all entries) for every leaf below [node].
  Map<int, _LeafStream> rootStreams(SchemaNode node) {
    final out = <int, _LeafStream>{};
    for (final leafIndex in leafIndices(node)) {
      out[leafIndex] = _stream(leafIndex);
    }
    return out;
  }

  /// Leaf indices below [node], in schema order.
  List<int> leafIndices(SchemaNode node) =>
      _leafIndexCache.putIfAbsent(node, () {
        final out = <int>[];
        void walk(SchemaNode current) {
          if (current.isLeaf) {
            out.add(current.leafIndex!);
            return;
          }
          for (final child in current.children) {
            walk(child);
          }
        }

        walk(node);
        return List<int>.unmodifiable(out);
      });

  _LeafStream _stream(int leafIndex) => _streams.putIfAbsent(leafIndex, () {
    final descriptor = schema.column(leafIndex);
    final decoded = leaves[leafIndex];
    if (decoded == null) {
      throw ParquetFormatException('missing leaf ${descriptor.dotPath}');
    }
    _validateLevels(descriptor, decoded);
    return _LeafStream(
      descriptor,
      decoded,
      List<int>.generate(decoded.numValues, (i) => i, growable: false),
    );
  });

  Map<int, _LeafStream> _restrict(
    Map<int, _LeafStream> streams,
    SchemaNode node,
  ) {
    final out = <int, _LeafStream>{};
    for (final leafIndex in leafIndices(node)) {
      final stream = streams[leafIndex];
      if (stream == null) {
        throw ParquetFormatException(
          'internal error: missing stream for leaf $leafIndex below '
          '${node.dotPath}',
        );
      }
      out[leafIndex] = stream;
    }
    return out;
  }

  void _validateLevels(
    ColumnDescriptor descriptor,
    DecodedColumnChunk decoded,
  ) {
    final numValues = decoded.numValues;
    final defs = decoded.defLevels;
    final reps = decoded.repLevels;
    if (defs != null) {
      if (defs.length != numValues) {
        throw ParquetFormatException(
          'leaf ${descriptor.dotPath} has ${defs.length} definition levels '
          'for $numValues entries',
        );
      }
      for (var i = 0; i < numValues; i++) {
        if (defs[i] > descriptor.maxDefLevel) {
          throw ParquetFormatException(
            'leaf ${descriptor.dotPath} definition level ${defs[i]} exceeds '
            'max ${descriptor.maxDefLevel} at entry $i',
          );
        }
      }
    } else if (descriptor.maxDefLevel > 0 && numValues > 0) {
      throw ParquetFormatException(
        'leaf ${descriptor.dotPath} has no definition levels',
      );
    }
    if (reps != null) {
      if (reps.length != numValues) {
        throw ParquetFormatException(
          'leaf ${descriptor.dotPath} has ${reps.length} repetition levels '
          'for $numValues entries',
        );
      }
      for (var i = 0; i < numValues; i++) {
        if (reps[i] > descriptor.maxRepLevel) {
          throw ParquetFormatException(
            'leaf ${descriptor.dotPath} repetition level ${reps[i]} exceeds '
            'max ${descriptor.maxRepLevel} at entry $i',
          );
        }
      }
      if (numValues > 0 && reps[0] != 0) {
        throw ParquetFormatException(
          'leaf ${descriptor.dotPath} starts with repetition level ${reps[0]} '
          'instead of 0',
        );
      }
    } else if (descriptor.maxRepLevel > 0 && numValues > 0) {
      throw ParquetFormatException(
        'leaf ${descriptor.dotPath} has no repetition levels',
      );
    }
  }

  /// Rebuilds the value of [pos] for [slotCount] occurrences.
  ///
  /// Every stream holds the entries of the leaves below [pos], in file order.
  /// The streams are partitioned at the position's context repetition level to
  /// recover the occurrence slots; nested repetition is recovered recursively.
  ParquetColumn assemble(
    _Position pos,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    if (slotCount < 0) {
      throw ParquetFormatException('negative slot count ($slotCount)');
    }
    if (streams.isEmpty) {
      final node = pos.node;
      final unsupported =
          pos.repeated ||
          _resolveList(node) != null ||
          _resolveMap(node) != null;
      if (unsupported) {
        throw ParquetFormatException(
          'group ${node.dotPath} has no leaf columns to reconstruct',
        );
      }
      // An empty struct carries no data and is always considered present.
      return StructColumn(
        name: node.name,
        length: slotCount,
        fields: const <String, ParquetColumn>{},
        validity: Validity(slotCount),
      );
    }

    if (pos.repeated) return _assembleRepeated(pos, streams, slotCount);

    final node = pos.node;
    if (node.isLeaf) return _assembleLeaf(node, streams, slotCount);

    final listShape = _resolveList(node);
    if (listShape != null) {
      return _assembleList(node, listShape, streams, slotCount);
    }
    final mapShape = _resolveMap(node);
    if (mapShape != null) {
      return _assembleMap(node, mapShape, streams, slotCount);
    }
    return _assembleStruct(node, streams, slotCount);
  }

  ParquetColumn _assembleLeaf(
    SchemaNode node,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    if (streams.length != 1) {
      throw ParquetFormatException(
        'leaf ${node.dotPath} has ${streams.length} leaf streams',
      );
    }
    final stream = streams.values.single;
    if (stream.entries.length != slotCount) {
      throw ParquetFormatException(
        'leaf ${stream.descriptor.dotPath} has ${stream.entries.length} '
        'entries, expected one per slot ($slotCount)',
      );
    }
    final validity = Validity(slotCount);
    final maxDef = stream.descriptor.maxDefLevel;
    for (var slot = 0; slot < slotCount; slot++) {
      if (stream.defAt(stream.entries[slot]) < maxDef) {
        validity.setNull(slot);
      }
    }
    return buildColumnFromCompact(
      stream.descriptor,
      stream.decoded.values,
      validity,
    );
  }

  ParquetColumn _assembleStruct(
    SchemaNode node,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    Validity validity = Validity(slotCount);
    if (node.isOptional) {
      final partitions = <int, List<List<int>>>{
        for (final entry in streams.entries)
          entry.key: _partition(
            entry.value,
            node.repLevel,
            slotCount,
            'struct ${node.dotPath}',
          ),
      };
      validity = _presenceValidity(node, partitions, streams, slotCount);
    }

    final fields = <String, ParquetColumn>{};
    for (final child in node.children) {
      fields[child.name] = assemble(
        _Position(child),
        _restrict(streams, child),
        slotCount,
      );
    }
    return StructColumn(
      name: node.name,
      length: slotCount,
      fields: fields,
      validity: validity,
    );
  }

  ParquetColumn _assembleRepeated(
    _Position pos,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    final node = pos.node;
    final partitions = <int, List<List<int>>>{
      for (final entry in streams.entries)
        entry.key: _partition(
          entry.value,
          pos.contextRep,
          slotCount,
          'repeated ${node.dotPath}',
        ),
    };
    final offsets = Uint32List(slotCount + 1);
    final elementEntries = <int, List<int>>{
      for (final leafIndex in streams.keys) leafIndex: <int>[],
    };
    var total = 0;
    for (var slot = 0; slot < slotCount; slot++) {
      final occurrences = <int, List<List<int>>>{
        for (final entry in streams.entries)
          entry.key: _subdivide(
            entry.value,
            partitions[entry.key]![slot],
            node.repLevel,
          ),
      };
      final count = _checkOccurrenceCounts(occurrences, node);
      for (var occurrence = 0; occurrence < count; occurrence++) {
        final present = _checkOccurrencePresence(
          occurrences,
          streams,
          occurrence,
          node.defLevel,
          node,
        );
        if (present) {
          for (final entry in occurrences.entries) {
            elementEntries[entry.key]!.addAll(entry.value[occurrence]);
          }
          total++;
        }
      }
      offsets[slot + 1] = total;
    }

    final elementStreams = <int, _LeafStream>{
      for (final entry in streams.entries)
        entry.key: entry.value.withEntries(elementEntries[entry.key]!),
    };
    final child = assemble(
      _Position(node, consumed: true),
      elementStreams,
      total,
    );
    return ListColumn(
      name: node.name,
      offsets: offsets,
      child: child,
      validity: Validity(slotCount),
    );
  }

  ParquetColumn _assembleList(
    SchemaNode node,
    _ListShape shape,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    final repeated = shape.repeated;
    final partitions = <int, List<List<int>>>{
      for (final entry in streams.entries)
        entry.key: _partition(
          entry.value,
          node.repLevel,
          slotCount,
          'LIST ${node.dotPath}',
        ),
    };
    final offsets = Uint32List(slotCount + 1);
    final elementEntries = <int, List<int>>{
      for (final leafIndex in streams.keys) leafIndex: <int>[],
    };
    var total = 0;
    for (var slot = 0; slot < slotCount; slot++) {
      final occurrences = <int, List<List<int>>>{
        for (final entry in streams.entries)
          entry.key: _subdivide(
            entry.value,
            partitions[entry.key]![slot],
            repeated.repLevel,
          ),
      };
      final count = _checkOccurrenceCounts(occurrences, repeated);
      for (var occurrence = 0; occurrence < count; occurrence++) {
        final present = _checkOccurrencePresence(
          occurrences,
          streams,
          occurrence,
          repeated.defLevel,
          repeated,
        );
        if (present) {
          for (final entry in occurrences.entries) {
            elementEntries[entry.key]!.addAll(entry.value[occurrence]);
          }
          total++;
        }
      }
      offsets[slot + 1] = total;
    }

    final validity = node.isOptional
        ? _presenceValidity(node, partitions, streams, slotCount)
        : Validity(slotCount);
    final elementStreams = <int, _LeafStream>{
      for (final entry in streams.entries)
        entry.key: entry.value.withEntries(elementEntries[entry.key]!),
    };
    final child = assemble(
      _Position(shape.element, consumed: shape.consumed),
      elementStreams,
      total,
    );
    return ListColumn(
      name: node.name,
      offsets: offsets,
      child: child,
      validity: validity,
    );
  }

  ParquetColumn _assembleMap(
    SchemaNode node,
    _MapShape shape,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    final repeated = shape.repeated;
    final keyLeaves = leafIndices(shape.key);
    final valueLeaves = shape.value == null
        ? const <int>[]
        : leafIndices(shape.value!);
    final known = <int>{...keyLeaves, ...valueLeaves};
    for (final leafIndex in streams.keys) {
      if (!known.contains(leafIndex)) {
        throw ParquetFormatException(
          'MAP ${node.dotPath} has unexpected leaf $leafIndex outside its '
          'key/value subtrees',
        );
      }
    }
    for (final leafIndex in known) {
      if (!streams.containsKey(leafIndex)) {
        throw ParquetFormatException(
          'MAP ${node.dotPath} is missing leaf $leafIndex; a MAP projection '
          'must include every key and value leaf',
        );
      }
    }
    if (keyLeaves.isEmpty) {
      throw ParquetFormatException(
        'MAP ${node.dotPath} has no key leaf column',
      );
    }

    final partitions = <int, List<List<int>>>{
      for (final entry in streams.entries)
        entry.key: _partition(
          entry.value,
          node.repLevel,
          slotCount,
          'MAP ${node.dotPath}',
        ),
    };
    final offsets = Uint32List(slotCount + 1);
    final keyEntries = <int, List<int>>{
      for (final leafIndex in keyLeaves) leafIndex: <int>[],
    };
    final valueEntries = <int, List<int>>{
      for (final leafIndex in valueLeaves) leafIndex: <int>[],
    };
    var total = 0;
    for (var slot = 0; slot < slotCount; slot++) {
      final occurrences = <int, List<List<int>>>{
        for (final entry in streams.entries)
          entry.key: _subdivide(
            entry.value,
            partitions[entry.key]![slot],
            repeated.repLevel,
          ),
      };
      final count = _checkOccurrenceCounts(occurrences, repeated);
      for (var occurrence = 0; occurrence < count; occurrence++) {
        final present = _checkOccurrencePresence(
          occurrences,
          streams,
          occurrence,
          repeated.defLevel,
          repeated,
        );
        if (present) {
          for (final leafIndex in keyLeaves) {
            keyEntries[leafIndex]!.addAll(occurrences[leafIndex]![occurrence]);
          }
          for (final leafIndex in valueLeaves) {
            valueEntries[leafIndex]!.addAll(
              occurrences[leafIndex]![occurrence],
            );
          }
          total++;
        }
      }
      offsets[slot + 1] = total;
    }

    final validity = node.isOptional
        ? _presenceValidity(node, partitions, streams, slotCount)
        : Validity(slotCount);
    final keyStreams = <int, _LeafStream>{
      for (final leafIndex in keyLeaves)
        leafIndex: streams[leafIndex]!.withEntries(keyEntries[leafIndex]!),
    };
    final keys = assemble(_Position(shape.key), keyStreams, total);
    ParquetColumn values;
    if (shape.value == null) {
      // Set semantics: the MAP only stores keys. Expose the missing values as
      // a null column so MapColumn's key/value API stays intact.
      values = NullColumn(name: 'value', length: total);
    } else {
      final valueStreams = <int, _LeafStream>{
        for (final leafIndex in valueLeaves)
          leafIndex: streams[leafIndex]!.withEntries(valueEntries[leafIndex]!),
      };
      values = assemble(_Position(shape.value!), valueStreams, total);
    }
    return MapColumn(
      name: node.name,
      offsets: offsets,
      keys: keys,
      values: values,
      validity: validity,
    );
  }

  /// Splits [stream]'s entries into [expected] occurrence groups. A new group
  /// starts at every entry whose repetition level is <= [thresholdRep].
  List<List<int>> _partition(
    _LeafStream stream,
    int thresholdRep,
    int expected,
    String context,
  ) {
    final entries = stream.entries;
    if (entries.isEmpty) {
      if (expected != 0) {
        throw ParquetFormatException(
          '$context: leaf ${stream.descriptor.dotPath} has no entries for '
          '$expected slot(s)',
        );
      }
      return const <List<int>>[];
    }
    final reps = stream.decoded.repLevels;
    if (reps == null) {
      if (entries.length != expected) {
        throw ParquetFormatException(
          '$context: leaf ${stream.descriptor.dotPath} has ${entries.length} '
          'entries for $expected slot(s)',
        );
      }
      return <List<int>>[
        for (final entry in entries) <int>[entry],
      ];
    }
    if (stream.repAt(entries.first) > thresholdRep) {
      throw ParquetFormatException(
        '$context: leaf ${stream.descriptor.dotPath} starts at repetition '
        'level ${stream.repAt(entries.first)} (expected <= $thresholdRep)',
      );
    }
    final out = <List<int>>[];
    var start = 0;
    for (var i = 1; i < entries.length; i++) {
      if (stream.repAt(entries[i]) <= thresholdRep) {
        out.add(entries.sublist(start, i));
        start = i;
      }
    }
    out.add(entries.sublist(start));
    if (out.length != expected) {
      throw ParquetFormatException(
        '$context: leaf ${stream.descriptor.dotPath} describes ${out.length} '
        'slot(s), expected $expected',
      );
    }
    return out;
  }

  /// Splits one occurrence group into repeated-element groups. Every entry with
  /// repetition level <= [thresholdRep] starts a new element.
  List<List<int>> _subdivide(
    _LeafStream stream,
    List<int> segment,
    int thresholdRep,
  ) {
    if (segment.isEmpty) return const <List<int>>[];
    final reps = stream.decoded.repLevels;
    if (reps == null) {
      throw ParquetFormatException(
        'leaf ${stream.descriptor.dotPath} has repetition level 0 but the '
        'schema requires repetition level $thresholdRep',
      );
    }
    final out = <List<int>>[];
    var start = 0;
    for (var i = 1; i < segment.length; i++) {
      if (stream.repAt(segment[i]) <= thresholdRep) {
        out.add(segment.sublist(start, i));
        start = i;
      }
    }
    out.add(segment.sublist(start));
    return out;
  }

  int _checkOccurrenceCounts(
    Map<int, List<List<int>>> occurrences,
    SchemaNode node,
  ) {
    int? count;
    for (final entry in occurrences.entries) {
      final current = entry.value.length;
      if (count == null) {
        count = current;
      } else if (current != count) {
        throw ParquetFormatException(
          'inconsistent repetition levels under ${node.dotPath}: leaf '
          '${entry.key} describes $current occurrence(s), expected $count',
        );
      }
    }
    return count ?? 0;
  }

  bool _checkOccurrencePresence(
    Map<int, List<List<int>>> occurrences,
    Map<int, _LeafStream> streams,
    int occurrence,
    int defLevel,
    SchemaNode node,
  ) {
    bool? present;
    for (final entry in occurrences.entries) {
      final segment = entry.value[occurrence];
      final current = streams[entry.key]!.defAt(segment.first) >= defLevel;
      if (present == null) {
        present = current;
      } else if (present != current) {
        throw ParquetFormatException(
          'inconsistent definition levels under ${node.dotPath}: leaf '
          '${entry.key} disagrees about occurrence $occurrence',
        );
      }
    }
    return present ?? false;
  }

  /// Builds a validity bitmap from the presence of an optional group in every
  /// slot, validating that all leaves agree.
  Validity _presenceValidity(
    SchemaNode node,
    Map<int, List<List<int>>> partitions,
    Map<int, _LeafStream> streams,
    int slotCount,
  ) {
    final validity = Validity(slotCount);
    if (!node.isOptional) return validity;
    final keys = partitions.keys.toList(growable: false);
    if (keys.isEmpty) return validity;
    final representative = keys.first;
    final presence = List<bool>.filled(slotCount, false);
    for (var slot = 0; slot < slotCount; slot++) {
      final segment = partitions[representative]![slot];
      presence[slot] =
          streams[representative]!.defAt(segment.first) >= node.defLevel;
    }
    for (var k = 1; k < keys.length; k++) {
      final key = keys[k];
      final segments = partitions[key]!;
      for (var slot = 0; slot < slotCount; slot++) {
        final current =
            streams[key]!.defAt(segments[slot].first) >= node.defLevel;
        if (current != presence[slot]) {
          throw ParquetFormatException(
            'inconsistent definition levels for ${node.dotPath}: leaf $key '
            'disagrees about whether slot $slot is null',
          );
        }
      }
    }
    for (var slot = 0; slot < slotCount; slot++) {
      if (!presence[slot]) validity.setNull(slot);
    }
    return validity;
  }
}

/// Resolves a LIST-annotated [node] to its repeated child and element position.
///
/// Implements the `parquet-format` backward-compatibility rules:
/// 1. a repeated leaf is the (required) element;
/// 2. a repeated group with multiple fields is the (required) element;
/// 3. a repeated group with one field named `array`, or named after the outer
///    group with a `_tuple` suffix, is the (required) element;
/// 4. otherwise the repeated group's single child is the element, keeping its
///    own repetition.
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

/// Whether [node] is a MAP group (including legacy `MAP_KEY_VALUE` encoding).
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

/// Resolves a MAP group to its repeated key/value group.
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
