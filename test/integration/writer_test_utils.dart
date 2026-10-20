/// Shared helpers for the writer integration tests.
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

/// This file only holds helpers; the empty entry point keeps `dart test`
/// happy when it is matched by a `writer_*` glob.
void main() {}

/// Reads `test/fixtures/[name]` and decodes every row group.
ParquetTable readFixtureTable(String name) {
  return ParquetReader.fromBytes(File('test/fixtures/$name').readAsBytesSync())
      .readTableSync();
}

/// Reads the reader for `test/fixtures/[name]`.
ParquetReader readFixtureReader(String name) {
  return ParquetReader.fromBytes(File('test/fixtures/$name').readAsBytesSync());
}

/// Writes [table] in memory and returns a reader over the produced bytes.
ParquetReader writeAndOpen(
  ParquetTable table, {
  ParquetWriterOptions options = const ParquetWriterOptions(),
}) {
  final bytes = writeParquetTable(table, options: options);
  return ParquetReader.fromBytes(bytes);
}

/// Returns a human-readable list of differences between [expected] and
/// [actual]; an empty list means the tables are value-identical.
List<String> diffTables(ParquetTable expected, ParquetTable actual) {
  final problems = <String>[];
  if (expected.numRows != actual.numRows) {
    problems.add('numRows ${expected.numRows} != ${actual.numRows}');
    return problems;
  }
  if (expected.columns.length != actual.columns.length) {
    problems.add(
      'columns ${expected.columns.length} != '
      '${actual.columns.length}',
    );
    return problems;
  }
  for (var c = 0; c < expected.columns.length; c++) {
    final a = expected.columns[c];
    final b = actual.columns[c];
    if (a.name != b.name) problems.add('column $c name ${a.name} != ${b.name}');
    if (a.length != b.length) {
      problems.add('${a.name} length ${a.length} != ${b.length}');
      continue;
    }
    for (var i = 0; i < a.length; i++) {
      final x = a.value(i);
      final y = b.value(i);
      if (!deepEquals(x, y)) {
        problems.add('${a.name}[$i]: ${_describe(x)} != ${_describe(y)}');
        if (problems.length >= 10) return problems;
      }
    }
  }
  return problems;
}

/// Asserts that [actual] contains exactly the values of [expected].
void expectTablesEqual(ParquetTable expected, ParquetTable actual) {
  expect(diffTables(expected, actual), isEmpty);
}

/// Returns differences between two schema trees (empty means identical).
List<String> diffSchemas(ParquetSchema expected, ParquetSchema actual) {
  final problems = <String>[];
  void compareNode(SchemaNode? a, SchemaNode? b, String path) {
    if (a == null || b == null) {
      problems.add('$path: missing node');
      return;
    }
    if (a.name != b.name) problems.add('$path: name ${a.name} != ${b.name}');
    if (a.repetition != b.repetition) {
      problems.add('$path: repetition ${a.repetition} != ${b.repetition}');
    }
    if (a.physicalType != b.physicalType) {
      problems.add('$path: type ${a.physicalType} != ${b.physicalType}');
    }
    if (a.typeLength != b.typeLength) {
      problems.add('$path: typeLength ${a.typeLength} != ${b.typeLength}');
    }
    if (a.defLevel != b.defLevel) {
      problems.add('$path: defLevel ${a.defLevel} != ${b.defLevel}');
    }
    if (a.repLevel != b.repLevel) {
      problems.add('$path: repLevel ${a.repLevel} != ${b.repLevel}');
    }
    if (a.convertedType != b.convertedType) {
      problems.add('$path: converted ${a.convertedType} != ${b.convertedType}');
    }
    if (a.logicalType.runtimeType != b.logicalType.runtimeType ||
        a.logicalType != b.logicalType) {
      problems.add('$path: logical ${a.logicalType} != ${b.logicalType}');
    }
    if (a.scale != b.scale || a.precision != b.precision) {
      problems.add(
        '$path: scale/precision ${a.scale}/${a.precision} != '
        '${b.scale}/${b.precision}',
      );
    }
    if (a.children.length != b.children.length) {
      problems.add(
        '$path: children ${a.children.length} != '
        '${b.children.length}',
      );
      return;
    }
    for (var i = 0; i < a.children.length; i++) {
      compareNode(a.children[i], b.children[i], '$path.${a.children[i].name}');
    }
  }

  compareNode(expected.root, actual.root, expected.root.name);
  return problems;
}

/// Asserts that [actual] preserves every schema detail of [expected].
void expectSchemasEqual(ParquetSchema expected, ParquetSchema actual) {
  expect(diffSchemas(expected, actual), isEmpty);
}

/// Deep equality for reader values (primitives, lists, maps and byte arrays).
bool deepEquals(Object? a, Object? b) {
  if (identical(a, b)) return true;
  if (a is Uint8List && b is Uint8List) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
  if (a is List && b is List) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (!deepEquals(a[i], b[i])) return false;
    }
    return true;
  }
  if (a is Map && b is Map) {
    if (a.length != b.length) return false;
    for (final key in a.keys) {
      if (!b.containsKey(key)) return false;
      if (!deepEquals(a[key], b[key])) return false;
    }
    return true;
  }
  if (a is double && b is double) {
    if (a.isNaN && b.isNaN) return true;
    return a == b;
  }
  return a == b;
}

String _describe(Object? value) {
  if (value is Uint8List) {
    return 'bytes(${value.length})${value.length <= 16 ? value.toList() : ''}';
  }
  return value.toString();
}

/// Builds a zero-row table with columns matching [schema]'s top-level shape.
ParquetTable emptyTableFor(ParquetSchema schema) {
  final columns = <ParquetColumn>[];
  for (final node in schema.root.children) {
    columns.add(emptyColumnFor(node, schema));
  }
  return ParquetTable(schema: schema, columns: columns, numRows: 0);
}

/// Builds a zero-length column matching [node] (which must be a top-level
/// leaf in these tests).
ParquetColumn emptyColumnFor(SchemaNode node, ParquetSchema schema) {
  final name = node.name;
  final validity = Validity(0);
  switch (node.physicalType) {
    case PhysicalType.int32:
      return Int32Column(name: name, values: Int32List(0), validity: validity);
    case PhysicalType.int64:
      return Int64Column(
        name: name,
        values: Int64Buffer(0),
        validity: validity,
      );
    case PhysicalType.float:
      return Float32Column(
        name: name,
        values: Float32List(0),
        validity: validity,
      );
    case PhysicalType.double:
      return Float64Column(
        name: name,
        values: Float64List(0),
        validity: validity,
      );
    case PhysicalType.boolean:
      return BooleanColumn(name: name, bits: Uint8List(0), validity: validity);
    case PhysicalType.byteArray:
      return BinaryColumn(
        name: name,
        data: Uint8List(0),
        offsets: Uint32List(1),
        validity: validity,
        isUtf8: node.logicalType?.kind == LogicalTypeKind.string,
      );
    case PhysicalType.int96:
      return Int96Column(name: name, data: Uint8List(0), validity: validity);
    case PhysicalType.fixedLenByteArray:
      return FixedLenColumn(
        name: name,
        data: Uint8List(0),
        width: node.typeLength ?? 1,
        validity: validity,
      );
    case null:
      throw UnsupportedError('emptyColumnFor only supports leaves');
  }
}
