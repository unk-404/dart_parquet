/// Writer matrix harness for bidirectional interop verification.
///
/// Writes flat and nested tables with the public `ParquetWriter` across every
/// codec, both data page versions and dictionary on/off, then emits a manifest
/// of expected values (canonical checksums) for `tool/interop/verify_writer.py`
/// to check with pyarrow, DuckDB and Polars.
///
/// Run from the repository root:
///
///     dart run tool/interop/write_matrix.dart
///     .agents/venv/Scripts/python.exe tool/interop/verify_writer.py
///
/// Output defaults to `.agents/interop/writer_out` (scratch, git-ignored);
/// pass `--out <dir>` to write elsewhere. This script is not part of CI.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';

import 'interop_canonical.dart';

const String sourceFixture =
    'test/fixtures/interop/generated/pyarrow_multi_rowgroup.parquet';

const List<String> flatColumns = <String>[
  'id',
  'name',
  'score',
  'flag',
  'ts',
  'd',
  'dec',
  'fx',
  'bin',
  'hf',
];

const List<String> nestedColumns = <String>['id', 'lst', 'st', 'mp'];

/// Codecs exercised by the matrix. Legacy LZ4 (Hadoop framing) is read-only.
const List<CompressionType> matrixCodecs = <CompressionType>[
  CompressionType.uncompressed,
  CompressionType.snappy,
  CompressionType.gzip,
  CompressionType.brotli,
  CompressionType.zstd,
  CompressionType.lz4Raw,
  CompressionType.lzo,
];

/// One writer configuration.
final class WriterCase {
  const WriterCase({
    required this.shape,
    required this.compression,
    required this.pageVersion,
    required this.dictionary,
  });

  final String shape;
  final CompressionType compression;
  final int pageVersion;
  final bool dictionary;

  String get id =>
      '${shape}__${compression.name}__v$pageVersion'
      '__${dictionary ? 'dict' : 'plain'}';

  String get fileName => '$id.parquet';

  ParquetWriterOptions get options => ParquetWriterOptions(
    compression: compression,
    dataPageVersion: pageVersion,
    useDictionary: dictionary,
    pageSize: 256,
    createdBy: 'parquet-dart interop write_matrix 0.1.2',
  );

  Map<String, Object?> toJson() => <String, Object?>{
    'shape': shape,
    'compression': compression.name,
    'page_version': pageVersion,
    'dictionary': dictionary,
  };
}

/// The complete deterministic matrix (2 shapes x 7 codecs x 2 versions x 2).
List<WriterCase> buildMatrix() {
  final cases = <WriterCase>[];
  for (final shape in <String>['flat', 'nested']) {
    for (final codec in matrixCodecs) {
      for (final version in <int>[1, 2]) {
        for (final dictionary in <bool>[true, false]) {
          cases.add(
            WriterCase(
              shape: shape,
              compression: codec,
              pageVersion: version,
              dictionary: dictionary,
            ),
          );
        }
      }
    }
  }
  return cases;
}

/// Loads the flat or nested source table from the pyarrow-generated fixture.
ParquetTable loadSourceTable(String shape) {
  final reader = ParquetReader.fromBytes(File(sourceFixture).readAsBytesSync());
  return reader.readTableSync(
    columns: shape == 'flat' ? flatColumns : nestedColumns,
  );
}

/// Writes [table] with [options] and returns the bytes.
Uint8List writeWithOptions(ParquetTable table, ParquetWriterOptions options) =>
    writeParquetTable(table, options: options);

/// Expected column manifest (checksums/samples) for a table.
List<Map<String, Object?>> expectedColumns(ParquetTable table) =>
    <Map<String, Object?>>[
      for (final column in table.columns)
        <String, Object?>{
          'name': column.name,
          'dart_class': column.runtimeType.toString(),
          'length': column.length,
          'null_count': column.validity.nullCount,
          'checksum': columnChecksum(column),
          'sample_rows': sampleRows(column.length),
          'samples': sampleValues(column),
        },
    ];

/// Schema description (leaf descriptors) for a table.
List<Map<String, Object?>> schemaLeaves(ParquetTable table) =>
    <Map<String, Object?>>[
      for (final column in table.schema.columns)
        <String, Object?>{
          'path': column.dotPath,
          'physical': column.physicalType.name,
          'type_length': column.typeLength,
          'logical': column.logicalType?.kind.name,
          'max_def': column.maxDefLevel,
          'max_rep': column.maxRepLevel,
          'scale': column.scale,
          'precision': column.precision,
        },
    ];

/// Writes the whole matrix into [outDir] and returns the manifest map.
Map<String, Object?> runMatrix({required String outDir}) {
  final directory = Directory(outDir)..createSync(recursive: true);
  final cases = buildMatrix();
  final tables = <String, ParquetTable>{
    'flat': loadSourceTable('flat'),
    'nested': loadSourceTable('nested'),
  };
  final entries = <Map<String, Object?>>[];
  for (final testCase in cases) {
    final table = tables[testCase.shape]!;
    final bytes = writeWithOptions(table, testCase.options);
    final file = File('${directory.path}/${testCase.fileName}');
    file.writeAsBytesSync(bytes);
    entries.add(<String, Object?>{
      'file': testCase.fileName,
      ...testCase.toJson(),
      'bytes': bytes.length,
      'rows': table.numRows,
      'row_groups': 1,
      'schema': schemaLeaves(table),
      'columns': expectedColumns(table),
    });
  }

  // Multi-row-group writer output: one add() per source row group.
  final sourceReader = ParquetReader.fromBytes(
    File(sourceFixture).readAsBytesSync(),
  );
  final multi = ParquetWriter(
    schema: tables['flat']!.schema,
    options: const ParquetWriterOptions(
      compression: CompressionType.snappy,
      pageSize: 256,
      createdBy: 'parquet-dart interop write_matrix 0.1.2',
    ),
  );
  final flatReader = ParquetReader.fromBytes(
    File(sourceFixture).readAsBytesSync(),
  );
  final flatProjection = flatReader.readTableSync(columns: flatColumns);
  // The generated fixture has four row groups; add each group separately while
  // keeping the projected flat schema.
  for (var i = 0; i < sourceReader.numRowGroups; i++) {
    final group = sourceReader.readRowGroupSync(i, columns: flatColumns);
    multi.add(group);
  }
  final multiBytes = multi.finish();
  File('${directory.path}/multi_rowgroup__snappy__v1__dict.parquet')
      .writeAsBytesSync(multiBytes);
  entries.add(<String, Object?>{
    'file': 'multi_rowgroup__snappy__v1__dict.parquet',
    'shape': 'flat',
    'compression': CompressionType.snappy.name,
    'page_version': 1,
    'dictionary': true,
    'bytes': multiBytes.length,
    'rows': multi.numRows,
    'row_groups': multi.numRowGroups,
    'schema': schemaLeaves(flatProjection),
    'columns': expectedColumns(flatProjection),
  });

  // Empty table (0 rows, 1 row group) through the public writer.
  final empty = ParquetReader.fromBytes(File(sourceFixture).readAsBytesSync())
      .readRowGroupSync(0, columns: flatColumns, maxRows: 0);
  final emptyWriter = ParquetWriter(
    schema: empty.schema,
    options: const ParquetWriterOptions(
      compression: CompressionType.uncompressed,
      pageSize: 256,
      createdBy: 'parquet-dart interop write_matrix 0.1.2',
    ),
  );
  emptyWriter.add(empty);
  final emptyBytes = emptyWriter.finish();
  File('${directory.path}/empty__uncompressed__v1__dict.parquet')
      .writeAsBytesSync(emptyBytes);
  entries.add(<String, Object?>{
    'file': 'empty__uncompressed__v1__dict.parquet',
    'shape': 'flat',
    'compression': CompressionType.uncompressed.name,
    'page_version': 1,
    'dictionary': true,
    'bytes': emptyBytes.length,
    'rows': emptyWriter.numRows,
    'row_groups': emptyWriter.numRowGroups,
    'schema': schemaLeaves(empty),
    'columns': expectedColumns(empty),
  });

  final manifest = <String, Object?>{
    'generator': 'tool/interop/write_matrix.dart',
    'source_fixture': sourceFixture,
    'cases': entries.length,
    'fixtures': entries,
  };
  File('${directory.path}/writer_matrix.json')
      .writeAsStringSync(const JsonEncoder.withIndent(' ').convert(manifest));
  return manifest;
}

Future<void> main(List<String> args) async {
  var outDir = '.agents/interop/writer_out';
  for (var i = 0; i < args.length - 1; i++) {
    if (args[i] == '--out') outDir = args[i + 1];
  }
  final stopwatch = Stopwatch()..start();
  final manifest = runMatrix(outDir: outDir);
  stdout.writeln(
    'wrote ${manifest['cases']} files to $outDir in '
    '${stopwatch.elapsedMilliseconds} ms',
  );
}
