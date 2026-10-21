@TestOn('vm')
library;

// Codec-only overhead benchmark.
//
// For each fixture column this measures:
//   * full Parquet read   — ParquetReader.fromBytes(...).readTableSync(column)
//   * codec-only decode   — codec.decode() over that column's compressed pages
//   * full Parquet write  — writeParquetTable() for the same projected table
//   * codec-only encode   — codec.encode() over the same pages' raw value bytes
//
// The ratios show how much of a full read/write is NOT codec work: page-header
// parsing, level/value decoding, column assembly, footer writing, allocations.
//
// Run with:
//   dart test benchmark/overhead_test.dart
//
// Uses the existing PLAIN snappy fixture for int32/int64/double/string and the
// dictionary fixture for a dictionary-encoded string. The repository has no
// FLOAT fixture, so a small uncompressed-source FLOAT table is written once in
// memory here (same writer, PLAIN, snappy) and measured like a fixture.

import 'dart:io';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/compression/codec.dart';
import 'package:parquet/src/compression/registry.dart';
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:test/test.dart';

/// Prevents the VM from eliminating measured calls.
int _sink = 0;

/// One compressed page extracted from a fixture column chunk.
final class _CompressedPage {
  _CompressedPage(this.compressed, this.raw, this.uncompressedSize);

  /// Bytes handed to the decoder (v1: whole body; v2: value bytes only).
  final Bytes compressed;

  /// Bytes the writer would hand to the encoder for this page.
  final Bytes raw;

  final int uncompressedSize;
}

/// One measured column / encoding case.
final class _Case {
  _Case({
    required this.name,
    required this.fixture,
    required this.column,
    required this.plain,
    required this.bytes,
    required this.table,
    required this.pages,
    required this.codec,
    required this.rows,
    required this.compressedBytes,
    required this.uncompressedBytes,
  });

  final String name;
  final String fixture;
  final String column;
  final bool plain;
  final Uint8List bytes;
  final ParquetTable table;
  final List<_CompressedPage> pages;
  final CompressionCodec codec;
  final int rows;
  final int compressedBytes;
  final int uncompressedBytes;

  ParquetWriterOptions get writeOptions => ParquetWriterOptions(
    compression: codec.compressionType,
    useDictionary: !plain,
    dataPageVersion: 1,
    pageSize: 1 << 20,
  );
}

extension on CompressionCodec {
  CompressionType get compressionType {
    for (final type in CompressionType.values) {
      if (type.value == codecId) return type;
    }
    throw StateError('unknown codec id $codecId');
  }
}

/// Extracts the compressed pages of [column] from [fileBytes] and decodes each
/// one once so the raw encoder input is available too.
List<_CompressedPage> _pagesFor(
  Uint8List fileBytes,
  ParquetMetadata metadata,
  String column,
) {
  final file = Bytes.fromUint8List(fileBytes);
  final pages = <_CompressedPage>[];
  var codec = codecFor(CompressionType.uncompressed);

  for (var groupIndex = 0; groupIndex < metadata.numRowGroups; groupIndex++) {
    final group = metadata.rowGroup(groupIndex);
    final chunk = group.columns.firstWhere(
      (candidate) => candidate.path.length == 1 && candidate.path[0] == column,
    );
    codec = codecFor(chunk.compression);
    final start = chunk.dictionaryPageOffset ?? chunk.dataPageOffset;
    if (start == null || chunk.totalCompressedSize <= 0) continue;
    final chunkBytes = file.slice(start, start + chunk.totalCompressedSize);
    final reader = ByteReader(chunkBytes);
    while (reader.remaining > 0) {
      final header = readPageHeader(reader);
      final body = reader.readSlice(header.compressedPageSize);
      switch (header.type) {
        case PageType.dictionaryPage:
        case PageType.dataPage:
          final raw = codec.decode(
            body,
            uncompressedSize: header.uncompressedPageSize,
          );
          pages.add(_CompressedPage(body, raw, header.uncompressedPageSize));
        case PageType.dataPageV2:
          final data = header.dataPageHeaderV2!;
          final levels =
              data.repetitionLevelsByteLength + data.definitionLevelsByteLength;
          final valueBody = body.slice(levels);
          final valueSize = header.uncompressedPageSize - levels;
          if (data.isCompressed && valueBody.isNotEmpty && valueSize > 0) {
            final raw = codec.decode(valueBody, uncompressedSize: valueSize);
            pages.add(_CompressedPage(valueBody, raw, valueSize));
          }
        case PageType.indexPage:
          break;
      }
    }
  }
  return pages;
}

ParquetMetadata _metadata(Uint8List bytes) =>
    ParquetReader.fromBytes(bytes).metadata;

_Case _buildCase({
  required String name,
  required String fixturePath,
  required String column,
  required bool plain,
  Uint8List? bytesOverride,
}) {
  final bytes = bytesOverride ?? File(fixturePath).readAsBytesSync();
  final reader = ParquetReader.fromBytes(bytes);
  final table = reader.readTableSync(columns: [column]);
  final metadata = reader.metadata;

  var compressedBytes = 0;
  var uncompressedBytes = 0;
  for (var groupIndex = 0; groupIndex < metadata.numRowGroups; groupIndex++) {
    final group = metadata.rowGroup(groupIndex);
    for (final chunk in group.columns) {
      if (chunk.path.length == 1 && chunk.path[0] == column) {
        compressedBytes += chunk.totalCompressedSize;
        uncompressedBytes += chunk.totalUncompressedSize;
      }
    }
  }

  final codes = <CompressionType>{
    for (var groupIndex = 0; groupIndex < metadata.numRowGroups; groupIndex++)
      for (final chunk in metadata.rowGroup(groupIndex).columns)
        if (chunk.path.length == 1 && chunk.path[0] == column)
          chunk.compression,
  };
  if (codes.length != 1) {
    throw StateError('$name uses mixed codecs: $codes');
  }

  return _Case(
    name: name,
    fixture: bytesOverride == null ? fixturePath : '<generated FLOAT>',
    column: column,
    plain: plain,
    bytes: bytes,
    table: table,
    pages: _pagesFor(bytes, metadata, column),
    codec: codecFor(codes.single),
    rows: table.numRows,
    compressedBytes: compressedBytes,
    uncompressedBytes: uncompressedBytes,
  );
}

/// Writes a single FLOAT column (no fixture exists in the repo).
Uint8List _floatFixtureBytes() {
  const rowCount = 25000;
  final values = Float32List(rowCount);
  for (var i = 0; i < rowCount; i++) {
    values[i] = i * 0.25 - 1000.0;
  }
  final schema = (ParquetSchemaBuilder()..addFloat32('f32', nullable: false))
      .build();
  final table = ParquetTable(
    schema: schema,
    columns: [
      Float32Column(name: 'f32', values: values, validity: Validity(rowCount)),
    ],
    numRows: rowCount,
  );
  return writeParquetTable(
    table,
    options: const ParquetWriterOptions(
      compression: CompressionType.snappy,
      useDictionary: false,
      pageSize: 1 << 20,
    ),
  );
}

/// Median microseconds per operation, with adaptive iteration counts.
double _measureMicros(
  void Function() body, {
  Duration targetSample = const Duration(milliseconds: 12),
  int samples = 7,
}) {
  var iterations = 1;
  while (true) {
    final stopwatch = Stopwatch()..start();
    for (var i = 0; i < iterations; i++) {
      body();
    }
    stopwatch.stop();
    if (stopwatch.elapsedMicroseconds >= targetSample.inMicroseconds ||
        iterations >= 1 << 20) {
      break;
    }
    iterations = math.max(1, iterations * 2);
  }

  final timings = <int>[];
  for (var sample = 0; sample < samples; sample++) {
    final stopwatch = Stopwatch()..start();
    for (var i = 0; i < iterations; i++) {
      body();
    }
    stopwatch.stop();
    timings.add(stopwatch.elapsedMicroseconds);
  }
  timings.sort();
  return timings[timings.length ~/ 2] / iterations;
}

String _ratio(double full, double codecOnly) {
  if (codecOnly <= 0) return 'n/a';
  return '${(full / codecOnly).toStringAsFixed(2)}x';
}

void _printTable(
  List<_Case> cases,
  Map<String, double> fullRead,
  Map<String, double> codecDecode,
  Map<String, double> fullWrite,
  Map<String, double> codecEncode,
) {
  const nameWidth = 14;
  String cell(Object value, int width) => value.toString().padLeft(width);

  final header = StringBuffer()
    ..write('case'.padRight(nameWidth))
    ..write(cell('rows', 8))
    ..write(cell('pages', 7))
    ..write(cell('comp MB', 10))
    ..write(cell('uncomp MB', 11))
    ..write(cell('read us', 11))
    ..write(cell('codec us', 11))
    ..write(cell('read x', 9))
    ..write(cell('write us', 11))
    ..write(cell('codec us', 11))
    ..write(cell('write x', 9));
  print(header);
  print('-' * header.length);
  for (final c in cases) {
    final line = StringBuffer()
      ..write(c.name.padRight(nameWidth))
      ..write(cell(c.rows, 8))
      ..write(cell(c.pages.length, 7))
      ..write(cell((c.compressedBytes / 1e6).toStringAsFixed(2), 10))
      ..write(cell((c.uncompressedBytes / 1e6).toStringAsFixed(2), 11))
      ..write(cell(fullRead[c.name]!.toStringAsFixed(1), 11))
      ..write(cell(codecDecode[c.name]!.toStringAsFixed(1), 11))
      ..write(cell(_ratio(fullRead[c.name]!, codecDecode[c.name]!), 9))
      ..write(cell(fullWrite[c.name]!.toStringAsFixed(1), 11))
      ..write(cell(codecEncode[c.name]!.toStringAsFixed(1), 11))
      ..write(cell(_ratio(fullWrite[c.name]!, codecEncode[c.name]!), 9));
    print(line);
  }
}

void main() {
  test(
    'codec-only vs full read/write overhead',
    timeout: const Timeout(Duration(minutes: 5)),
    () {
      final floatBytes = _floatFixtureBytes();
      final cases = <_Case>[
        _buildCase(
          name: 'plain.int32',
          fixturePath: 'test/fixtures/plain_snappy.parquet',
          column: 'nullable_i32',
          plain: true,
        ),
        _buildCase(
          name: 'plain.int64',
          fixturePath: 'test/fixtures/plain_snappy.parquet',
          column: 'i64',
          plain: true,
        ),
        _buildCase(
          name: 'plain.float',
          fixturePath: '<generated>',
          column: 'f32',
          plain: true,
          bytesOverride: floatBytes,
        ),
        _buildCase(
          name: 'plain.double',
          fixturePath: 'test/fixtures/plain_snappy.parquet',
          column: 'f64',
          plain: true,
        ),
        _buildCase(
          name: 'plain.string',
          fixturePath: 'test/fixtures/plain_snappy.parquet',
          column: 'str',
          plain: true,
        ),
        _buildCase(
          name: 'dict.string',
          fixturePath: 'test/fixtures/dict_snappy.parquet',
          column: 'str',
          plain: false,
        ),
      ];

      print('Codec-only overhead benchmark (Windows VM, single-threaded)');
      print(
        'Codec-only = codec decode/encode over the fixture page bodies only; '
        'full = complete Parquet read/write.',
      );
      print(
        'ratio = full / codec-only (>1 means non-codec overhead dominates).',
      );
      print('');

      final fullRead = <String, double>{};
      final codecDecode = <String, double>{};
      final fullWrite = <String, double>{};
      final codecEncode = <String, double>{};

      // Load projection/table/codec classes before the first measured case so
      // class initialization does not inflate the first row.
      for (final c in cases) {
        for (var i = 0; i < 3; i++) {
          ParquetReader.fromBytes(c.bytes).readTableSync(columns: [c.column]);
          writeParquetTable(c.table, options: c.writeOptions);
          for (final page in c.pages) {
            c.codec.decode(
              page.compressed,
              uncompressedSize: page.uncompressedSize,
            );
            c.codec.encode(page.raw);
          }
        }
      }

      for (final c in cases) {
        fullRead[c.name] = _measureMicros(() {
          final table = ParquetReader.fromBytes(c.bytes)
              .readTableSync(columns: [c.column]);
          _sink ^= table.numRows;
        });

        codecDecode[c.name] = _measureMicros(() {
          for (final page in c.pages) {
            final decoded = c.codec.decode(
              page.compressed,
              uncompressedSize: page.uncompressedSize,
            );
            _sink ^= decoded.length;
          }
        });

        fullWrite[c.name] = _measureMicros(() {
          final bytes = writeParquetTable(c.table, options: c.writeOptions);
          _sink ^= bytes.length;
        });

        codecEncode[c.name] = _measureMicros(() {
          for (final page in c.pages) {
            final encoded = c.codec.encode(page.raw);
            _sink ^= encoded.length;
          }
        });

        // Report the encoding the writer actually chose for dictionary cases.
        if (!c.plain) {
          final written = writeParquetTable(c.table, options: c.writeOptions);
          final metadata = _metadata(written);
          final encodings = <String>{};
          for (final chunk in metadata.rowGroup(0).columns) {
            encodings.addAll(chunk.encodings.map((e) => e.name));
          }
          print(
            'note: ${c.name} writes with encodings $encodings '
            '(fixture pages are dictionary-encoded)',
          );
        }
      }

      _printTable(cases, fullRead, codecDecode, fullWrite, codecEncode);
      print('');
      print('codec ids/codecs:');
      for (final c in cases) {
        print(
          '  ${c.name.padRight(14)} ${c.codec.runtimeType} '
          '${c.plain ? 'PLAIN' : 'DICTIONARY'} '
          'pages=${c.pages.length} fixture=${c.fixture}',
        );
      }
      expect(_sink, isNonNegative);
    },
  );
}
