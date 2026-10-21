@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/column/page_reader.dart';
import 'package:parquet/src/encodings/plain.dart'
    show ByteArrayData, FixedLenData;
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

const int _rows = 3000;
const int _pageSize = 512;

int _i32(int i) => i * 7 - 500;
int _i64(int i) => i * 1000003 - 9000000000;
double _f32(int i) => i * 0.25 - 100;
double _f64(int i) => i * 1.5 + 0.125;
String _str(int i) => 'row-$i-${'abc' * (i % 5)}';
String _dictValue(int i) => 'v${i % 25}';

int _nullEvery(int i, int step) => i % step == 0 ? 1 : 0;

Uint8List _write(
  ParquetTable table, {
  int version = 1,
  bool useDictionary = false,
}) => writeParquetTable(
  table,
  options: ParquetWriterOptions(
    compression: CompressionType.snappy,
    useDictionary: useDictionary,
    pageSize: _pageSize,
    dataPageVersion: version,
  ),
);

Validity _validity({int step = 0}) {
  final validity = Validity(_rows);
  if (step > 0) {
    for (var i = 0; i < _rows; i++) {
      if (_nullEvery(i, step) != 0) validity.setNull(i);
    }
  }
  return validity;
}

ParquetTable _numericTable({required bool nullable}) {
  final schema =
      (ParquetSchemaBuilder()
            ..addInt32('i32', nullable: nullable)
            ..addInt64('i64', nullable: nullable)
            ..addFloat32('f32', nullable: nullable)
            ..addFloat64('f64', nullable: nullable))
          .build();
  final i32 = Int32List(_rows);
  final i64 = Int64Buffer(_rows);
  final f32 = Float32List(_rows);
  final f64 = Float64List(_rows);
  for (var i = 0; i < _rows; i++) {
    i32[i] = _i32(i);
    i64.setInt(i, _i64(i));
    f32[i] = _f32(i);
    f64[i] = _f64(i);
  }
  return ParquetTable(
    schema: schema,
    numRows: _rows,
    columns: [
      Int32Column(
        name: 'i32',
        values: i32,
        validity: _validity(step: nullable ? 4 : 0),
      ),
      Int64Column(
        name: 'i64',
        values: i64,
        validity: _validity(step: nullable ? 5 : 0),
      ),
      Float32Column(
        name: 'f32',
        values: f32,
        validity: _validity(step: nullable ? 6 : 0),
      ),
      Float64Column(
        name: 'f64',
        values: f64,
        validity: _validity(step: nullable ? 7 : 0),
      ),
    ],
  );
}

ParquetTable _stringTable({required bool nullable, required bool dictionary}) {
  final schema =
      (ParquetSchemaBuilder()..addBinary('s', nullable: nullable, utf8: true))
          .build();
  final data = <int>[];
  final offsets = Uint32List(_rows + 1);
  for (var i = 0; i < _rows; i++) {
    final value = dictionary ? _dictValue(i) : _str(i);
    data.addAll(value.codeUnits);
    offsets[i + 1] = data.length;
  }
  return ParquetTable(
    schema: schema,
    numRows: _rows,
    columns: [
      BinaryColumn(
        name: 's',
        data: Uint8List.fromList(data),
        offsets: offsets,
        validity: _validity(step: nullable ? 5 : 0),
        isUtf8: true,
      ),
    ],
  );
}

ParquetTable _fixedTable({required bool nullable}) {
  final schema =
      (ParquetSchemaBuilder()
            ..addFixedLenByteArray('fx', 16, nullable: nullable))
          .build();
  final data = Uint8List(_rows * 16);
  for (var i = 0; i < data.length; i++) {
    data[i] = (i * 31 + i ~/ 7) & 0xff;
  }
  return ParquetTable(
    schema: schema,
    numRows: _rows,
    columns: [
      FixedLenColumn(
        name: 'fx',
        data: data,
        width: 16,
        validity: _validity(step: nullable ? 9 : 0),
      ),
    ],
  );
}

void _expectNumeric(ParquetTable table, {required bool nullable}) {
  for (var i = 0; i < _rows; i++) {
    final nullI32 = nullable && _nullEvery(i, 4) != 0;
    final nullI64 = nullable && _nullEvery(i, 5) != 0;
    final nullF32 = nullable && _nullEvery(i, 6) != 0;
    final nullF64 = nullable && _nullEvery(i, 7) != 0;
    expect(
      table['i32'].value(i),
      nullI32 ? isNull : _i32(i),
      reason: 'i32[$i]',
    );
    expect(
      table['i64'].value(i),
      nullI64 ? isNull : _i64(i),
      reason: 'i64[$i]',
    );
    expect(
      table['f32'].value(i),
      nullF32 ? isNull : closeTo(_f32(i), 1e-6),
      reason: 'f32[$i]',
    );
    expect(
      table['f64'].value(i),
      nullF64 ? isNull : _f64(i),
      reason: 'f64[$i]',
    );
  }
}

void _expectStrings(
  ParquetTable table, {
  required bool nullable,
  required bool dictionary,
}) {
  for (var i = 0; i < _rows; i++) {
    final isMissing = nullable && _nullEvery(i, 5) != 0;
    final expected = dictionary ? _dictValue(i) : _str(i);
    expect(table['s'].value(i), isMissing ? isNull : expected, reason: 's[$i]');
  }
}

void _expectFixed(ParquetTable table, {required bool nullable}) {
  final source = _fixedTable(nullable: nullable);
  final expected = (source['fx'] as FixedLenColumn).data;
  for (var i = 0; i < _rows; i++) {
    final isMissing = nullable && _nullEvery(i, 9) != 0;
    if (isMissing) {
      expect(table['fx'].value(i), isNull);
    } else {
      expect(
        table['fx'].value(i),
        Uint8List.sublistView(expected, i * 16, (i + 1) * 16),
      );
    }
  }
}

List<int> _dataPageCounts(Uint8List bytes) {
  final reader = ParquetReader.fromBytes(bytes);
  final file = Bytes.fromUint8List(bytes);
  final counts = <int>[];
  for (var g = 0; g < reader.metadata.numRowGroups; g++) {
    final group = reader.metadata.rowGroup(g);
    for (final chunk in group.columns) {
      final start = chunk.dictionaryPageOffset ?? chunk.dataPageOffset;
      final chunkBytes = file.slice(start!, start + chunk.totalCompressedSize);
      final pageReader = ByteReader(chunkBytes);
      var pages = 0;
      while (pageReader.remaining > 0) {
        final header = readPageHeader(pageReader);
        pageReader.skip(header.compressedPageSize);
        if (header.type == PageType.dataPage ||
            header.type == PageType.dataPageV2) {
          pages++;
        }
      }
      counts.add(pages);
    }
  }
  return counts;
}

({ColumnChunkMetaData meta, Bytes chunk}) _chunk(
  Uint8List bytes,
  String column,
) {
  final reader = ParquetReader.fromBytes(bytes);
  final descriptor = reader.schema.columnByName(column)!;
  final group = reader.metadata.rowGroup(0);
  final meta = group.columns.firstWhere(
    (candidate) => candidate.path.length == 1 && candidate.path[0] == column,
  );
  final start = meta.dictionaryPageOffset ?? meta.dataPageOffset;
  final chunk = Bytes.fromUint8List(bytes)
      .slice(start!, start + meta.totalCompressedSize);
  expect(descriptor.name, column);
  return (meta: meta, chunk: chunk);
}

void main() {
  group('multi-page flat values', () {
    for (final version in const [1, 2]) {
      test('required numeric v$version', () {
        final bytes = _write(_numericTable(nullable: false), version: version);
        expect(
          _dataPageCounts(bytes).every((count) => count > 1),
          isTrue,
          reason: 'page counts: ${_dataPageCounts(bytes)}',
        );
        _expectNumeric(
          ParquetReader.fromBytes(bytes).readTableSync(),
          nullable: false,
        );
      });

      test('nullable numeric v$version', () {
        final bytes = _write(_numericTable(nullable: true), version: version);
        expect(_dataPageCounts(bytes).every((count) => count > 1), isTrue);
        _expectNumeric(
          ParquetReader.fromBytes(bytes).readTableSync(),
          nullable: true,
        );
      });

      test('nullable string v$version', () {
        final bytes = _write(
          _stringTable(nullable: true, dictionary: false),
          version: version,
        );
        expect(_dataPageCounts(bytes).every((count) => count > 1), isTrue);
        _expectStrings(
          ParquetReader.fromBytes(bytes).readTableSync(),
          nullable: true,
          dictionary: false,
        );
      });

      test('nullable dictionary string v$version', () {
        final bytes = _write(
          _stringTable(nullable: true, dictionary: true),
          version: version,
          useDictionary: true,
        );
        final counts = _dataPageCounts(bytes);
        expect(counts.every((count) => count > 1), isTrue, reason: '$counts');
        final table = ParquetReader.fromBytes(bytes).readTableSync();
        _expectStrings(table, nullable: true, dictionary: true);
        final chunk = _chunk(bytes, 's');
        expect(
          chunk.meta.encodings,
          contains(Encoding.rleDictionary),
          reason:
              'writer should choose a dictionary for low-cardinality strings',
        );
      });
    }

    test('required fixed length v1', () {
      final bytes = _write(_fixedTable(nullable: false));
      expect(_dataPageCounts(bytes).every((count) => count > 1), isTrue);
      _expectFixed(
        ParquetReader.fromBytes(bytes).readTableSync(),
        nullable: false,
      );
    });

    test('nullable fixed length v2', () {
      final bytes = _write(_fixedTable(nullable: true), version: 2);
      expect(_dataPageCounts(bytes).every((count) => count > 1), isTrue);
      _expectFixed(
        ParquetReader.fromBytes(bytes).readTableSync(),
        nullable: true,
      );
    });
  });

  group('multi-page chunk decode', () {
    test('nullable string preallocates exact present values', () {
      final bytes = _write(_stringTable(nullable: true, dictionary: false));
      final chunk = _chunk(bytes, 's');
      final descriptor = ParquetReader.fromBytes(bytes).schema
          .columnByName('s')!;
      final decoded = ColumnChunkReader(
        chunk.meta,
        descriptor,
      ).decode(chunk.chunk);
      final expectedPresent = _rows - (_rows / 5).ceil();

      expect(decoded.numValues, _rows);
      expect(decoded.presentCount, expectedPresent);
      expect(decoded.defLevels!.length, _rows);
      expect(decoded.values, isA<ByteArrayData>());
      final binary = decoded.values as ByteArrayData;
      expect(binary.numValues, expectedPresent);
      var value = 0;
      for (var i = 0; i < _rows; i++) {
        if (_nullEvery(i, 5) != 0) continue;
        expect(
          String.fromCharCodes(
            binary.data.sublist(
              binary.offsets[value],
              binary.offsets[value + 1],
            ),
          ),
          _str(i),
        );
        value++;
      }
    });

    test('dictionary string materialises directly into one buffer', () {
      final bytes = _write(
        _stringTable(nullable: true, dictionary: true),
        useDictionary: true,
      );
      final chunk = _chunk(bytes, 's');
      final descriptor = ParquetReader.fromBytes(bytes).schema
          .columnByName('s')!;
      final decoded = ColumnChunkReader(
        chunk.meta,
        descriptor,
      ).decode(chunk.chunk);
      final expectedPresent = _rows - (_rows / 5).ceil();
      expect(decoded.presentCount, expectedPresent);
      expect(decoded.values, isA<ByteArrayData>());
      final binary = decoded.values as ByteArrayData;
      expect(binary.numValues, expectedPresent);
      var value = 0;
      for (var i = 0; i < _rows; i++) {
        if (_nullEvery(i, 5) != 0) continue;
        expect(
          String.fromCharCodes(
            binary.data.sublist(
              binary.offsets[value],
              binary.offsets[value + 1],
            ),
          ),
          _dictValue(i),
        );
        value++;
      }
    });

    test('fixed length values stay exact', () {
      final bytes = _write(_fixedTable(nullable: true), version: 2);
      final chunk = _chunk(bytes, 'fx');
      final descriptor = ParquetReader.fromBytes(bytes).schema
          .columnByName('fx')!;
      final decoded = ColumnChunkReader(
        chunk.meta,
        descriptor,
      ).decode(chunk.chunk);
      expect(decoded.values, isA<FixedLenData>());
      final fixed = decoded.values as FixedLenData;
      expect(fixed.width, 16);
      expect(fixed.numValues, decoded.presentCount);
      expect(fixed.data.length, decoded.presentCount * 16);
    });
  });

  test('multiple row groups stay independent', () {
    final table = _stringTable(nullable: true, dictionary: true);
    final writer = ParquetWriter(
      schema: table.schema,
      options: const ParquetWriterOptions(
        compression: CompressionType.snappy,
        useDictionary: true,
        pageSize: _pageSize,
      ),
    );
    writer.add(table);
    writer.add(table);
    final bytes = writer.finish();
    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.metadata.numRowGroups, 2);
    final result = reader.readTableSync();
    expect(result.numRows, _rows * 2);
    for (var i = 0; i < _rows; i++) {
      expect(result['s'].value(i), result['s'].value(i + _rows));
    }
  });
}
