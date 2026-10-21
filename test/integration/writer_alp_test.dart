@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

ParquetTable _table({int rows = 2000}) {
  final schema =
      (ParquetSchemaBuilder()
            ..addFloat64('d')
            ..addFloat32('f'))
          .build();
  final doubles = Float64List(rows);
  final floats = Float32List(rows);
  for (var i = 0; i < rows; i++) {
    doubles[i] = (i - rows / 2) / 7.0;
    floats[i] = i / 3.0;
  }
  return ParquetTable(
    schema: schema,
    columns: [
      Float64Column(name: 'd', values: doubles, validity: Validity(rows)),
      Float32Column(name: 'f', values: floats, validity: Validity(rows)),
    ],
    numRows: rows,
  );
}

void main() {
  for (final version in [1, 2]) {
    for (final codec in [
      CompressionType.uncompressed,
      CompressionType.snappy,
      CompressionType.zstd,
    ]) {
      test('ALP round-trip v$version ${codec.name}', () {
        final input = _table();
        final bytes = writeParquetTable(
          input,
          options: ParquetWriterOptions(
            compression: codec,
            dataPageVersion: version,
            enableAlp: true,
          ),
        );
        final reader = ParquetReader.fromBytes(bytes);
        final header = reader.metadata.rowGroup(0).column(0);
        expect(header.encodings, contains(Encoding.alp));
        final output = reader.readTableSync();
        final d = output['d'] as Float64Column;
        final f = output['f'] as Float32Column;
        for (var i = 0; i < input.numRows; i++) {
          expect(d.values[i], input['d'].value(i));
          expect(f.values[i], input['f'].value(i));
        }
      });
    }
  }

  test('ALP writer defaults off', () {
    final bytes = writeParquetTable(_table(rows: 10));
    final reader = ParquetReader.fromBytes(bytes);
    expect(
      reader.metadata.rowGroup(0).column(0).encodings,
      isNot(contains(Encoding.alp)),
    );
  });
}
