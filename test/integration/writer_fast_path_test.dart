@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

void main() {
  ParquetSchema flatSchema() =>
      (ParquetSchemaBuilder()
            ..addInt64('id')
            ..addFloat64('score')
            ..addBinary('name', utf8: true)
            ..addBoolean('flag'))
          .build();

  ParquetTable makeTable(int rows) {
    final schema = flatSchema();
    final id = Int64Buffer(rows);
    final score = Float64List(rows);
    final flags = Uint8List((rows + 7) >> 3);
    final data = <int>[];
    final offsets = Uint32List(rows + 1);
    for (var i = 0; i < rows; i++) {
      id.setInt(i, i * 3 - 7);
      score[i] = i / 3;
      if (i.isEven) flags[i >> 3] |= 1 << (i & 7);
      final value = 'value-${i % 1000}';
      for (final byte in value.codeUnits) {
        data.add(byte);
      }
      offsets[i + 1] = data.length;
    }
    final table = ParquetTable(
      schema: schema,
      columns: [
        Int64Column(name: 'id', values: id, validity: Validity(rows)),
        Float64Column(name: 'score', values: score, validity: Validity(rows)),
        BinaryColumn(
          name: 'name',
          data: Uint8List.fromList(data),
          offsets: offsets,
          validity: Validity(rows),
          isUtf8: true,
        ),
        BooleanColumn(name: 'flag', bits: flags, validity: Validity(rows)),
      ],
      numRows: rows,
    );
    return table;
  }

  test('large flat tables use the column fast path and round-trip', () {
    const rows = 10000;
    final input = makeTable(rows);
    final bytes = writeParquetTable(
      input,
      options: const ParquetWriterOptions(
        compression: CompressionType.snappy,
        dataPageVersion: 2,
        writePageIndex: true,
        writeCrc: true,
      ),
    );
    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.numRows, rows);
    final output = reader.readTableSync();
    final id = output['id'] as Int64Column;
    final score = output['score'] as Float64Column;
    final name = output['name'] as BinaryColumn;
    final flag = output['flag'] as BooleanColumn;
    for (var i = 0; i < rows; i++) {
      expect(id.getInt(i), i * 3 - 7);
      expect(score.values[i], i / 3);
      expect(name.getString(i), 'value-${i % 1000}');
      expect(flag.getBool(i), i.isEven);
    }
    for (final group in reader.metadata.rowGroups) {
      for (final chunk in group.columns) {
        expect(chunk.hasDataPage, isTrue);
      }
    }
  });

  test('large flat table with nulls preserves nulls', () {
    const rows = 10000;
    final idValues = Int64Buffer(rows);
    final validity = Validity(rows);
    for (var i = 0; i < rows; i++) {
      if (i % 7 == 0) {
        validity.setNull(i);
      } else {
        idValues.setInt(i, i);
      }
    }
    final id = Int64Column(name: 'id', values: idValues, validity: validity);
    final table = ParquetTable(
      schema: (ParquetSchemaBuilder()..addInt64('id')).build(),
      columns: [id],
      numRows: rows,
    );
    final output =
        ParquetReader.fromBytes(writeParquetTable(table)).readTableSync()['id']
            as Int64Column;
    for (var i = 0; i < rows; i++) {
      expect(output.isNull(i), i % 7 == 0);
      if (i % 7 != 0) expect(output.getInt(i), i);
    }
  });
}
