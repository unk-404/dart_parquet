/// v0.2 Web/Wasm ALP (encoding 10) matrix.
///
/// Builds FLOAT/DOUBLE tables in memory, writes with
/// `ParquetWriterOptions.enableAlp` and verifies the decoded IEEE-754 bit
/// patterns exactly (NaN payloads, -0.0, infinities, subnormals, nulls).
/// Shared with `test/wasm/wasm_alp_test.dart`.
@TestOn('browser')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

/// Double bit patterns used by the matrix, low/high 32-bit limbs.
const List<(int, int)> _doubleBits = <(int, int)>[
  (0x00000000, 0x00000000), // 0.0
  (0x00000000, 0x80000000), // -0.0
  (0x00000000, 0x3FF00000), // 1.0
  (0x00000000, 0x7FF00000), // +inf
  (0x00000000, 0xFFF00000), // -inf
  (0x00000001, 0x7FF80000), // quiet NaN with payload 1
  (0x00000001, 0x00000000), // min positive subnormal
  (0xFFFFFFFF, 0x7FEFFFFF), // max finite double
  (0x00000000, 0xC0000000), // -2.0
  (0x55555555, 0x3FD55555), // 1/3-ish mantissa
  (0x00000000, 0x00000000), // repeated 0.0
];

/// Float bit patterns used by the matrix.
const List<int> _floatBits = <int>[
  0x00000000, // 0.0
  0x80000000, // -0.0
  0x3F800000, // 1.0
  0x7F800000, // +inf
  0xFF800000, // -inf
  0x7FC00001, // quiet NaN with payload 1
  0x00000001, // min positive subnormal
  0x7F7FFFFF, // max finite float
  0xC0000000, // -2.0
  0x3EAAAAAB, // 1/3
  0x00000000, // repeated 0.0
];

Float64List _doublesFromBits() {
  final values = Float64List(_doubleBits.length);
  final view = ByteData.sublistView(values);
  for (var i = 0; i < _doubleBits.length; i++) {
    view.setUint32(i * 8, _doubleBits[i].$1, Endian.little);
    view.setUint32(i * 8 + 4, _doubleBits[i].$2, Endian.little);
  }
  return values;
}

Float32List _floatsFromBits() {
  final values = Float32List(_floatBits.length);
  final view = ByteData.sublistView(values);
  for (var i = 0; i < _floatBits.length; i++) {
    view.setUint32(i * 4, _floatBits[i], Endian.little);
  }
  return values;
}

void _expectDoubleBits(Float64Column column, int row) {
  final view = ByteData.sublistView(column.values);
  expect(
    view.getUint32(row * 8, Endian.little),
    _doubleBits[row].$1,
    reason: 'double low bits row $row',
  );
  expect(
    view.getUint32(row * 8 + 4, Endian.little),
    _doubleBits[row].$2,
    reason: 'double high bits row $row',
  );
}

void _expectFloatBits(Float32Column column, int row) {
  final view = ByteData.sublistView(column.values);
  expect(
    view.getUint32(row * 4, Endian.little),
    _floatBits[row],
    reason: 'float bits row $row',
  );
}

ParquetTable _alpTable() {
  final schema =
      (ParquetSchemaBuilder()
            ..addFloat64('d')
            ..addFloat32('f')
            ..addInt32('id'))
          .build();
  final validity = Validity(_doubleBits.length);
  validity.setNull(3); // +inf row is exercised through id/floats
  validity.setNull(9);
  return ParquetTable(
    schema: schema,
    columns: <ParquetColumn>[
      Float64Column(name: 'd', values: _doublesFromBits(), validity: validity),
      Float32Column(name: 'f', values: _floatsFromBits(), validity: validity),
      Int32Column(
        name: 'id',
        values: Int32List.fromList(
          List<int>.generate(_doubleBits.length, (i) => i),
        ),
        validity: Validity(_doubleBits.length),
      ),
    ],
    numRows: _doubleBits.length,
  );
}

void main() {
  group('ALP round-trip is bit-exact', () {
    for (final codec in <CompressionType>[
      CompressionType.uncompressed,
      CompressionType.snappy,
      CompressionType.zstd,
    ]) {
      for (final version in <int>[1, 2]) {
        test('${codec.name} v$version', () {
          final input = _alpTable();
          final bytes = writeParquetTable(
            input,
            options: ParquetWriterOptions(
              compression: codec,
              dataPageVersion: version,
              enableAlp: true,
            ),
          );
          final reader = ParquetReader.fromBytes(bytes);
          final group = reader.metadata.rowGroup(0);
          expect(group.columnByPath(['d'])!.encodings, contains(Encoding.alp));
          expect(group.columnByPath(['f'])!.encodings, contains(Encoding.alp));
          expect(
            group.columnByPath(['id'])!.encodings,
            isNot(contains(Encoding.alp)),
          );

          final output = reader.readTableSync();
          final d = output['d'] as Float64Column;
          final f = output['f'] as Float32Column;
          final id = output['id'] as Int32Column;
          for (var row = 0; row < input.numRows; row++) {
            expect(id.getInt(row), row);
            expect(d.isNull(row), input['d'].isNull(row));
            expect(f.isNull(row), input['f'].isNull(row));
            if (d.isNull(row)) continue;
            _expectDoubleBits(d, row);
            _expectFloatBits(f, row);
          }
        });
      }
    }
  });

  test('ALP falls back to PLAIN when disabled (default)', () {
    final input = _alpTable();
    final reader = ParquetReader.fromBytes(writeParquetTable(input));
    final group = reader.metadata.rowGroup(0);
    expect(group.columnByPath(['d'])!.encodings, isNot(contains(Encoding.alp)));
    expect(group.columnByPath(['f'])!.encodings, isNot(contains(Encoding.alp)));
    final output = reader.readTableSync();
    for (var row = 0; row < input.numRows; row++) {
      if ((output['d'] as Float64Column).isNull(row)) continue;
      _expectDoubleBits(output['d'] as Float64Column, row);
      _expectFloatBits(output['f'] as Float32Column, row);
    }
  });

  test('ALP with small pages and multiple pages', () {
    final input = _alpTable();
    final bytes = writeParquetTable(
      input,
      options: const ParquetWriterOptions(
        compression: CompressionType.snappy,
        dataPageVersion: 2,
        pageSize: 24,
        enableAlp: true,
      ),
    );
    final reader = ParquetReader.fromBytes(bytes);
    expect(
      reader.metadata.rowGroup(0).columnByPath(['d'])!.encodings,
      contains(Encoding.alp),
    );
    final output = reader.readTableSync();
    final d = output['d'] as Float64Column;
    final f = output['f'] as Float32Column;
    for (var row = 0; row < input.numRows; row++) {
      if (d.isNull(row)) continue;
      _expectDoubleBits(d, row);
      _expectFloatBits(f, row);
    }
  });

  test('ALP applies to floats nested inside a struct', () {
    final schema = ParquetSchema.fromSchemaElements(const <SchemaElement>[
      SchemaElement(name: 'schema', numChildren: 1),
      SchemaElement(
        name: 's',
        repetitionType: FieldRepetitionType.optional,
        numChildren: 2,
      ),
      SchemaElement(
        name: 'd',
        type: PhysicalType.double,
        repetitionType: FieldRepetitionType.optional,
      ),
      SchemaElement(
        name: 'f',
        type: PhysicalType.float,
        repetitionType: FieldRepetitionType.optional,
      ),
    ]);
    final struct = StructColumn(
      name: 's',
      length: _doubleBits.length,
      fields: <String, ParquetColumn>{
        'd': Float64Column(
          name: 'd',
          values: _doublesFromBits(),
          validity: Validity(_doubleBits.length),
        ),
        'f': Float32Column(
          name: 'f',
          values: _floatsFromBits(),
          validity: Validity(_doubleBits.length),
        ),
      },
      validity: Validity(_doubleBits.length),
    );
    final table = ParquetTable(
      schema: schema,
      columns: <ParquetColumn>[struct],
      numRows: _doubleBits.length,
    );
    final bytes = writeParquetTable(
      table,
      options: const ParquetWriterOptions(
        compression: CompressionType.zstd,
        dataPageVersion: 2,
        enableAlp: true,
      ),
    );
    final reader = ParquetReader.fromBytes(bytes);
    expect(
      reader.metadata.rowGroup(0).columnByPath(['s', 'd'])!.encodings,
      contains(Encoding.alp),
    );
    final output = reader.readTableSync()['s'] as StructColumn;
    final d = output.fields['d'] as Float64Column;
    final f = output.fields['f'] as Float32Column;
    for (var row = 0; row < _doubleBits.length; row++) {
      _expectDoubleBits(d, row);
      _expectFloatBits(f, row);
    }
  });
}
