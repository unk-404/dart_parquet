@TestOn('vm')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

void main() {
  final schema = (ParquetSchemaBuilder()..addVariant('v')).build();

  ParquetTable table() {
    final values = <Variant?>[
      Variant.fromJson({
        'a': 1,
        'b': ['x', true, null],
      }),
      null,
      Variant.fromJson('hello'),
      Variant.fromJson(3.5),
      Variant.fromJson([1, 2, 3]),
    ];
    final validity = Validity(values.length);
    validity.setNull(1);
    return ParquetTable(
      schema: schema,
      columns: [VariantColumn(name: 'v', values: values, validity: validity)],
      numRows: values.length,
    );
  }

  for (final version in [1, 2]) {
    test('Variant round-trip v$version', () {
      final input = table();
      final bytes = writeParquetTable(
        input,
        options: ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: version,
        ),
      );
      final output = ParquetReader.fromBytes(bytes).readTableSync();
      final column = output['v'] as VariantColumn;
      expect(column.length, input.numRows);
      for (var i = 0; i < input.numRows; i++) {
        expect(column.isNull(i), input['v'].isNull(i));
        if (!column.isNull(i)) {
          expect(
            column.getVariant(i)!.toJson(),
            input['v'].value(i) is Variant
                ? (input['v'].value(i) as Variant).toJson()
                : null,
          );
        }
      }
    });
  }
}
