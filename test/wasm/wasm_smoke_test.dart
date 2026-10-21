/// Small dart2wasm smoke suite: read, exact int64 and write without any Web
/// storage API. Useful as the first Wasm CI gate.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_smoke_test.dart
/// ```
@TestOn('browser')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import '../web/deep_fixture_expectations.dart';
import '../web/fixture_expectations.dart';
import '../web/web_fixtures.dart';

void main() {
  test('wasm: reads a flat Snappy file', () {
    final reader = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'));
    expect(reader.numRows, 8);
    expectFlatTable(reader.readTableSync());
  });

  test('wasm: reads nested list/struct/map', () {
    final reader = ParquetReader.fromBytes(webFixture('nested_v2.parquet'));
    expectNestedTable(reader.readTableSync());
  });

  test('wasm: int64 is exact through BigInt', () {
    final i64 =
        ParquetReader.fromBytes(webFixture('flat_delta.parquet'))
                .readTableSync()['i64']
            as Int64Column;
    expect(i64.getBigInt(6), BigInt.parse('9007199254740993'));
    expect(i64.getBigInt(7), BigInt.parse('-9223372036854775808'));
  });

  test('wasm: writer round trip with Zstd and page v2', () {
    final table = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'))
        .readTableSync();
    final bytes = writeParquetTable(
      table,
      options: const ParquetWriterOptions(
        compression: CompressionType.zstd,
        dataPageVersion: 2,
      ),
    );
    final reader = ParquetReader.fromBytes(bytes);
    expect(
      reader.metadata.rowGroup(0).column(0).compression,
      CompressionType.zstd,
    );
    expectFirstDataPageVersion(bytes, reader, ['i64'], 2);
    expectFlatTable(reader.readTableSync());
  });

  test('wasm: deep list<struct{list}> read', () {
    final table = ParquetReader.fromBytes(
      webFixture('deep_list_struct.parquet'),
    ).readTableSync();
    final lsn = table['lsn'] as ListColumn;
    expect((lsn.child as StructColumn).fields['tags'], isA<ListColumn>());
    expectColumnValues(lsn, <Object?>[
      [
        {
          'a': 1,
          'tags': ['t1', 't2'],
        },
      ],
      <Object?>[],
      null,
      [
        {'a': 2, 'tags': null},
        {'a': 3, 'tags': <String>[]},
      ],
    ], reason: 'lsn');
  });

  test('wasm: ALP write/read preserves special float bits', () {
    final schema = (ParquetSchemaBuilder()..addFloat64('d')).build();
    final values = Float64List.fromList(<double>[
      1.5,
      -0.0,
      double.nan,
      double.infinity,
      5e-324,
    ]);
    final validity = Validity(values.length)..setNull(4);
    final table = ParquetTable(
      schema: schema,
      columns: <ParquetColumn>[
        Float64Column(name: 'd', values: values, validity: validity),
      ],
      numRows: values.length,
    );
    final reader = ParquetReader.fromBytes(
      writeParquetTable(
        table,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          enableAlp: true,
        ),
      ),
    );
    expect(
      reader.metadata.rowGroup(0).column(0).encodings,
      contains(Encoding.alp),
    );
    final output = reader.readTableSync()['d'] as Float64Column;
    final bits = ByteData.sublistView(output.values);
    expect(bits.getUint32(8, Endian.little), 0);
    expect(bits.getUint32(12, Endian.little), 0x80000000, reason: '-0.0');
    expect(output.isNull(4), isTrue);
    expect(output.getDouble(2).isNaN, isTrue);
    expect(output.getDouble(3), double.infinity);
  });

  test('wasm: unshredded Variant write/read', () {
    final schema = (ParquetSchemaBuilder()..addVariant('v')).build();
    final input = <Variant?>[
      Variant.fromJson(<String, Object?>{
        'a': 1,
        'b': <Object?>['x', true, null],
      }),
      null,
    ];
    final validity = Validity(input.length)..setNull(1);
    final table = ParquetTable(
      schema: schema,
      columns: <ParquetColumn>[
        VariantColumn(name: 'v', values: input, validity: validity),
      ],
      numRows: input.length,
    );
    final output =
        ParquetReader.fromBytes(writeParquetTable(table)).readTableSync()['v']
            as VariantColumn;
    expect(output.isNull(1), isTrue);
    expect(output.getVariant(0)!.toJson(), <String, Object?>{
      'a': 1,
      'b': <Object?>['x', true, null],
    });
  });
}
