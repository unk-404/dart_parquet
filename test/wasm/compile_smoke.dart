/// Compile-only Web smoke entry point.
///
/// This file is not a test: it exists so the full import graph can be compiled
/// for both compilers without starting a browser:
///
/// ```sh
/// mkdir -p build
/// dart compile js test/wasm/compile_smoke.dart -o build/web_smoke.js
/// dart compile wasm test/wasm/compile_smoke.dart -o build/wasm_smoke.wasm
/// ```
///
/// Running the compiled output also performs a tiny end-to-end read/write.
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';

import '../web/web_fixtures.dart';

void main() {
  final bytes = webFixture('flat_snappy.parquet');
  final reader = ParquetReader.fromBytes(bytes);
  final table = reader.readTableSync();
  if (table.numRows != 8) {
    throw StateError('expected 8 rows, got ${table.numRows}');
  }
  final i64 = table['i64'] as Int64Column;
  final exact = i64.getBigInt(6);
  if (exact != BigInt.parse('9007199254740993')) {
    throw StateError('int64 exactness lost: $exact');
  }

  final Uint8List encoded = writeParquetTable(
    table,
    options: const ParquetWriterOptions(
      compression: CompressionType.zstd,
      dataPageVersion: 2,
    ),
  );
  final roundTrip = ParquetReader.fromBytes(encoded).readTableSync();
  if (roundTrip.numRows != table.numRows) {
    throw StateError('round trip lost rows');
  }

  // v0.2 surface: deep nesting, ALP, Variant, feature types must all compile.
  final deep = ParquetReader.fromBytes(webFixture('deep_list_struct.parquet'))
      .readTableSync();
  if (deep['lsn'] is! ListColumn ||
      ((deep['lsn'] as ListColumn).child is! StructColumn)) {
    throw StateError('deep nesting shape changed');
  }

  final alpSchema = (ParquetSchemaBuilder()..addFloat64('d')).build();
  final alpValues = Float64List.fromList(<double>[1.5, -0.0, double.nan]);
  final alpTable = ParquetTable(
    schema: alpSchema,
    columns: <ParquetColumn>[
      Float64Column(
        name: 'd',
        values: alpValues,
        validity: Validity(alpValues.length),
      ),
    ],
    numRows: alpValues.length,
  );
  final alpBytes = writeParquetTable(
    alpTable,
    options: const ParquetWriterOptions(enableAlp: true),
  );
  final alpOut =
      ParquetReader.fromBytes(alpBytes).readTableSync()['d'] as Float64Column;
  if (!alpOut.getDouble(2).isNaN || !alpOut.getDouble(1).isNegative) {
    throw StateError('ALP special values changed');
  }

  final variantSchema = (ParquetSchemaBuilder()..addVariant('v')).build();
  final variantValues = <Variant?>[
    Variant.fromJson(<String, Object?>{'a': 1}),
    null,
  ];
  final variantValidity = Validity(variantValues.length)..setNull(1);
  final variantTable = ParquetTable(
    schema: variantSchema,
    columns: <ParquetColumn>[
      VariantColumn(
        name: 'v',
        values: variantValues,
        validity: variantValidity,
      ),
    ],
    numRows: variantValues.length,
  );
  final variantOut =
      ParquetReader.fromBytes(writeParquetTable(variantTable))
              .readTableSync()['v']
          as VariantColumn;
  if (variantOut.isNull(1) ||
      (variantOut.getVariant(0)!.toJson()! as Map<Object?, Object?>)['a'] !=
          1) {
    throw StateError('Variant round trip changed');
  }

  // Feature-detection types must be reachable from the public library.
  final features = ParquetFeature.values.map((feature) => feature.name).toSet();
  if (!features.contains('encryption') || !features.contains('variant')) {
    throw StateError('ParquetFeature surface changed: $features');
  }
  final cdc = CdcTable(table);
  if (cdc.numRows != table.numRows) {
    throw StateError('CdcTable surface changed');
  }

  // ignore: avoid_print
  print(
    'parquet compile smoke ok: ${table.numRows} rows, '
    '${bytes.length} bytes in, ${encoded.length} bytes out, '
    'deep/variant/alp/cdc surfaces compiled, i64[6]=$exact',
  );
}
