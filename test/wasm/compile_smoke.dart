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
  // ignore: avoid_print
  print(
    'parquet compile smoke ok: ${table.numRows} rows, '
    '${bytes.length} bytes in, ${encoded.length} bytes out, '
    'i64[6]=$exact',
  );
}
