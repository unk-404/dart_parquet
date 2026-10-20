/// Small dart2wasm smoke suite: read, exact int64 and write without any Web
/// storage API. Useful as the first Wasm CI gate.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_smoke_test.dart
/// ```
@TestOn('browser')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

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
}
