@TestOn('vm')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import '../../tool/interop/interop_canonical.dart';
import 'interop_support.dart';

/// Page CRC verification against the official parquet-testing checksum
/// fixtures (v0.1.2 CRC scope). Verification is opt-in via
/// [ParquetReaderOptions.verifyCrc].
void main() {
  final fixtures = loadExpectedFixtures();

  test('valid CRC fixture reads identically with verifyCrc', () {
    final fixture = fixtures.firstWhere(
      (candidate) =>
          candidate.file ==
          'official/datapage_v1-uncompressed-checksum.parquet',
    );
    final verified = ParquetReader.fromBytes(
      fixture.readBytes(),
      options: const ParquetReaderOptions(verifyCrc: true),
    );
    final table = verified.readTableSync();
    for (final expected in fixture.columns) {
      final column = table[expected['name'] as String];
      expect(
        columnChecksum(column),
        expected['checksum'],
        reason: 'verifyCrc column ${expected['name']}',
      );
    }
  });

  test('corrupt CRC fixture throws only with verifyCrc enabled', () {
    final fixture = fixtures.firstWhere(
      (candidate) =>
          candidate.file == 'official/datapage_v1-corrupt-checksum.parquet',
    );
    final bytes = fixture.readBytes();

    // Default is lenient: the values decode even though one page CRC is bad.
    final lenient = ParquetReader.fromBytes(bytes).readTableSync();
    expect(lenient.numRows, 5120);

    expect(
      () => ParquetReader.fromBytes(
        bytes,
        options: const ParquetReaderOptions(verifyCrc: true),
      ).readTableSync(),
      throwsA(
        isA<ParquetFormatException>().having(
          (error) => error.message,
          'message',
          contains('CRC mismatch'),
        ),
      ),
    );
  });
}
