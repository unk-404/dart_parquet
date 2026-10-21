@TestOn('vm')
library;

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'interop_support.dart';

/// Unsupported encodings/schema shapes and malformed files must fail with typed
/// `ParquetException`s (never hang, never leak `RangeError`/`StateError`/...).
void main() {
  final fixtures = loadExpectedFixtures();
  final unsupported = <ExpectedFixture>[
    for (final fixture in fixtures)
      if (fixture.kind == 'unsupported') fixture,
  ];
  final malformed = <ExpectedFixture>[
    for (final fixture in fixtures)
      if (fixture.kind == 'malformed') fixture,
  ];

  test('manifest lists unsupported and malformed fixtures', () {
    expect(unsupported.length, 2);
    expect(malformed.length, 6);
  });

  for (final fixture in unsupported) {
    test(
      '${fixture.file} throws typed unsupported error',
      () => verifyFixtureThrows(fixture),
      timeout: const Timeout(Duration(seconds: 30)),
    );
  }

  for (final fixture in malformed) {
    test(
      '${fixture.file} throws typed malformed error',
      () => verifyFixtureThrows(fixture),
      timeout: const Timeout(Duration(seconds: 30)),
    );
  }

  test('encrypted footer magic reports ParquetUnsupportedException', () {
    final fixture = fixtures.firstWhere(
      (candidate) =>
          candidate.file ==
          'official/encrypt_columns_and_footer.parquet.encrypted',
    );
    final bytes = fixture.readBytes();
    expect(
      () => ParquetReader.fromBytes(bytes),
      throwsA(
        isA<ParquetUnsupportedException>().having(
          (error) => error.message,
          'message',
          contains('encrypted'),
        ),
      ),
    );
  });
}
