@TestOn('vm')
library;

import 'package:test/test.dart';

import 'interop_support.dart';

/// Value-exact reads of the apache/parquet-testing subset and the
/// pyarrow-generated fixtures. Expectations come from pyarrow ("expected/*.json").
void main() {
  final fixtures = loadExpectedFixtures();
  final values = <ExpectedFixture>[
    for (final fixture in fixtures)
      if (fixture.kind == 'values' || fixture.kind == 'manual') fixture,
  ];
  final gaps = <ExpectedFixture>[
    for (final fixture in fixtures)
      if (fixture.kind == 'gap') fixture,
  ];

  test('manifest lists 50 value fixtures from multiple writer origins', () {
    expect(values.length, 50);
    final families = <String>{
      for (final fixture in fixtures)
        (fixture.doc['writer_family'] as String?) ?? 'unknown',
    };
    expect(
      families,
      containsAll(<String>[
        'impala',
        'parquet-mr',
        'arrow-cpp',
        'parquet-cpp',
        'arrow-rs',
      ]),
    );
  });

  for (final fixture in values) {
    test(
      '${fixture.file} matches pyarrow values',
      () => verifyFixtureValues(fixture),
      timeout: const Timeout(Duration(seconds: 120)),
    );
  }

  for (final fixture in gaps) {
    test(
      '${fixture.file} known gap',
      () {},
      skip: fixture.doc['gap'] as String? ?? 'known gap',
    );
  }
}
