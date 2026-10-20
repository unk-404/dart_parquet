@TestOn('vm')
library;

import 'package:parquet/src/util/int64_web.dart';
import 'package:test/test.dart';

/// Regression tests for the limb arithmetic used by the dart2js build.
///
/// Running the web implementation on the VM is intentional: the file has no
/// web-only imports, and this catches rounding bugs such as `-2 -> 0` without
/// needing a browser.
void main() {
  test('exact negative values use two\'s-complement limbs', () {
    final buffer = Int64Buffer(4);
    buffer.setLimbs(0, 0xFFFFFFFE, 0xFFFFFFFF); // -2
    buffer.setBigInt(1, BigInt.from(-1));
    buffer.setBigInt(2, BigInt.from(-9007199254740991));
    buffer.setBigInt(3, BigInt.from(9007199254740991));

    expect(buffer.getInt(0), -2);
    expect(buffer.getInt(1), -1);
    expect(buffer.getInt(2), -9007199254740991);
    expect(buffer.getInt(3), 9007199254740991);
    expect(buffer.getBigInt(0), BigInt.from(-2));
  });

  test('values outside the JS exact range throw', () {
    final buffer = Int64Buffer(3);
    buffer.setBigInt(0, BigInt.parse('9007199254740993')); // 2^53 + 1
    buffer.setBigInt(1, BigInt.parse('-9007199254740992')); // -2^53
    buffer.setBigInt(2, BigInt.parse('9007199254740992')); // 2^53

    expect(() => buffer.getInt(0), throwsStateError);
    expect(() => buffer.getInt(1), throwsStateError);
    expect(() => buffer.getInt(2), throwsStateError);
    expect(buffer.getBigInt(1), BigInt.parse('-9007199254740992'));
  });
}
