@TestOn('vm')
library;

import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'interop_support.dart';

/// Deterministic, bounded fuzzing of footer / Thrift / page / level / codec
/// bytes. Every failure must be a typed [ParquetException]; arbitrary
/// `RangeError`/`StateError`/`TypeError` leaks are reported as test failures.
void main() {
  final seedBytes = buildFuzzSeed();
  final compressedBytes = _readFixture('official/datapage_v2.snappy.parquet');

  test('fuzz seed is a valid file', () {
    final reader = ParquetReader.fromBytes(seedBytes);
    expect(reader.numRows, 64);
    expect(reader.readTableSync().numRows, 64);
  });

  test('malformed footer magic / length / truncation always throw', () {
    // Too short, bad magic, impossible footer length.
    _expectThrow(Uint8List(0), 'empty');
    _expectThrow(Uint8List.fromList(<int>[0x50, 0x41, 0x52, 0x31]), '4 bytes');
    _expectThrow(seedBytes.sublist(0, 7), '7 bytes');
    _expectThrow(_withMagic(seedBytes, 'XXXX'), 'bad leading magic');
    _expectThrow(_withTrailingMagic(seedBytes, 'XXXX'), 'bad trailing magic');

    final footerLengths = <int>[0, 1, 2, 3, 7, 9, 0x7FFFFFFF, 0xFFFFFFFF];
    for (final length in footerLengths) {
      final mutated = Uint8List.fromList(seedBytes);
      writeUint32LE(mutated, mutated.length - 8, length);
      _expectThrow(mutated, 'footer length $length');
    }

    // Every truncation point must fail cleanly (footer/trailer is incomplete).
    final starts = <int>{
      0,
      1,
      2,
      3,
      4,
      5,
      7,
      8,
      12,
      seedBytes.length - 20,
      seedBytes.length - 9,
      seedBytes.length - 8,
      seedBytes.length - 4,
      footerStart(seedBytes),
      footerStart(seedBytes) + 1,
      seedBytes.length ~/ 2,
    };
    for (final start in starts) {
      if (start < 0 || start >= seedBytes.length) continue;
      _expectThrow(
        Uint8List.fromList(seedBytes.sublist(0, start)),
        'truncated at $start',
      );
    }
  });

  test('random footer byte flips stay typed', () {
    final random = Random(0xC0FFEE);
    final start = footerStart(seedBytes);
    final stats = _Stats();
    for (var i = 0; i < 80; i++) {
      final mutated = Uint8List.fromList(seedBytes);
      final offset = start + random.nextInt(mutated.length - 8 - start);
      mutated[offset] ^= 1 << random.nextInt(8);
      stats.exercise(mutated, 'footer flip $offset');
    }
    expect(stats.total, 80);
    expect(stats.typedErrors, greaterThan(0));
  });

  test('random page/level/data byte flips stay typed', () {
    final random = Random(0xBADF00D);
    final stop = footerStart(seedBytes);
    final stats = _Stats();
    for (var i = 0; i < 160; i++) {
      final mutated = Uint8List.fromList(seedBytes);
      final offset = 4 + random.nextInt(stop - 4);
      final width = 1 + random.nextInt(4);
      for (var b = 0; b < width && offset + b < stop; b++) {
        mutated[offset + b] ^= 1 << random.nextInt(8);
      }
      stats.exercise(mutated, 'page flip $offset');
    }
    expect(stats.total, 160);
    expect(stats.typedErrors, greaterThan(0));
  });

  test('targeted page-header and compressed-payload corruption', () {
    final reader = ParquetReader.fromBytes(compressedBytes);
    final chunk = reader.metadata.rowGroup(0).column(0);
    final dataOffset = chunk.dataPageOffset!;
    final random = Random(0x5EED);
    final stats = _Stats();
    // Mutate around the page header and the first bytes of the level/values
    // payload. Some flips may still decode (values just change); none may
    // throw anything other than ParquetException.
    for (var i = 0; i < 120; i++) {
      final mutated = Uint8List.fromList(compressedBytes);
      final offset = dataOffset + random.nextInt(48);
      if (offset >= mutated.length - 8) continue;
      mutated[offset] ^= 1 << random.nextInt(8);
      stats.exercise(mutated, 'page payload flip $offset');
    }
    expect(stats.total, greaterThan(60));
  });

  test('corrupt Thrift-compressed footer bytes stay typed', () {
    final random = Random(0xDEADBEEF);
    final start = footerStart(seedBytes);
    final stats = _Stats();
    for (var i = 0; i < 60; i++) {
      final mutated = Uint8List.fromList(seedBytes);
      for (var b = start; b < mutated.length - 8; b++) {
        mutated[b] = random.nextInt(256);
      }
      stats.exercise(mutated, 'random footer $i');
    }
    expect(stats.total, 60);
    expect(
      stats.typedErrors,
      equals(60),
      reason: 'random footer must not parse',
    );
  });

  test('deeply nested length/count corruption does not hang', () {
    // Flip each of the first 24 bytes of the footer's Thrift struct one at a
    // time; bounded enough to catch unbounded allocation loops.
    final start = footerStart(seedBytes);
    final stats = _Stats();
    for (var i = 0; i < 24; i++) {
      final mutated = Uint8List.fromList(seedBytes);
      final offset = start + i;
      mutated[offset] = (mutated[offset] + 1) & 0xFF;
      stats.exercise(mutated, 'footer prefix $i');
    }
    expect(stats.total, 24);
  });
}

Uint8List _readFixture(String relative) =>
    File('test/fixtures/interop/$relative').readAsBytesSync();

Uint8List _withMagic(Uint8List bytes, String magic) {
  final out = Uint8List.fromList(bytes);
  out.setRange(0, 4, magic.codeUnits);
  return out;
}

Uint8List _withTrailingMagic(Uint8List bytes, String magic) {
  final out = Uint8List.fromList(bytes);
  out.setRange(out.length - 4, out.length, magic.codeUnits);
  return out;
}

void _expectThrow(Uint8List bytes, String label) {
  try {
    ParquetReader.fromBytes(bytes).readTableSync();
    fail('$label: expected a ParquetException');
  } on ParquetException {
    // expected
  } catch (error, stackTrace) {
    fail(
      '$label threw ${error.runtimeType} instead of ParquetException: '
      '$error\n$stackTrace',
    );
  }
}

final class _Stats {
  int successes = 0;
  int typedErrors = 0;

  int get total => successes + typedErrors;

  void exercise(Uint8List bytes, String label) {
    try {
      final reader = ParquetReader.fromBytes(bytes);
      final table = reader.readTableSync();
      expect(
        table.numRows,
        reader.numRows,
        reason: '$label: row count mismatch after successful parse',
      );
      successes++;
    } on ParquetException {
      typedErrors++;
    } catch (error, stackTrace) {
      fail(
        '$label threw ${error.runtimeType} instead of ParquetException: '
        '$error\n$stackTrace',
      );
    }
  }
}
