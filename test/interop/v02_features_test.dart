@TestOn('vm')
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

const _v02 = 'test/fixtures/interop/v0.2';

void main() {
  test('ALP fixture decodes bit-exactly against PLAIN references', () {
    final bytes = File('$_v02/alp/alp_extended.zstd.parquet').readAsBytesSync();
    final reader = ParquetReader.fromBytes(bytes);
    expect(reader.numRows, 9032);
    final table = reader.readTableSync();
    for (final name in ['float_alp_1024', 'float_alp_4096', 'float_alp_32']) {
      final reference = table['float_plain'] as Float32Column;
      final alp = table[name] as Float32Column;
      expect(alp.length, reference.length);
      final a = ByteData.sublistView(reference.values);
      final b = ByteData.sublistView(alp.values);
      for (var i = 0; i < reference.length; i++) {
        expect(reference.isNull(i), alp.isNull(i));
        if (!reference.isNull(i)) {
          expect(
            b.getUint32(i * 4, Endian.little),
            a.getUint32(i * 4, Endian.little),
            reason: '$name row $i',
          );
        }
      }
    }
    for (final name in [
      'double_alp_1024',
      'double_alp_4096',
      'double_alp_32',
    ]) {
      final reference = table['double_plain'] as Float64Column;
      final alp = table[name] as Float64Column;
      final a = ByteData.sublistView(reference.values);
      final b = ByteData.sublistView(alp.values);
      for (var i = 0; i < reference.length; i++) {
        expect(reference.isNull(i), alp.isNull(i));
        if (!reference.isNull(i)) {
          expect(
            b.getUint64(i * 8, Endian.little),
            a.getUint64(i * 8, Endian.little),
            reason: '$name row $i',
          );
        }
      }
    }
    for (final group in reader.metadata.rowGroups) {
      for (final column in group.columns) {
        if (column.dotPath.contains('_alp_')) {
          expect(column.encodings, contains(Encoding.alp));
        }
      }
    }
  });

  test('shredded Variant schemas are rejected for writing', () {
    final table = ParquetReader.fromBytes(
      File('$_v02/variant/case-044.parquet').readAsBytesSync(),
    ).readTableSync();
    expect(
      () => writeParquetTable(table),
      throwsA(isA<ParquetUnsupportedException>()),
    );
  });

  test('PARE public reader exposes the encryption feature', () {
    final bytes = File(
      'test/fixtures/interop/official/'
      'encrypt_columns_and_footer.parquet.encrypted',
    ).readAsBytesSync();
    expect(
      () => ParquetReader.fromBytes(bytes),
      throwsA(
        isA<ParquetUnsupportedFeatureException>().having(
          (error) => error.feature,
          'feature',
          ParquetFeature.encryption,
        ),
      ),
    );
  });

  test('Variant fixtures match upstream semantics', () {
    final expected = <String, Object?>{
      '047': null,
      '048': true,
      '049': false,
      '050': 34,
      '005': false,
      '006': 34,
      '044': {
        'c': {'a': 34, 'b': 'iceberg'},
        'd': -0.0,
      },
      '130': <String, Object?>{},
    };
    for (final entry in expected.entries) {
      final reader = ParquetReader.fromBytes(
        File('$_v02/variant/case-${entry.key}.parquet').readAsBytesSync(),
      );
      final table = reader.readTableSync();
      final column = table['var'] as VariantColumn;
      expect(column.isNull(0), isFalse, reason: 'case ${entry.key}');
      expect(column.getVariant(0)!.toJson(), entry.value);
    }
  });
}
