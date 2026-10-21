/// v0.2 Web/Wasm Variant matrix.
///
/// Covers the unshredded writer path (`ParquetSchemaBuilder.addVariant` +
/// `VariantColumn`) and the shredded reader path (vendored
/// apache/parquet-testing case-044). Shared with
/// `test/wasm/wasm_variant_test.dart`.
@TestOn('browser')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'web_fixtures.dart';

Object? _canonical(Object? value) {
  if (value is Map) {
    final pairs = <List<Object?>>[
      for (final entry in value.entries)
        <Object?>[_canonical(entry.key), _canonical(entry.value)],
    ]..sort((a, b) => a[0].toString().compareTo(b[0].toString()));
    return pairs;
  }
  if (value is List) {
    return <Object?>[for (final item in value) _canonical(item)];
  }
  return value;
}

void _expectVariantJson(Variant variant, Object? expected, {String? reason}) {
  expect(_canonical(variant.toJson()), _canonical(expected), reason: reason);
}

const List<Object?> _vJson = <Object?>[
  <String, Object?>{
    'a': 1,
    'b': <Object?>['x', true, null],
    'nested': <String, Object?>{
      'k': -0.0,
      'deep': <Object?>[1, 2],
    },
  },
  null, // SQL NULL (validity bit), not a Variant NULL value
  'hello',
  3.5,
  <int>[1, 2, 3],
  true,
  <Object?>[null, 'tail'],
];

const List<Object?> _wJson = <Object?>[
  'w0',
  'w1',
  null, // SQL NULL
  <Object?>[true, false],
  0,
  <String, Object?>{'only': 'object'},
  <int>[7],
];

ParquetTable _unshreddedTable() {
  final schema =
      (ParquetSchemaBuilder()
            ..addVariant('v')
            ..addVariant('w')
            ..addInt32('id'))
          .build();
  final v = <Variant?>[for (final json in _vJson) Variant.fromJson(json)];
  final w = <Variant?>[for (final json in _wJson) Variant.fromJson(json)];
  final vValidity = Validity(v.length)..setNull(1);
  final wValidity = Validity(w.length)..setNull(2);
  return ParquetTable(
    schema: schema,
    columns: <ParquetColumn>[
      VariantColumn(name: 'v', values: v, validity: vValidity),
      VariantColumn(name: 'w', values: w, validity: wValidity),
      Int32Column(
        name: 'id',
        values: Int32List.fromList(List<int>.generate(v.length, (i) => i)),
        validity: Validity(v.length),
      ),
    ],
    numRows: v.length,
  );
}

void main() {
  group('unshredded writer round-trip', () {
    for (final codec in <CompressionType>[
      CompressionType.uncompressed,
      CompressionType.snappy,
      CompressionType.zstd,
    ]) {
      for (final version in <int>[1, 2]) {
        test('${codec.name} v$version', () {
          final input = _unshreddedTable();
          final bytes = writeParquetTable(
            input,
            options: ParquetWriterOptions(
              compression: codec,
              dataPageVersion: version,
            ),
          );
          final reader = ParquetReader.fromBytes(bytes);
          final node = reader.schema.root.children.first;
          expect(node.logicalType, isA<VariantLogicalType>());
          expect(
            supportsVariantSpecificationVersion(
              (node.logicalType! as VariantLogicalType).specificationVersion,
            ),
            isTrue,
          );

          final output = reader.readTableSync();
          final v = output['v'] as VariantColumn;
          final w = output['w'] as VariantColumn;
          expect(v.length, _vJson.length);
          for (var row = 0; row < _vJson.length; row++) {
            expect(v.isNull(row), row == 1, reason: 'v[$row] validity');
            if (row == 1) continue;
            final variant = v.getVariant(row)!;
            _expectVariantJson(variant, _vJson[row], reason: 'v[$row]');
            final original = input['v'].value(row)! as Variant;
            expect(
              variant.metadataBytes,
              original.metadataBytes,
              reason: 'v[$row] metadata bytes',
            );
            expect(
              variant.valueBytes,
              original.valueBytes,
              reason: 'v[$row] value bytes',
            );
          }
          for (var row = 0; row < _wJson.length; row++) {
            expect(w.isNull(row), row == 2, reason: 'w[$row] validity');
            if (row == 2) continue;
            _expectVariantJson(
              w.getVariant(row)!,
              _wJson[row],
              reason: 'w[$row]',
            );
          }
        });
      }
    }

    test('nested -0.0 survives JSON construction and round-trip', () {
      final source = Variant.fromJson(<String, Object?>{'k': -0.0});
      final table = ParquetTable(
        schema: (ParquetSchemaBuilder()..addVariant('v')).build(),
        columns: <ParquetColumn>[
          VariantColumn(
            name: 'v',
            values: <Variant?>[source],
            validity: Validity(1),
          ),
        ],
        numRows: 1,
      );
      final back = ParquetReader.fromBytes(writeParquetTable(table))
          .readTableSync();
      final json =
          (back['v'] as VariantColumn).getVariant(0)!.toJson()!
              as Map<Object?, Object?>;
      final value = json['k']! as double;
      expect(value, 0.0);
      expect(value.isNegative, isTrue);
    });

    test('projection selects only the variant column', () {
      final table = _unshreddedTable();
      final reader = ParquetReader.fromBytes(writeParquetTable(table));
      final projected = reader.readTableSync(columns: ['v']);
      expect(projected.columns.map((column) => column.name).toList(), <String>[
        'v',
      ]);
      _expectVariantJson(
        (projected['v'] as VariantColumn).getVariant(0)!,
        _vJson[0],
      );
    });
  });

  group('shredded reader fixtures', () {
    test('case-047 is an unshredded Variant NULL value', () {
      final reader = ParquetReader.fromBytes(webFixture('case-047.parquet'));
      final table = reader.readTableSync();
      final column = table['var'] as VariantColumn;
      expect(column.isNull(0), isFalse);
      _expectVariantJson(column.getVariant(0)!, null);
      expect(column.getVariant(0)!.metadataBytes, <int>[1, 0, 0]);
      expect(column.getVariant(0)!.valueBytes, <int>[0]);
    });

    test('case-048 is an unshredded Variant true value', () {
      final table = ParquetReader.fromBytes(webFixture('case-048.parquet'))
          .readTableSync();
      final column = table['var'] as VariantColumn;
      expect(column.isNull(0), isFalse);
      expect(column.getVariant(0)!.toJson(), isTrue);
      expect(column.getVariant(0)!.valueBytes, <int>[4]);
    });

    test('case-044 reconstructs object shredding', () {
      final reader = ParquetReader.fromBytes(webFixture('case-044.parquet'));
      final table = reader.readTableSync();
      final column = table['var'] as VariantColumn;
      expect(column.isNull(0), isFalse);
      final json = column.getVariant(0)!.toJson();
      expect(json, isA<Map>());
      final map = json! as Map<Object?, Object?>;
      final nested = map['c']! as Map<Object?, Object?>;
      expect(nested['a'], 34);
      expect(nested['b'], 'iceberg');
      final d = map['d'];
      expect(d, isA<double>());
      expect(d! as double, 0.0);
      expect((d as double).isNegative, isTrue, reason: 'negative zero');
      expect(
        reader.schema.root.children
            .firstWhere((node) => node.name == 'var')
            .logicalType,
        isA<VariantLogicalType>(),
      );
    });
  });
}
