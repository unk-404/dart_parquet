/// v0.2 Web/Wasm feature-detection matrix.
///
/// Covers CDC typed access, encrypted-footer (`PARE`) detection and the
/// detection-only geospatial annotations. Shared with
/// `test/wasm/wasm_features_test.dart`.
@TestOn('browser')
library;

import 'dart:convert';
import 'dart:typed_data';

import 'package:parquet/parquet.dart';
// ignore: implementation_imports
import 'package:parquet/src/format/footer.dart';
// ignore: implementation_imports
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

ParquetTable _cdcTable() {
  final schema =
      (ParquetSchemaBuilder()
            ..addInt32('id')
            ..addBinary('value', utf8: true)
            ..addBinary('_change_type', utf8: true)
            ..addInt64('_commit_version')
            ..addInt64('_commit_timestamp'))
          .build();
  return ParquetTable(
    schema: schema,
    columns: <ParquetColumn>[
      Int32Column(
        name: 'id',
        values: Int32List.fromList(<int>[1, 2, 1, 1, 2, 3]),
        validity: Validity(6),
      ),
      _strings('value', <String?>['a', 'b', 'a-old', 'a-new', 'b-old', 'c']),
      _strings('_change_type', <String?>[
        'insert',
        'insert',
        'update_preimage',
        'update_postimage',
        'delete',
        'append',
      ]),
      _int64s('_commit_version', <int>[1, 1, 2, 2, 3, 4]),
      _int64s('_commit_timestamp', <int>[1000, 1000, 2000, 2000, 3000, 4000]),
    ],
    numRows: 6,
  );
}

BinaryColumn _strings(String name, List<String?> values) {
  final validity = Validity(values.length);
  final bytes = <int>[];
  final offsets = Uint32List(values.length + 1);
  for (var i = 0; i < values.length; i++) {
    final value = values[i];
    if (value == null) {
      validity.setNull(i);
    } else {
      bytes.addAll(utf8.encode(value));
    }
    offsets[i + 1] = bytes.length;
  }
  return BinaryColumn(
    name: name,
    data: Uint8List.fromList(bytes),
    offsets: offsets,
    validity: validity,
    isUtf8: true,
  );
}

Int64Column _int64s(String name, List<int> values) {
  final buffer = Int64Buffer(values.length);
  for (var i = 0; i < values.length; i++) {
    buffer.setInt(i, values[i]);
  }
  return Int64Column(
    name: name,
    values: buffer,
    validity: Validity(values.length),
  );
}

void main() {
  group('CDC typed access', () {
    test('reads change kind, commit version and timestamp', () {
      final table = ParquetReader.fromBytes(writeParquetTable(_cdcTable()))
          .readTableSync();
      final cdc = CdcTable(table);
      expect(cdc.numRows, 6);
      expect(cdc.hasChangeType, isTrue);
      expect(
        <ChangeType?>[
          for (var row = 0; row < cdc.numRows; row++) cdc.changeType(row),
        ],
        <ChangeType?>[
          ChangeType.insert,
          ChangeType.insert,
          ChangeType.updatePreimage,
          ChangeType.updatePostimage,
          ChangeType.delete,
          ChangeType.insert,
        ],
      );
      expect(
        <int?>[
          for (var row = 0; row < cdc.numRows; row++) cdc.commitVersion(row),
        ],
        <int?>[1, 1, 2, 2, 3, 4],
      );
      expect(
        <int?>[
          for (var row = 0; row < cdc.numRows; row++) cdc.commitTimestamp(row),
        ],
        <int?>[1000, 1000, 2000, 2000, 3000, 4000],
      );
    });

    test('custom column names and missing columns degrade to null', () {
      final table = ParquetReader.fromBytes(writeParquetTable(_cdcTable()))
          .readTableSync();
      final custom = CdcTable(
        table,
        changeTypeColumn: 'value',
        commitVersionColumn: 'missing_version',
      );
      expect(custom.changeType(0), isNull);
      expect(custom.commitVersion(0), isNull);
      expect(custom.commitTimestamp(0), 1000);

      final noCdc = CdcTable(table, changeTypeColumn: 'missing');
      expect(noCdc.hasChangeType, isFalse);
      expect(noCdc.changeType(0), isNull);
    });

    test('parses the common CDC aliases', () {
      expect(CdcTable.parseChangeType('INSERT'), ChangeType.insert);
      expect(CdcTable.parseChangeType('append'), ChangeType.insert);
      expect(
        CdcTable.parseChangeType('update_before'),
        ChangeType.updatePreimage,
      );
      expect(CdcTable.parseChangeType('u_after'), ChangeType.updatePostimage);
      expect(CdcTable.parseChangeType(' delete '), ChangeType.delete);
      expect(CdcTable.parseChangeType('unknown'), isNull);
      expect(CdcTable.parseChangeType(42), isNull);
    });
  });

  group('encrypted footer detection', () {
    test('footer parser reports the typed encryption feature', () {
      // A minimal exact trailer: zero footer length + trailing PARE magic.
      final bytes = Bytes.fromUint8List(
        Uint8List.fromList(<int>[
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0x50,
          0x41,
          0x52,
          0x45,
        ]),
      );
      expect(isEncryptedFooter(bytes), isTrue);
      expect(
        () => parseFooter(bytes),
        throwsA(
          isA<ParquetUnsupportedFeatureException>()
              .having(
                (error) => error.feature,
                'feature',
                ParquetFeature.encryption,
              )
              .having(
                (error) => error.toString(),
                'message',
                contains('encrypted footer'),
              ),
        ),
      );
    });

    test('public reader rejects a PARE file with a typed exception base', () {
      final bytes = Uint8List.fromList(<int>[
        0x50,
        0x41,
        0x52,
        0x45,
        0,
        0,
        0,
        0,
      ]);
      // The reader validates the leading magic first and reports the base
      // ParquetUnsupportedException; the footer layer above carries the
      // ParquetFeature.encryption detail.
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
  });

  group('geospatial detection-only', () {
    test(
      'geometry/geography annotations and WKB bytes survive a round-trip',
      () {
        final schema = ParquetSchema.fromSchemaElements(const <SchemaElement>[
          SchemaElement(name: 'schema', numChildren: 2),
          SchemaElement(
            name: 'geo',
            type: PhysicalType.byteArray,
            repetitionType: FieldRepetitionType.optional,
            logicalType: GeometryLogicalType('OGC:CRS84'),
          ),
          SchemaElement(
            name: 'geog',
            type: PhysicalType.byteArray,
            repetitionType: FieldRepetitionType.optional,
            logicalType: GeographyLogicalType(
              'OGC:CRS84',
              EdgeInterpolationAlgorithm.spherical,
            ),
          ),
        ]);
        final geoBytes = Uint8List.fromList(<int>[1, 2, 3, 4, 5, 6]);
        final geogBytes = Uint8List.fromList(<int>[7, 8, 9, 10]);
        final table = ParquetTable(
          schema: schema,
          columns: <ParquetColumn>[
            BinaryColumn(
              name: 'geo',
              data: geoBytes,
              offsets: Uint32List.fromList(<int>[0, 6, 6]),
              validity: Validity(2)..setNull(1),
            ),
            BinaryColumn(
              name: 'geog',
              data: geogBytes,
              offsets: Uint32List.fromList(<int>[0, 4, 4]),
              validity: Validity(2)..setNull(1),
            ),
          ],
          numRows: 2,
        );

        final reader = ParquetReader.fromBytes(writeParquetTable(table));
        final nodes = reader.schema.root.children;
        final geoType = nodes[0].logicalType;
        final geogType = nodes[1].logicalType;
        expect(geoType, isA<GeometryLogicalType>());
        expect((geoType! as GeometryLogicalType).crs, 'OGC:CRS84');
        expect(geogType, isA<GeographyLogicalType>());
        expect((geogType! as GeographyLogicalType).crs, 'OGC:CRS84');
        expect(
          (geogType as GeographyLogicalType).algorithm,
          EdgeInterpolationAlgorithm.spherical,
        );

        final output = reader.readTableSync();
        expect((output['geo'] as BinaryColumn).getBytes(0), geoBytes);
        expect((output['geo'] as BinaryColumn).isNull(1), isTrue);
        expect((output['geog'] as BinaryColumn).getBytes(0), geogBytes);
        expect((output['geog'] as BinaryColumn).isNull(1), isTrue);
      },
    );

    test('reserved feature enum is exported', () {
      expect(
        ParquetFeature.values.map((feature) => feature.name).toSet(),
        <String>{'encryption', 'geospatial', 'variant', 'alp', 'cdc'},
      );
    });
  });
}
