import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:parquet/src/util/int64.dart';
import 'package:test/test.dart';

ParquetTable _cdcTable() {
  final schema =
      (ParquetSchemaBuilder()
            ..addInt64('id')
            ..addBinary('_change_type', utf8: true)
            ..addInt64('_commit_version'))
          .build();
  final id = Int64Buffer(3)
    ..setInt(0, 1)
    ..setInt(1, 1)
    ..setInt(2, 2);
  final versions = Int64Buffer(3)
    ..setInt(0, 10)
    ..setInt(1, 11)
    ..setInt(2, 12);
  final data = Uint8List.fromList('insertdeleteupdate_postimage'.codeUnits);
  final offsets = Uint32List.fromList([0, 6, 12, 28]);
  return ParquetTable(
    schema: schema,
    columns: [
      Int64Column(name: 'id', values: id, validity: Validity(3)),
      BinaryColumn(
        name: '_change_type',
        data: data,
        offsets: offsets,
        validity: Validity(3),
        isUtf8: true,
      ),
      Int64Column(
        name: '_commit_version',
        values: versions,
        validity: Validity(3),
      ),
    ],
    numRows: 3,
  );
}

void main() {
  test('CDC markers and commit versions are typed', () {
    final cdc = CdcTable(_cdcTable());
    expect(cdc.changeType(0), ChangeType.insert);
    expect(cdc.changeType(1), ChangeType.delete);
    expect(cdc.changeType(2), ChangeType.updatePostimage);
    expect(cdc.commitVersion(0), 10);
    expect(cdc.commitVersion(2), 12);
    expect(cdc.commitTimestamp(0), isNull);
  });

  test('CDC parser accepts common aliases', () {
    expect(CdcTable.parseChangeType('append'), ChangeType.insert);
    expect(CdcTable.parseChangeType('REMOVE'), ChangeType.delete);
    expect(
      CdcTable.parseChangeType('update_before'),
      ChangeType.updatePreimage,
    );
    expect(CdcTable.parseChangeType('unknown'), isNull);
  });

  test('encrypted footer is a typed reserved feature', () {
    expect(ParquetFeature.encryption, isNotNull);
    expect(ParquetFeature.geospatial, isNotNull);
    final error = ParquetUnsupportedFeatureException(
      ParquetFeature.encryption,
      'PARE',
      path: 'footer',
    );
    expect(error, isA<ParquetUnsupportedException>());
    expect(error.toString(), contains('encryption'));
  });
}
