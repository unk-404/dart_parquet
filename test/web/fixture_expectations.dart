/// Expected values for the tiny PyArrow-generated Web/Wasm fixtures.
///
/// This file is shared by `test/web/` and `test/wasm/`: it contains no
/// `dart:io` imports so it compiles for both dart2js and dart2wasm.
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
// ignore: implementation_imports
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:test/test.dart';

/// Expected `BigInt` values of the flat fixture's INT64 column.
final List<BigInt> expectedInt64 = <BigInt>[
  BigInt.zero,
  BigInt.one,
  -BigInt.one,
  BigInt.from(42),
  BigInt.from(-2),
  BigInt.parse('9007199254740992'), // 2^53
  BigInt.parse('9007199254740993'), // 2^53 + 1
  BigInt.parse('-9223372036854775808'), // int64 min
];

/// Expected rows of the flat fixture (8 rows, 8 columns).
const List<String> flatColumnNames = <String>[
  'i64',
  'i32',
  'f64',
  'f32',
  'b',
  's',
  'bin',
  'fixed4',
];

/// Asserts every value of the flat fixture in [table].
void expectFlatTable(ParquetTable table) {
  expect(table.numRows, 8);
  expect(table.columns.map((c) => c.name).toList(), flatColumnNames);

  final i64 = table['i64'] as Int64Column;
  for (var i = 0; i < expectedInt64.length; i++) {
    expect(i64.getBigInt(i), expectedInt64[i], reason: 'i64[$i]');
  }

  final i32 = table['i32'] as Int32Column;
  expect(i32.isNull(0), isFalse);
  expect(i32.getInt(0), 0);
  expect(i32.isNull(1), isTrue);
  expect(i32.getInt(2), 2);
  expect(i32.getInt(3), 3);
  expect(i32.isNull(4), isTrue);
  expect(i32.getInt(5), 5);
  expect(i32.getInt(6), 6);
  expect(i32.getInt(7), -7);

  final f64 = table['f64'] as Float64Column;
  expect(f64.getDouble(0), 0.0);
  expect(f64.getDouble(1), 1.5);
  expect(f64.isNull(2), isTrue);
  expect(f64.getDouble(3), -2.25);
  expect(f64.getDouble(4), 3.75);
  expect(f64.getDouble(5), 5.0);
  expect(f64.isNull(6), isTrue);
  expect(f64.getDouble(7), -7.5);

  final f32 = table['f32'] as Float32Column;
  expect(f32.getDouble(0), 0.5);
  expect(f32.getDouble(1), 1.5);
  expect(f32.getDouble(2), 2.5);
  expect(f32.isNull(3), isTrue);
  expect(f32.getDouble(4), 4.5);
  expect(f32.getDouble(5), 5.5);
  expect(f32.getDouble(6), 6.5);
  expect(f32.getDouble(7), -7.5);

  final b = table['b'] as BooleanColumn;
  expect(b.getBool(0), isTrue);
  expect(b.getBool(1), isFalse);
  expect(b.isNull(2), isTrue);
  expect(b.getBool(3), isTrue);
  expect(b.getBool(4), isFalse);
  expect(b.isNull(5), isTrue);
  expect(b.getBool(6), isTrue);
  expect(b.getBool(7), isFalse);

  final s = table['s'] as BinaryColumn;
  expect(
    <String?>[for (var i = 0; i < 8; i++) s.isNull(i) ? null : s.getString(i)],
    <String?>['alpha', 'beta', '', 'delta', null, 'zeta', 'eta', 'theta'],
  );

  final bin = table['bin'] as BinaryColumn;
  expect(bin.isNull(4), isTrue);
  expect(bin.getBytes(0), <int>[0, 1]);
  expect(bin.getBytes(1), <int>[255]);
  expect(bin.getBytes(2), isEmpty);
  expect(bin.getBytes(3), <int>[1, 2, 3]);
  expect(bin.getBytes(5), <int>[127]);
  expect(bin.getBytes(6), <int>[128, 129]);
  expect(bin.getBytes(7), List<int>.generate(16, (i) => i));

  final fixed = table['fixed4'] as FixedLenColumn;
  expect(fixed.width, 4);
  expect(fixed.bytesAt(0).asUint8List(), 'abcd'.codeUnits);
  expect(fixed.bytesAt(1).asUint8List(), 'efgh'.codeUnits);
  expect(fixed.isNull(2), isTrue);
  expect(fixed.bytesAt(3).asUint8List(), 'ijkl'.codeUnits);
  expect(fixed.bytesAt(7).asUint8List(), 'yz01'.codeUnits);
}

/// Expected rows of the nested fixture (6 rows, list/struct/map).
void expectNestedTable(ParquetTable table) {
  expect(table.numRows, 6);
  expect(table.columns.map((c) => c.name).toList(), <String>[
    'id',
    'lst',
    'st',
    'mp',
  ]);

  final id = table['id'] as Int32Column;
  for (var i = 0; i < 6; i++) {
    expect(id.getInt(i), i);
  }

  final lst = table['lst'] as ListColumn;
  expect(
    <List<Object?>>[for (var i = 0; i < 6; i++) lst.getList(i)],
    <List<Object?>>[
      <Object?>[],
      <Object?>[10],
      <Object?>[20, 21],
      <Object?>[],
      <Object?>[30, 31, 32],
      <Object?>[],
    ],
  );

  final st = table['st'] as StructColumn;
  expect(st.isNull(0), isTrue);
  expect(st.getStruct(1), <String, Object?>{'a': 1, 'b': 's1'});
  expect(st.getStruct(2), <String, Object?>{'a': 2, 'b': null});
  expect(st.isNull(3), isTrue);
  expect(st.getStruct(4), <String, Object?>{'a': 4, 'b': 's4'});
  expect(st.getStruct(5), <String, Object?>{'a': 5, 'b': 's5'});

  final mp = table['mp'] as MapColumn;
  expect(mp.getMap(0), <Object?, Object?>{});
  expect(mp.getMap(1), <Object?, Object?>{'k0': 0});
  expect(mp.getMap(2), <Object?, Object?>{'k0': 0, 'k1': 1});
  expect(mp.getMap(3), <Object?, Object?>{});
  expect(mp.getMap(4), <Object?, Object?>{'x': 9});
  expect(mp.getMap(5), <Object?, Object?>{});
}

/// Expected values of the 3-row-group fixture (12 rows).
void expectMultiRowGroupTable(ParquetTable table) {
  expect(table.numRows, 12);
  final id = table['id'] as Int64Column;
  final s = table['s'] as BinaryColumn;
  final flag = table['flag'] as BooleanColumn;
  for (var i = 0; i < 12; i++) {
    expect(id.getInt(i), i, reason: 'id[$i]');
    expect(s.getString(i), 'row-${i.toString().padLeft(2, '0')}');
    expect(flag.getBool(i), i.isEven, reason: 'flag[$i]');
  }
}

/// Asserts that the first data page in [columnPath] is a `DATA_PAGE` v1 or a
/// `DATA_PAGE_V2` v2 header.
void expectFirstDataPageVersion(
  Uint8List bytes,
  ParquetReader reader,
  List<String> columnPath,
  int version,
) {
  final chunk = reader.metadata.rowGroup(0).columnByPath(columnPath)!;
  final start = chunk.dictionaryPageOffset ?? chunk.dataPageOffset!;
  final dataOffset = chunk.dataPageOffset!;
  final end = start + chunk.totalCompressedSize;
  final input = ByteReader(
    MemorySource.fromUint8List(bytes).read(dataOffset, end - dataOffset),
  );
  final result = readPageHeaderWithSize(input);
  expect(
    result.header.type,
    version == 1 ? PageType.dataPage : PageType.dataPageV2,
    reason: columnPath.join('.'),
  );
  expect(result.bytesConsumed, greaterThan(0));
  if (version == 1) {
    expect(result.header.dataPageHeader, isNotNull);
  } else {
    expect(result.header.dataPageHeaderV2, isNotNull);
    expect(result.header.dataPageHeaderV2!.numRows, greaterThan(0));
  }
}

/// Returns the absolute offset of the first byte of the first data page body.
///
/// Used by the CRC test to corrupt exactly one body byte while leaving the
/// header parseable.
int firstDataPageBodyOffset(
  Uint8List bytes,
  ParquetReader reader,
  List<String> columnPath,
) {
  final chunk = reader.metadata.rowGroup(0).columnByPath(columnPath)!;
  final dataOffset = chunk.dataPageOffset!;
  final start = chunk.dictionaryPageOffset ?? dataOffset;
  final end = start + chunk.totalCompressedSize;
  final input = ByteReader(
    MemorySource.fromUint8List(bytes).read(dataOffset, end - dataOffset),
  );
  final result = readPageHeaderWithSize(input);
  return dataOffset + result.bytesConsumed;
}

/// Every codec a v0.1.2 writer can emit (used by the writer round-trip matrix).
const List<CompressionType> allCompressionTypes = <CompressionType>[
  CompressionType.uncompressed,
  CompressionType.snappy,
  CompressionType.gzip,
  CompressionType.lzo,
  CompressionType.brotli,
  CompressionType.lz4,
  CompressionType.zstd,
  CompressionType.lz4Raw,
];
