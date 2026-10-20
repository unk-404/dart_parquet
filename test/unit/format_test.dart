@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:parquet/src/format/footer.dart';
import 'package:parquet/src/format/page_header.dart';
import 'package:parquet/src/format/parquet_format.dart';
import 'package:parquet/src/format/thrift.dart';
import 'package:parquet/src/util/bytes.dart';
import 'package:parquet/src/util/errors.dart';
import 'package:test/test.dart';

Bytes fixtureBytes(String name) =>
    Bytes.fromUint8List(File('test/fixtures/$name').readAsBytesSync());

Bytes buildFile(
  Bytes metadata, {
  String startMagic = 'PAR1',
  String endMagic = 'PAR1',
  int? declaredFooterLength,
}) {
  final trailer = Uint8List(8);
  ByteData.sublistView(trailer)
      .setUint32(0, declaredFooterLength ?? metadata.length, Endian.little);
  trailer.setAll(4, endMagic.codeUnits);
  return Bytes.fromUint8List(
    Uint8List.fromList([
      ...startMagic.codeUnits,
      ...metadata.asUint8List(),
      ...trailer,
    ]),
  );
}

Bytes minimalMetadata() {
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: 1, // version
    2: TList(TCompactType.struct, <Object?>[]), // schema
    3: 0, // num_rows
    4: TList(TCompactType.struct, <Object?>[]), // row_groups
  });
  return writer.toBytes();
}

void main() {
  group('Thrift compact protocol', () {
    test('round-trips every wire type', () {
      final nested = <int, Object?>{
        1: 'nested',
        2: false,
        3: TList(TCompactType.i32, <Object?>[1, 2, 3]),
      };
      final original = <int, Object?>{
        1: -42,
        2: Uint8List.fromList([0, 1, 254, 255]),
        3: true,
        4: false,
        5: 3.25,
        6: TList(TCompactType.i64, <Object?>[0, -1, 1 << 40]),
        7: TMap(TCompactType.i64, TCompactType.boolTrue, {1: true, 2: false}),
        8: nested,
        9: TList(TCompactType.struct, <Object?>[
          <int, Object?>{1: 7},
          <int, Object?>{1: 8},
        ]),
        10: TList(TCompactType.boolTrue, <Object?>[true, false, true]),
        11: TList(TCompactType.i64, <Object?>[]),
        12: TMap(TCompactType.i64, TCompactType.i64, {}),
        20: 'jump', // delta > 15 => explicit field id
      };

      final writer = ThriftCompactWriter();
      writer.writeStruct(original);
      final encoded = writer.toBytes();
      final reader = ThriftCompactReader.fromBytes(encoded);
      final decoded = reader.readStruct();

      expect(decoded[1], -42);
      expect(decoded[2], [0, 1, 254, 255]);
      expect(decoded[3], isTrue);
      expect(decoded[4], isFalse);
      expect(decoded[5], 3.25);
      expect(decoded[6], [0, -1, 1 << 40]);
      expect(decoded[7], {1: true, 2: false});
      expect((decoded[8]! as Map<int, Object?>)[2], isFalse);
      expect((decoded[8]! as Map<int, Object?>)[1], utf8.encode('nested'));
      expect(decoded[9], [
        {1: 7},
        {1: 8},
      ]);
      expect(decoded[10], [true, false, true]);
      expect(decoded[11], isEmpty);
      expect(decoded[12], isEmpty);
      expect(utf8.decode(decoded[20]! as Uint8List), 'jump');
      expect(reader.position, encoded.length);
    });

    test('skips unknown fields with explicit ids', () {
      // field 20 (binary) then field 2 (binary), both with explicit ids.
      final bytes = Bytes.fromUint8List(
        Uint8List.fromList([
          0x08, 40, // header: explicit binary field 20
          3, 0x61, 0x62, 0x63, // "abc"
          0x08, 4, // header: explicit binary field 2
          2, 0x6f, 0x6b, // "ok"
          0x00, // STOP
        ]),
      );
      final decoded = ThriftCompactReader.fromBytes(bytes).readStruct();
      expect(decoded[20], [0x61, 0x62, 0x63]);
      expect(decoded[2], [0x6f, 0x6b]);
    });

    test('rejects truncated binary payloads', () {
      final bytes = Bytes.fromUint8List(
        Uint8List.fromList([
          0x08, 4, // explicit binary field 2
          100, // length 100, no payload
        ]),
      );
      expect(
        () => ThriftCompactReader.fromBytes(bytes).readStruct(),
        throwsA(isA<ParquetException>()),
      );
    });
  });

  group('Footer', () {
    test('parses a whole file footer', () {
      final bytes = fixtureBytes('plain_lz4.parquet');
      final footer = parseFooter(bytes);

      expect(footer.fileMetadata.version, 2);
      expect(footer.fileMetadata.numRows, 1000);
      expect(footer.fileMetadata.rowGroups, hasLength(1));
      expect(footer.fileMetadata.schema, hasLength(10));
      expect(footer.fileMetadata.createdBy, contains('parquet-cpp-arrow'));
      expect(footer.footerLength, 1762);
      expect(footer.metadataOffset, 27970);
      expect(footer.metadataEnd, bytes.length - 8);
      expect(readFooterLength(bytes), footer.footerLength);
      expect(isEncryptedFooter(bytes), isFalse);
    });

    test('parses an exact footer trailer in isolation', () {
      final bytes = fixtureBytes('plain_lz4.parquet');
      final whole = parseFooter(bytes);
      final tail = bytes.slice(whole.metadataOffset, bytes.length);
      final partial = parseFooter(tail);

      expect(partial.metadataOffset, 0);
      expect(partial.footerLength, whole.footerLength);
      expect(partial.fileMetadata.numRows, whole.fileMetadata.numRows);
      expect(partial.fileMetadata.schema, hasLength(10));
    });

    test('parseFileMetadata accepts the raw metadata bytes', () {
      final bytes = fixtureBytes('plain_lz4.parquet');
      final footer = parseFooter(bytes);
      final metadataBytes = bytes.slice(
        footer.metadataOffset,
        footer.metadataEnd,
      );
      final metadata = parseFileMetadata(metadataBytes);

      expect(metadata.numRows, 1000);
      expect(metadata.rowGroups, hasLength(1));
      expect(metadata.schema.first.name, 'schema');
      expect(parseFileFooter(bytes).fileMetadata.numRows, 1000);
    });

    test('round-trips handcrafted metadata', () {
      final metadataBytes = minimalMetadata();
      final file = buildFile(metadataBytes);
      final footer = parseFooter(file);

      expect(footer.fileMetadata.version, 1);
      expect(footer.fileMetadata.numRows, 0);
      expect(footer.fileMetadata.schema, isEmpty);
      expect(footer.fileMetadata.rowGroups, isEmpty);
      expect(footer.metadataOffset, 4);
    });

    test('rejects an encrypted footer (PARE)', () {
      final file = buildFile(minimalMetadata(), endMagic: 'PARE');
      expect(
        () => parseFooter(file),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('rejects an encrypted leading magic', () {
      final file = buildFile(minimalMetadata(), startMagic: 'PARE');
      expect(
        () => parseFooter(file),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });

    test('rejects bad trailing magic', () {
      final file = buildFile(minimalMetadata(), endMagic: 'XXXX');
      expect(() => parseFooter(file), throwsA(isA<ParquetFormatException>()));
    });

    test('rejects a buffer that is too short', () {
      final bytes = Bytes.fromUint8List(Uint8List.fromList([1, 2, 3, 4]));
      expect(() => parseFooter(bytes), throwsA(isA<ParquetFormatException>()));
    });

    test('rejects a footer length larger than the file', () {
      final file = buildFile(minimalMetadata(), declaredFooterLength: 9999);
      expect(() => parseFooter(file), throwsA(isA<ParquetFormatException>()));
    });

    test('rejects a truncated header', () {
      final metadata = minimalMetadata();
      final trailer = Uint8List(8);
      ByteData.sublistView(trailer)
          .setUint32(0, metadata.length, Endian.little);
      trailer.setAll(4, 'PAR1'.codeUnits);
      final file = Bytes.fromUint8List(
        Uint8List.fromList([
          0x50, // one byte of the leading magic only
          ...metadata.asUint8List(),
          ...trailer,
        ]),
      );
      expect(() => parseFooter(file), throwsA(isA<ParquetFormatException>()));
    });
  });

  group('Page headers', () {
    test('reads a v1 data page header and leaves the body unread', () {
      final bytes = fixtureBytes('plain_lz4.parquet');
      final footer = parseFooter(bytes);
      final columns = footer.fileMetadata.rowGroups.first.columns;
      final column = columns.first.metaData;
      expect(column.dataPageOffset, 4);

      final reader = ByteReader(bytes, position: 4);
      final header = readPageHeader(reader);

      expect(header.type, PageType.dataPage);
      expect(header.uncompressedPageSize, 8007);
      expect(header.compressedPageSize, 4157);
      expect(header.headerSize, 66);
      expect(header.bytesConsumed, 66);
      expect(header.dataPageHeader, isNotNull);
      expect(header.dataPageHeader!.numValues, 1000);
      expect(header.dataPageHeader!.encoding, Encoding.plain);
      expect(header.dataPageHeader!.definitionLevelEncoding, Encoding.rle);
      expect(header.dataPageHeader!.repetitionLevelEncoding, Encoding.rle);
      expect(header.dataPageHeader!.statistics, isNotNull);
      expect(header.dataPageHeader!.statistics!.nullCount, 0);
      expect(header.dictionaryPageHeader, isNull);
      expect(header.dataPageHeaderV2, isNull);

      // The reader stops exactly at the body, and header + body ends at the
      // next column chunk.
      expect(reader.position, 4 + header.headerSize);
      final body = reader.readSlice(header.compressedPageSize);
      expect(body.length, header.compressedPageSize);
      expect(reader.position, columns[1].metaData.dataPageOffset);
    });

    test('reads dictionary and v2 data page headers', () {
      final bytes = fixtureBytes('v2_pages.parquet');
      final footer = parseFooter(bytes);
      final columns = footer.fileMetadata.rowGroups.first.columns;
      final column = columns.first.metaData;
      expect(column.dictionaryPageOffset, 4);
      expect(column.dataPageOffset, 4062);

      final dictionary = readPageHeader(ByteReader(bytes, position: 4));
      expect(dictionary.type, PageType.dictionaryPage);
      expect(dictionary.uncompressedPageSize, 8000);
      expect(dictionary.compressedPageSize, 4041);
      expect(dictionary.headerSize, 17);
      expect(dictionary.dictionaryPageHeader, isNotNull);
      expect(dictionary.dictionaryPageHeader!.numValues, 1000);
      expect(dictionary.dictionaryPageHeader!.encoding, Encoding.plain);
      expect(dictionary.dictionaryPageHeader!.isSorted, isFalse);
      expect(
        4 + dictionary.headerSize + dictionary.compressedPageSize,
        column.dataPageOffset,
      );

      final reader = ByteReader(bytes, position: column.dataPageOffset!);
      final dataPage = readPageHeader(reader);
      expect(dataPage.type, PageType.dataPageV2);
      expect(dataPage.uncompressedPageSize, 1256);
      expect(dataPage.compressedPageSize, 1256);
      expect(dataPage.headerSize, 72);
      expect(dataPage.dataPageHeaderV2, isNotNull);
      final v2 = dataPage.dataPageHeaderV2!;
      expect(v2.numValues, 1000);
      expect(v2.numNulls, 0);
      expect(v2.numRows, 1000);
      expect(v2.encoding, Encoding.rleDictionary);
      expect(v2.definitionLevelsByteLength, 3);
      expect(v2.repetitionLevelsByteLength, 0);
      expect(v2.isCompressed, isFalse);
      expect(v2.statistics, isNotNull);
      expect(
        column.dataPageOffset! +
            dataPage.headerSize +
            dataPage.compressedPageSize,
        columns[1].metaData.dictionaryPageOffset,
      );
      expect(reader.position, column.dataPageOffset! + 72);
    });

    test('readPageHeaderWithSize reports the consumed bytes', () {
      final bytes = fixtureBytes('plain_uncompressed.parquet');
      final result = readPageHeaderWithSize(ByteReader(bytes, position: 4));

      expect(result.bytesConsumed, 66);
      expect(result.header.headerSize, 66);
      expect(result.header.type, PageType.dataPage);
      expect(result.header.uncompressedPageSize, 8007);
      expect(result.header.compressedPageSize, 8007);
      expect(result.header.dataPageHeader!.encoding, Encoding.plain);
    });

    test('rejects an unknown page type', () {
      final writer = ThriftCompactWriter();
      writer.writeStruct({1: 99, 2: 1, 3: 1});
      expect(
        () => readPageHeader(ByteReader(writer.toBytes())),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects a data page without its type-specific header', () {
      final writer = ThriftCompactWriter();
      writer.writeStruct({1: PageType.dataPage.value, 2: 1, 3: 1});
      expect(
        () => readPageHeader(ByteReader(writer.toBytes())),
        throwsA(isA<ParquetFormatException>()),
      );
    });

    test('rejects negative page sizes', () {
      final writer = ThriftCompactWriter();
      writer.writeStruct({
        1: PageType.dataPage.value,
        2: -1,
        3: 1,
        5: <int, Object?>{1: 1, 2: 0, 3: 3, 4: 3},
      });
      expect(
        () => readPageHeader(ByteReader(writer.toBytes())),
        throwsA(isA<ParquetFormatException>()),
      );
    });
  });
}
