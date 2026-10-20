/// Page-level decoding of one Parquet column chunk.
library;

import 'dart:typed_data';

import '../compression/codec.dart';
import '../compression/registry.dart';
import '../encodings/encoding.dart';
import '../encodings/levels.dart';
import '../encodings/rle.dart';
import '../format/page_header.dart';
import '../io/source.dart';
import '../metadata/metadata.dart';
import '../util/bytes.dart';
import '../util/crc32.dart';
import '../util/errors.dart';
import '../util/int64.dart';
import 'decoded_values.dart';

/// A decoded column chunk: levels for every value slot, plus compact values
/// (only entries with `definitionLevel == maxDefinitionLevel`).
final class DecodedColumnChunk {
  DecodedColumnChunk({
    required this.numValues,
    required this.presentCount,
    required this.repLevels,
    required this.defLevels,
    required this.values,
  });

  /// Number of level entries (including nulls/empty lists).
  final int numValues;

  /// Number of actual values stored in [values].
  final int presentCount;

  /// Repetition levels, length [numValues]; null when maxRepLevel == 0.
  final Uint8List? repLevels;

  /// Definition levels, length [numValues]; null when maxDefLevel == 0.
  final Uint8List? defLevels;

  /// Compact values; type depends on the physical type:
  /// Int32List, Int64Buffer, Float32List, Float64List, Uint8List (booleans,
  /// one byte per value), ByteArrayData or FixedLenData.
  final Object values;
}

/// Reads and decodes the pages of a single column chunk.
final class ColumnChunkReader {
  ColumnChunkReader(this.metadata, this.descriptor, {this.verifyCrc = false});

  final ColumnChunkMetaData metadata;
  final ColumnDescriptor descriptor;

  /// Verify `PageHeader.crc` against the compressed page body when present.
  final bool verifyCrc;

  int get _startOffset {
    final offset = metadata.dictionaryPageOffset ?? metadata.dataPageOffset;
    if (offset == null) {
      throw ParquetFormatException(
        'column ${descriptor.dotPath} has no data_page_offset',
      );
    }
    return offset;
  }

  /// An empty chunk for a zero-row row group.
  DecodedColumnChunk _emptyChunk() => DecodedColumnChunk(
    numValues: 0,
    presentCount: 0,
    repLevels: null,
    defLevels: null,
    values: _emptyValues(descriptor.physicalType, descriptor.typeLength),
  );

  /// Reads the whole compressed column chunk from [source] and decodes it.
  Future<DecodedColumnChunk> read(ParquetSource source) async {
    if (metadata.numValues == 0) return _emptyChunk();
    final start = _startOffset;
    final length = metadata.totalCompressedSize;
    if (length <= 0) {
      throw ParquetFormatException(
        'column ${descriptor.dotPath} has invalid compressed size $length',
      );
    }
    final chunk = await source.read(start, length);
    return decode(chunk);
  }

  /// Synchronous variant for [SyncParquetSource].
  DecodedColumnChunk readSync(SyncParquetSource source) {
    if (metadata.numValues == 0) return _emptyChunk();
    final start = _startOffset;
    final length = metadata.totalCompressedSize;
    if (length <= 0) {
      throw ParquetFormatException(
        'column ${descriptor.dotPath} has invalid compressed size $length',
      );
    }
    return decode(source.read(start, length));
  }

  /// Decodes a chunk whose bytes start at its first page header.
  DecodedColumnChunk decode(Bytes chunk) {
    final codec = codecFor(metadata.compression);
    final reader = ByteReader(chunk);
    Object? dictionary;
    final dictIndices = <Int32List>[];
    final valueChunks = <Object>[];
    final repChunks = <Uint8List>[];
    final defChunks = <Uint8List>[];
    var totalValues = 0;
    var presentCount = 0;
    var pageIndex = 0;

    while (reader.remaining > 0) {
      final header = readPageHeader(reader);
      if (header.compressedPageSize > reader.remaining) {
        throw ParquetEofException(
          'page $pageIndex declares ${header.compressedPageSize} bytes but '
          'only ${reader.remaining} remain in column ${descriptor.dotPath}',
        );
      }
      final body = reader.readSlice(header.compressedPageSize);
      if (verifyCrc && header.crc != null) {
        final expected = header.crc! & 0xFFFFFFFF;
        final actual = crc32(body.asUint8List());
        if (actual != expected) {
          throw ParquetFormatException(
            'CRC mismatch in page $pageIndex of ${descriptor.dotPath}: '
            'expected 0x${expected.toRadixString(16)}, '
            'got 0x${actual.toRadixString(16)}',
          );
        }
      }
      switch (header.type) {
        case PageType.dictionaryPage:
          if (dictionary != null) {
            throw ParquetFormatException(
              'column ${descriptor.dotPath} has multiple dictionary pages',
            );
          }
          final dictHeader = header.dictionaryPageHeader!;
          final raw = codec.decode(
            body,
            uncompressedSize: header.uncompressedPageSize,
          );
          if (dictHeader.encoding != Encoding.plain &&
              dictHeader.encoding != Encoding.plainDictionary) {
            throw ParquetUnsupportedException(
              'dictionary page encoding ${dictHeader.encoding} is not '
              'supported (only PLAIN/PLAIN_DICTIONARY)',
            );
          }
          dictionary = decodePlainValues(
            raw,
            physicalType: descriptor.physicalType,
            numValues: dictHeader.numValues,
            typeLength: descriptor.typeLength,
          );
        case PageType.dataPage:
          _decodeV1(
            header,
            body,
            codec,
            repChunks: repChunks,
            defChunks: defChunks,
            valueChunks: valueChunks,
            dictIndices: dictIndices,
            onCounts: (total, present) {
              totalValues += total;
              presentCount += present;
            },
          );
        case PageType.dataPageV2:
          _decodeV2(
            header,
            body,
            codec,
            repChunks: repChunks,
            defChunks: defChunks,
            valueChunks: valueChunks,
            dictIndices: dictIndices,
            onCounts: (total, present) {
              totalValues += total;
              presentCount += present;
            },
          );
        case PageType.indexPage:
          break;
      }
      pageIndex++;
    }

    if (dictIndices.isNotEmpty) {
      if (dictionary == null) {
        throw ParquetFormatException(
          'column ${descriptor.dotPath} uses dictionary indices but has no '
          'dictionary page',
        );
      }
      final indices = concatValueChunks(dictIndices) as Int32List;
      if (indices.length != presentCount) {
        throw ParquetFormatException(
          'column ${descriptor.dotPath} has ${indices.length} dictionary '
          'indices, expected $presentCount',
        );
      }
      valueChunks.add(materialiseDictionary(dictionary, indices));
    }

    final repLevels = repChunks.isEmpty ? null : _concatBytes(repChunks);
    final defLevels = defChunks.isEmpty ? null : _concatBytes(defChunks);
    if (defLevels != null && defLevels.length != totalValues) {
      throw ParquetFormatException(
        'definition levels length ${defLevels.length} != $totalValues',
      );
    }
    if (repLevels != null && repLevels.length != totalValues) {
      throw ParquetFormatException(
        'repetition levels length ${repLevels.length} != $totalValues',
      );
    }

    final values = valueChunks.isEmpty
        ? _emptyValues(descriptor.physicalType, descriptor.typeLength)
        : concatValueChunks(valueChunks);
    final actualPresent = _valueCount(values);
    if (actualPresent != presentCount) {
      throw ParquetFormatException(
        'column ${descriptor.dotPath} decoded $actualPresent values, '
        'expected $presentCount',
      );
    }
    return DecodedColumnChunk(
      numValues: totalValues,
      presentCount: presentCount,
      repLevels: repLevels,
      defLevels: defLevels,
      values: values,
    );
  }

  void _decodeV1(
    PageHeader header,
    Bytes body,
    CompressionCodec codec, {
    required List<Uint8List> repChunks,
    required List<Uint8List> defChunks,
    required List<Object> valueChunks,
    required List<Int32List> dictIndices,
    required void Function(int total, int present) onCounts,
  }) {
    final dataHeader = header.dataPageHeader!;
    final numValues = dataHeader.numValues;
    final raw = codec.decode(
      body,
      uncompressedSize: header.uncompressedPageSize,
    );
    final reader = ByteReader(raw);

    Uint8List? def;
    if (descriptor.maxRepLevel > 0) {
      final len = reader.readUint32LE();
      final levelBytes = reader.readSlice(len);
      repChunks.add(
        decodeLevels(
          levelBytes,
          maxLevel: descriptor.maxRepLevel,
          numValues: numValues,
          encoding: dataHeader.repetitionLevelEncoding,
        ),
      );
    }
    if (descriptor.maxDefLevel > 0) {
      final len = reader.readUint32LE();
      final levelBytes = reader.readSlice(len);
      def = decodeLevels(
        levelBytes,
        maxLevel: descriptor.maxDefLevel,
        numValues: numValues,
        encoding: dataHeader.definitionLevelEncoding,
      );
      defChunks.add(def);
    }
    final present = _presentCount(def, numValues);
    final valueBytes = reader.readSlice(reader.remaining);
    final decoded = _decodeValues(
      dataHeader.encoding,
      valueBytes,
      present,
      lengthPrefixedBoolean: true,
    );
    if (decoded is Int32List && isDictionaryEncoding(dataHeader.encoding)) {
      dictIndices.add(decoded);
    } else {
      valueChunks.add(decoded);
    }
    onCounts(numValues, present);
  }

  void _decodeV2(
    PageHeader header,
    Bytes body,
    CompressionCodec codec, {
    required List<Uint8List> repChunks,
    required List<Uint8List> defChunks,
    required List<Object> valueChunks,
    required List<Int32List> dictIndices,
    required void Function(int total, int present) onCounts,
  }) {
    final dataHeader = header.dataPageHeaderV2!;
    final numValues = dataHeader.numValues;
    final repLen = dataHeader.repetitionLevelsByteLength;
    final defLen = dataHeader.definitionLevelsByteLength;
    if (repLen + defLen > body.length) {
      throw ParquetFormatException(
        'DATA_PAGE_V2 levels exceed page body in ${descriptor.dotPath}',
      );
    }
    Uint8List? def;
    if (descriptor.maxRepLevel > 0) {
      repChunks.add(
        decodeLevels(
          body.slice(0, repLen),
          maxLevel: descriptor.maxRepLevel,
          numValues: numValues,
          encoding: Encoding.rle,
        ),
      );
    }
    if (descriptor.maxDefLevel > 0) {
      def = decodeLevels(
        body.slice(repLen, repLen + defLen),
        maxLevel: descriptor.maxDefLevel,
        numValues: numValues,
        encoding: Encoding.rle,
      );
      defChunks.add(def);
    }
    final valueBody = body.slice(repLen + defLen);
    final uncompressedValueSize = header.uncompressedPageSize - repLen - defLen;
    if (uncompressedValueSize < 0) {
      throw ParquetFormatException(
        'DATA_PAGE_V2 uncompressed size is smaller than its levels',
      );
    }
    // A valid empty v2 data page can have compressed_page_size=0. The spec
    // requires readers not to feed those bytes to the decompressor.
    final valueBytes = dataHeader.isCompressed && valueBody.isNotEmpty
        ? codec.decode(valueBody, uncompressedSize: uncompressedValueSize)
        : valueBody;
    final present = _presentCount(def, numValues);
    final decoded = _decodeValues(
      dataHeader.encoding,
      valueBytes,
      present,
      lengthPrefixedBoolean: true,
    );
    if (decoded is Int32List && isDictionaryEncoding(dataHeader.encoding)) {
      dictIndices.add(decoded);
    } else {
      valueChunks.add(decoded);
    }
    onCounts(numValues, present);
  }

  int _presentCount(Uint8List? def, int numValues) {
    if (descriptor.maxDefLevel == 0) return numValues;
    if (def == null) return numValues;
    var count = 0;
    final max = descriptor.maxDefLevel;
    for (final level in def) {
      if (level == max) count++;
    }
    return count;
  }

  Object _decodeValues(
    Encoding encoding,
    Bytes data,
    int count, {
    required bool lengthPrefixedBoolean,
  }) {
    switch (encoding) {
      case Encoding.plain:
        final decoded = decodePlainValues(
          data,
          physicalType: descriptor.physicalType,
          numValues: count,
          typeLength: descriptor.typeLength,
        );
        // decodePlainBoolean already returns one byte (0/1) per value.
        return decoded;
      case Encoding.plainDictionary:
      case Encoding.rleDictionary:
        return decodeDictionaryIndexValues(data, numValues: count);
      case Encoding.deltaBinaryPacked:
        return decodeDeltaBinaryPackedValues(
          data,
          physicalType: descriptor.physicalType,
          expectedCount: count,
        );
      case Encoding.deltaLengthByteArray:
      case Encoding.deltaByteArray:
        return decodeDeltaByteArrayValues(
          data,
          encoding: encoding,
          expectedCount: count,
        );
      case Encoding.byteStreamSplit:
        return decodeByteStreamSplitValues(
          data,
          physicalType: descriptor.physicalType,
          numValues: count,
          typeLength: descriptor.typeLength,
        );
      case Encoding.rle:
        if (descriptor.physicalType != PhysicalType.boolean) {
          throw ParquetUnsupportedException(
            'RLE values are only defined for BOOLEAN',
          );
        }
        final Bytes payload;
        if (lengthPrefixedBoolean) {
          if (data.length < 4) {
            throw ParquetEofException('truncated RLE boolean length prefix');
          }
          final len = ByteReader(data).readUint32LE();
          payload = data.slice(4, 4 + len);
        } else {
          payload = data;
        }
        final values = decodeRleBitPacked(
          payload,
          bitWidth: 1,
          numValues: count,
        );
        final out = Uint8List(count);
        for (var i = 0; i < count; i++) {
          out[i] = values[i] & 1;
        }
        return out;
      case Encoding.bitPacked:
        throw ParquetUnsupportedException('BIT_PACKED is not a value encoding');
      case Encoding.alp:
        throw ParquetUnsupportedException(
          'ALP encoding is not supported in v0.1.0',
        );
    }
  }
}

int _valueCount(Object values) {
  if (values is Int32List) return values.length;
  if (values is Int64Buffer) return values.length;
  if (values is Float32List) return values.length;
  if (values is Float64List) return values.length;
  if (values is Uint8List) return values.length;
  if (values is ByteArrayData) return values.numValues;
  if (values is FixedLenData) return values.numValues;
  return -1;
}

Object _emptyValues(PhysicalType type, int? typeLength) {
  switch (type) {
    case PhysicalType.boolean:
      return Uint8List(0);
    case PhysicalType.int32:
      return Int32List(0);
    case PhysicalType.int64:
      return Int64Buffer(0);
    case PhysicalType.int96:
      return FixedLenData(Uint8List(0), 12);
    case PhysicalType.float:
      return Float32List(0);
    case PhysicalType.double:
      return Float64List(0);
    case PhysicalType.byteArray:
      return ByteArrayData(Uint8List(0), Uint32List(1));
    case PhysicalType.fixedLenByteArray:
      return FixedLenData(Uint8List(0), typeLength ?? 1);
  }
}

Uint8List _concatBytes(List<Uint8List> chunks) {
  if (chunks.length == 1) return chunks.first;
  var total = 0;
  for (final c in chunks) {
    total += c.length;
  }
  final out = Uint8List(total);
  var pos = 0;
  for (final c in chunks) {
    out.setRange(pos, pos + c.length, c);
    pos += c.length;
  }
  return out;
}
