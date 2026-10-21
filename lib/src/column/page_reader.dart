/// Page-level decoding of one Parquet column chunk.
library;

import 'dart:typed_data';

import '../compression/registry.dart';
import '../encodings/alp.dart';
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

/// A page header plus its zero-copy body view.
final class _ScannedPage {
  _ScannedPage(this.header, this.body);

  final PageHeader header;
  final Bytes body;
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
  ///
  /// Multi-page flat chunks (`maxRepLevel == 0`) decode PLAIN and dictionary
  /// values into one chunk-level buffer; level arrays for every multi-page
  /// chunk are also decoded into one pre-sized buffer. Other shapes (nested,
  /// DELTA, BYTE_STREAM_SPLIT, ALP, PLAIN BOOLEAN, mixed encodings) keep the
  /// page-by-page path and still produce identical [DecodedColumnChunk] data.
  DecodedColumnChunk decode(Bytes chunk) {
    final pages = _scanPages(chunk);
    var dataPageCount = 0;
    var totalEntries = 0;
    var valueByteCapacity = 0;
    for (final page in pages) {
      switch (page.header.type) {
        case PageType.dataPage:
          final data = page.header.dataPageHeader!;
          dataPageCount++;
          totalEntries += data.numValues;
          valueByteCapacity += page.header.uncompressedPageSize;
        case PageType.dataPageV2:
          final data = page.header.dataPageHeaderV2!;
          dataPageCount++;
          totalEntries += data.numValues;
          final levels =
              data.repetitionLevelsByteLength + data.definitionLevelsByteLength;
          final valueBytes = page.header.uncompressedPageSize - levels;
          if (valueBytes > 0) valueByteCapacity += valueBytes;
        case PageType.dictionaryPage:
        case PageType.indexPage:
          break;
      }
    }

    final hasRep = descriptor.maxRepLevel > 0;
    final hasDef = descriptor.maxDefLevel > 0;
    // Single-page chunks are already exact: decoding levels/values once and
    // returning them avoids allocating a second pre-sized buffer.
    final preallocateLevels =
        dataPageCount > 1 && totalEntries > 0 && (hasRep || hasDef);
    final repLevels = preallocateLevels && hasRep
        ? Uint8List(totalEntries)
        : null;
    final defLevels = preallocateLevels && hasDef
        ? Uint8List(totalEntries)
        : null;
    final repChunks = <Uint8List>[];
    final defChunks = <Uint8List>[];
    final flatWriter = _newFlatValueWriter(
      pages,
      dataPageCount,
      totalEntries,
      valueByteCapacity,
    );

    final codec = codecFor(metadata.compression);
    Object? dictionary;
    final dictIndices = <Int32List>[];
    final valueChunks = <Object>[];
    var totalValues = 0;
    var presentCount = 0;
    var entryOffset = 0;
    var pageIndex = 0;

    for (final page in pages) {
      final header = page.header;
      final body = page.body;
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
          flatWriter?.setDictionary(dictionary);
        case PageType.dataPage:
          final dataHeader = header.dataPageHeader!;
          final numValues = dataHeader.numValues;
          final raw = codec.decode(
            body,
            uncompressedSize: header.uncompressedPageSize,
          );
          final reader = ByteReader(raw);
          final pageRep = hasRep
              ? _readV1Levels(
                  reader,
                  descriptor.maxRepLevel,
                  numValues,
                  dataHeader.repetitionLevelEncoding,
                )
              : null;
          final pageDef = hasDef
              ? _readV1Levels(
                  reader,
                  descriptor.maxDefLevel,
                  numValues,
                  dataHeader.definitionLevelEncoding,
                )
              : null;
          _storeLevels(pageRep, repLevels, repChunks, entryOffset);
          _storeLevels(pageDef, defLevels, defChunks, entryOffset);
          final present = hasDef
              ? _presentCount(pageDef, numValues)
              : numValues;
          final valueBytes = reader.readSlice(reader.remaining);
          _storeValues(
            flatWriter,
            dataHeader.encoding,
            valueBytes,
            present,
            dictIndices,
            valueChunks,
          );
          totalValues += numValues;
          presentCount += present;
          entryOffset += numValues;
        case PageType.dataPageV2:
          final dataHeader = header.dataPageHeaderV2!;
          final numValues = dataHeader.numValues;
          final repLength = dataHeader.repetitionLevelsByteLength;
          final defLength = dataHeader.definitionLevelsByteLength;
          if (repLength + defLength > body.length) {
            throw ParquetFormatException(
              'DATA_PAGE_V2 levels exceed page body in ${descriptor.dotPath}',
            );
          }
          final pageRep = hasRep
              ? decodeLevels(
                  body.slice(0, repLength),
                  maxLevel: descriptor.maxRepLevel,
                  numValues: numValues,
                  encoding: Encoding.rle,
                )
              : null;
          final pageDef = hasDef
              ? decodeLevels(
                  body.slice(repLength, repLength + defLength),
                  maxLevel: descriptor.maxDefLevel,
                  numValues: numValues,
                  encoding: Encoding.rle,
                )
              : null;
          _storeLevels(pageRep, repLevels, repChunks, entryOffset);
          _storeLevels(pageDef, defLevels, defChunks, entryOffset);
          final present = hasDef
              ? _presentCount(pageDef, numValues)
              : numValues;
          final valueBody = body.slice(repLength + defLength);
          final uncompressedValueSize =
              header.uncompressedPageSize - repLength - defLength;
          if (uncompressedValueSize < 0) {
            throw ParquetFormatException(
              'DATA_PAGE_V2 uncompressed size is smaller than its levels',
            );
          }
          // A valid empty v2 data page can have compressed_page_size=0. The
          // spec requires readers not to feed those bytes to the decompressor.
          final valueBytes = dataHeader.isCompressed && valueBody.isNotEmpty
              ? codec.decode(valueBody, uncompressedSize: uncompressedValueSize)
              : valueBody;
          _storeValues(
            flatWriter,
            dataHeader.encoding,
            valueBytes,
            present,
            dictIndices,
            valueChunks,
          );
          totalValues += numValues;
          presentCount += present;
          entryOffset += numValues;
        case PageType.indexPage:
          break;
      }
      pageIndex++;
    }

    final finalRepLevels = hasRep
        ? (repLevels ?? (repChunks.isEmpty ? null : _concatBytes(repChunks)))
        : null;
    final finalDefLevels = hasDef
        ? (defLevels ?? (defChunks.isEmpty ? null : _concatBytes(defChunks)))
        : null;

    final Object values;
    if (flatWriter != null) {
      values = flatWriter.finish(presentCount);
    } else if (dictIndices.isNotEmpty) {
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
      values = concatValueChunks(valueChunks);
    } else {
      values = valueChunks.isEmpty
          ? _emptyValues(descriptor.physicalType, descriptor.typeLength)
          : concatValueChunks(valueChunks);
    }

    if (finalDefLevels != null && finalDefLevels.length != totalValues) {
      throw ParquetFormatException(
        'definition levels length ${finalDefLevels.length} != $totalValues',
      );
    }
    if (finalRepLevels != null && finalRepLevels.length != totalValues) {
      throw ParquetFormatException(
        'repetition levels length ${finalRepLevels.length} != $totalValues',
      );
    }

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
      repLevels: finalRepLevels,
      defLevels: finalDefLevels,
      values: values,
    );
  }

  /// Parses every page header once and keeps zero-copy body views.
  ///
  /// The scan lets the decoder size output buffers by the chunk's total entry
  /// count and decide, before decoding any body, whether the uniform fast path
  /// applies.
  List<_ScannedPage> _scanPages(Bytes chunk) {
    final pages = <_ScannedPage>[];
    final reader = ByteReader(chunk);
    var pageIndex = 0;
    while (reader.remaining > 0) {
      final header = readPageHeader(reader);
      if (header.compressedPageSize > reader.remaining) {
        throw ParquetEofException(
          'page $pageIndex declares ${header.compressedPageSize} bytes but '
          'only ${reader.remaining} remain in column ${descriptor.dotPath}',
        );
      }
      pages.add(
        _ScannedPage(header, reader.readSlice(header.compressedPageSize)),
      );
      pageIndex++;
    }
    return pages;
  }

  /// Returns a writer when at least two data pages share one supported
  /// encoding; single-page and mixed/unsupported chunks keep the exact
  /// page-by-page path.
  FlatValueWriter? _newFlatValueWriter(
    List<_ScannedPage> pages,
    int dataPageCount,
    int totalEntries,
    int valueByteCapacity,
  ) {
    if (dataPageCount < 2 || totalEntries <= 0) return null;
    if (descriptor.maxRepLevel != 0) return null;

    Encoding? encoding;
    for (final page in pages) {
      final pageEncoding = switch (page.header.type) {
        PageType.dataPage => page.header.dataPageHeader!.encoding,
        PageType.dataPageV2 => page.header.dataPageHeaderV2!.encoding,
        _ => null,
      };
      if (pageEncoding == null) continue;
      if (encoding == null) {
        encoding = pageEncoding;
      } else if (encoding != pageEncoding) {
        return null;
      }
    }
    if (encoding == null) return null;

    if (encoding == Encoding.plain) {
      switch (descriptor.physicalType) {
        // Fixed-width PLAIN pages decode to zero-copy views into the codec
        // output; `concatValueChunks` already copies them exactly once, so
        // preallocating here would add an allocation/copy instead of removing
        // one. Only encodings whose per-page decoders allocate are profitable.
        case PhysicalType.byteArray:
        case PhysicalType.fixedLenByteArray:
        case PhysicalType.int96:
          return FlatValueWriter(
            descriptor,
            totalEntries,
            byteCapacity: valueByteCapacity,
          );
        case PhysicalType.int32:
        case PhysicalType.int64:
        case PhysicalType.float:
        case PhysicalType.double:
        case PhysicalType.boolean:
          return null;
      }
    }
    if (isDictionaryEncoding(encoding)) {
      if (descriptor.physicalType == PhysicalType.boolean) return null;
      return FlatValueWriter(
        descriptor,
        totalEntries,
        byteCapacity: valueByteCapacity,
      );
    }
    return null;
  }

  Uint8List? _readV1Levels(
    ByteReader reader,
    int maxLevel,
    int numValues,
    Encoding encoding,
  ) {
    final length = reader.readUint32LE();
    final levelBytes = reader.readSlice(length);
    return decodeLevels(
      levelBytes,
      maxLevel: maxLevel,
      numValues: numValues,
      encoding: encoding,
    );
  }

  void _storeLevels(
    Uint8List? pageLevels,
    Uint8List? preallocated,
    List<Uint8List> chunks,
    int offset,
  ) {
    if (pageLevels == null) return;
    if (preallocated != null) {
      preallocated.setRange(offset, offset + pageLevels.length, pageLevels);
    } else {
      chunks.add(pageLevels);
    }
  }

  void _storeValues(
    FlatValueWriter? writer,
    Encoding encoding,
    Bytes valueBytes,
    int present,
    List<Int32List> dictIndices,
    List<Object> valueChunks,
  ) {
    if (writer != null) {
      if (isDictionaryEncoding(encoding)) {
        writer.writeDictionaryIndices(valueBytes, present);
      } else {
        writer.writePlain(valueBytes, present);
      }
      return;
    }
    final decoded = _decodeValues(
      encoding,
      valueBytes,
      present,
      lengthPrefixedBoolean: true,
    );
    if (decoded is Int32List && isDictionaryEncoding(encoding)) {
      dictIndices.add(decoded);
    } else {
      valueChunks.add(decoded);
    }
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
        return decodeAlp(data, type: descriptor.physicalType, numValues: count);
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
  var position = 0;
  for (final c in chunks) {
    out.setRange(position, position + c.length, c);
    position += c.length;
  }
  return out;
}
