/// Reading Parquet page headers.
library;

import '../util/bytes.dart';
import '../util/errors.dart';
import 'parquet_format.dart';
import 'thrift.dart';

/// A page header together with the number of bytes it occupied.
final class PageHeaderReadResult {
  /// Creates a result.
  const PageHeaderReadResult(this.header, this.bytesConsumed);

  /// Parsed header.
  final PageHeader header;

  /// Bytes consumed by the serialised header (the body follows immediately).
  final int bytesConsumed;
}

/// Reads one [PageHeader] from the current position of [reader].
///
/// The reader advances to the first byte of the page body; the body itself is
/// never read here. The returned header records the consumed byte count in
/// [PageHeader.headerSize].
PageHeader readPageHeader(ByteReader reader) {
  final start = reader.position;
  final thriftReader = ThriftCompactReader(reader);
  final fields = thriftReader.readStruct();
  final headerSize = reader.position - start;
  final header = PageHeader.fromThrift(fields, headerSize: headerSize);
  _validatePageHeader(header);
  return header;
}

/// Like [readPageHeader], but returns the header and consumed size explicitly.
PageHeaderReadResult readPageHeaderWithSize(ByteReader reader) {
  final start = reader.position;
  final header = readPageHeader(reader);
  return PageHeaderReadResult(header, reader.position - start);
}

void _validatePageHeader(PageHeader header) {
  switch (header.type) {
    case PageType.dataPage:
      final dataPage = header.dataPageHeader;
      if (dataPage == null) {
        throw ParquetFormatException(
          'DATA_PAGE header is missing data_page_header',
        );
      }
      if (dataPage.numValues < 0) {
        throw ParquetFormatException(
          'DATA_PAGE has negative num_values: ${dataPage.numValues}',
        );
      }
    case PageType.dictionaryPage:
      final dictionaryPage = header.dictionaryPageHeader;
      if (dictionaryPage == null) {
        throw ParquetFormatException(
          'DICTIONARY_PAGE header is missing dictionary_page_header',
        );
      }
      if (dictionaryPage.numValues < 0) {
        throw ParquetFormatException(
          'DICTIONARY_PAGE has negative num_values: '
          '${dictionaryPage.numValues}',
        );
      }
    case PageType.dataPageV2:
      final dataPage = header.dataPageHeaderV2;
      if (dataPage == null) {
        throw ParquetFormatException(
          'DATA_PAGE_V2 header is missing data_page_header_v2',
        );
      }
      if (dataPage.numValues < 0 ||
          dataPage.numNulls < 0 ||
          dataPage.numRows < 0 ||
          dataPage.definitionLevelsByteLength < 0 ||
          dataPage.repetitionLevelsByteLength < 0) {
        throw ParquetFormatException('DATA_PAGE_V2 has negative counts');
      }
      if (dataPage.numNulls > dataPage.numValues) {
        throw ParquetFormatException(
          'DATA_PAGE_V2 num_nulls (${dataPage.numNulls}) exceeds '
          'num_values (${dataPage.numValues})',
        );
      }
    case PageType.indexPage:
      break;
  }
}
