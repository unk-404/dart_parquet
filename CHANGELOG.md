# Changelog

## 0.1.0

- Initial read-only release.
- Pure Dart Thrift compact protocol, footer/metadata/schema/page-header parsing.
- Read support for flat and nested LIST/STRUCT/MAP schemas.
- Data pages v1/v2, dictionary pages, statistics metadata.
- Encodings: PLAIN, PLAIN_DICTIONARY/RLE_DICTIONARY, RLE, BIT_PACKED levels,
  DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY,
  BYTE_STREAM_SPLIT.
- Compression: uncompressed, Snappy, Gzip, LZO, Brotli, Zstandard, LZ4_RAW;
  legacy LZ4 (Hadoop framing) read.
- Columnar and row APIs; typed columns, zero-copy binary values, exact int64
  handling on the VM and web.
- I/O abstraction: memory, native file, blit OPFS, Blob/File slices and HTTP
  Range callbacks.
- Web-safe core; Chrome test for Snappy + int64 + nested reads.
