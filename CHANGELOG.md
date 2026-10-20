# Changelog

## 0.1.1

- **Writer**: flat and nested LIST/STRUCT/MAP, all physical/logical types,
  PLAIN + dictionary (with fallback), RLE levels, data pages v1/v2, page/chunk
  statistics, all 8 codecs, multiple row groups, empty files.
- Writer APIs: `ParquetWriterOptions`, streaming `ParquetWriter.add`,
  `writeParquetTable`, flat `ParquetSchemaBuilder`, and native/OPFS output via
  `writeParquetTableToFile` / `writeParquetTableToBytesFile`.
- **Reader**: dotted-path nested projection (struct/list/map), statistics-based
  `rowGroupFilter`, lazy per-row-group `readRowGroups` streaming, and
  OffsetIndex/ColumnIndex parsing.
- **Parallel reads**: async `readTable` on memory sources uses up to 8 isolates
  (`parallelism: auto|1|N`); sync APIs stay on the calling isolate; Web stays
  single-threaded.
- Rust arrow-rs benchmark counterpart (`benchmark/rust`, pinned
  `parquet = "=60.0.0"`) matching every Dart read/write operation, plus opt-in
  multi-thread read rows; benchcat chart now compares JIT, AOT and Rust.
- Chrome tests cover Blob, IndexedDB and OPFS writer roundtrips.

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
- Web-safe core; Chrome tests for Snappy + int64 + nested reads, Blob and
  IndexedDB sources.
- blit 0.1.6 Web backends: `openBlobBytesFile` for read-only `Blob`/`File`
  sources and `openIdbBytesFile` for IndexedDB (`idb://`).
- Fixed dart2js `Int64Buffer.getInt()` for negative values (two's-complement
  limb arithmetic instead of a double subtraction that rounded `-2` to `0`).
