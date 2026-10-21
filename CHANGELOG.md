# Changelog

## 0.2.0

- Full arbitrary Dremel nesting: list of list/map, map of list/map, struct in
  list/map, bare repeated leaf/group, legacy two-level LIST rules, MAP without
  value and MAP_KEY_VALUE.
- VARIANT: binary metadata/value codec, JSON conversion, unshredded write, and
  shredded read reconstruction for primitives, nested objects and arrays.
- ALP encoding 10 for FLOAT/DOUBLE: reader, opt-in writer `enableAlp`, and
  bit-exact fixture verification.
- CDC helpers: `CdcTable` / `ChangeType` for `_change_type`,
  `_commit_version` and `_commit_timestamp`.
- `RecordBatch` / `ColumnVector` batch API with `readBatchSync`, `readBatch`
  and `readBatches`.
- Crash-safe `ParquetStreamWriter.resume` and `fileMetaData`; shared footer
  builder; sidecar metadata round-trip.
- Persistent worker pool for parallel row-group reads.
- Multi-page level/value preallocation, dictionary materialization, zero-copy
  PLAIN typed views and nullable BYTE_ARRAY scan removal.
- `ParquetFeature` / `ParquetUnsupportedFeatureException`, encryption and
  geospatial extension points.
- `created_by` now reports `parquet-dart version 0.2.0`.

## 0.1.3

- Added `ParquetStreamWriter(schema, BytesFile)`: `add()` writes row groups
  directly to the file, holding at most one column chunk in memory.
- Added public `writeFileMetaData` / `readFileMetaData` for bare Thrift
  `FileMetaData` sidecars (for example dual `.meta` slots).
- Added `ParquetWriter.resume(existingData, fileMetaData)`, which strips the
  old footer and appends new row groups while preserving existing offsets,
  page indexes and Bloom filters.
- Performance release; no public API removed or changed.
- Reader: reuse fully-valid compact column buffers instead of copying every
  value, fast single-page concatenation, cheaper PLAIN BYTE_ARRAY decoding, and
  metadata reuse in parallel row-group isolates.
- Writer: large flat tables now encode fixed-width and BYTE_ARRAY values
  directly from typed columns, skip boxed value lists and level arrays, merge
  per-page bounds into chunk statistics, and sample high-cardinality dictionaries
  before building the full map. Small-table output remains byte-compatible with
  v0.1.2 defaults.
- `created_by` now reports `parquet-dart version 0.1.3`.
- Added a large-table fast-path round-trip test (all flat types, nulls, page
  index, CRC).

## 0.1.2

- Final functional/API acceptance release.
- Writer: `writeCrc` (CRC-32 page checksums), `writePageIndex`
  (OffsetIndex/ColumnIndex), `writeBloomFilter` (Parquet Split Block Bloom
  Filter with XXH64), all optional and off by default.
- Reader: `verifyCrc`, OffsetIndex/ColumnIndex/Bloom read APIs.
- Predicate API: `ColumnPredicate` + AND/OR/NOT over row-group statistics;
  `readTable(predicate:)` prunes groups and filters flat rows.
- I/O: `IoStrategy.auto|sync|mmap|batched|parallel`,
  `openSourceWithStrategy`, read-only `MmapSource` and
  `ParallelBytesFileSource`; `ParquetReader.fromSyncSource`.
- Compatibility fixes: legacy PLAIN_DICTIONARY dictionary pages, empty
  DATA_PAGE_V2 payloads, unknown LogicalType union members, BSS FLBA typing,
  legacy `num_rows=0`, zero-row files, and nested-list mis-flattening now
  throws a typed unsupported error.
- Chrome/Wasm matrix expanded; official parquet-testing + pyarrow/DuckDB/Polars
  interop and bounded fuzz tests added.
- v0.1.3 is the performance release and must not break this API.

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
