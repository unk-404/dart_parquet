# parquet

A pure Dart Apache Parquet reader and writer for the Dart VM and the Web.

![Benchmarks: Dart JIT/AOT vs Rust](https://raw.githubusercontent.com/unk-404/dart_parquet/master/assets/benchmarks.svg)

`parquet` reads and writes Parquet files without native libraries or FFI. It
supports the Dart VM, dart2js and dart2wasm, with in-memory bytes, native files,
OPFS, IndexedDB, browser `Blob`/`File` and HTTP Range sources.

## Requirements

- Dart SDK 3.13.5 or newer.
- Native I/O uses `dart:io`; browser I/O uses `dart:js_interop`.

## Installation

```yaml
dependencies:
  parquet: ^0.1.3
```

```sh
dart pub get
```

## Quick start

### Read a file

```dart
import 'dart:io';

import 'package:parquet/parquet.dart';

void main() {
  final reader = ParquetReader.fromBytes(File('data.parquet').readAsBytesSync());
  final table = reader.readTableSync();

  final id = table['id'] as Int64Column;
  final name = table['name'] as BinaryColumn;
  print(id.getInt(0));
  print(name.getString(0));
}
```

### Write a file

```dart
final table = reader.readTableSync();
final bytes = writeParquetTable(
  table,
  options: const ParquetWriterOptions(
    compression: CompressionType.zstd,
  ),
);
File('out.parquet').writeAsBytesSync(bytes);
```

To write directly to a native path or Web OPFS:

```dart
await writeParquetTableToFile(table, 'out.parquet');
```

## Reading

Column projection, including nested paths:

```dart
final table = reader.readTableSync(columns: ['id', 'profile.name']);
```

Per-row-group streaming:

```dart
await for (final group in reader.readRowGroups(columns: ['id', 'name'])) {
  print(group.numRows);
}
```

Statistics and predicate pruning:

```dart
final filtered = reader.readTableSync(
  predicate: ColumnPredicate('id', FilterOperator.gtEq, 100),
);
```

Parallel row-group decoding for in-memory sources on the VM:

```dart
final table = await ParquetReader.fromBytes(
  bytes,
  options: const ParquetReaderOptions(parallelism: 4),
).readTable();
```

Synchronous methods always execute on the calling isolate.

## Writing

```dart
final writer = ParquetWriter(
  schema: table.schema,
  options: const ParquetWriterOptions(
    compression: CompressionType.snappy,
    dataPageVersion: 2,
    writePageIndex: true,
    writeBloomFilter: true,
    writeCrc: true,
  ),
);

writer.add(table);            // one row group
final bytes = writer.finish();
```

A flat schema can be created without an existing file:

```dart
final schema = (ParquetSchemaBuilder()
      ..addInt64('id', nullable: false)
      ..addBinary('name', utf8: true)
      ..addFloat64('score'))
    .build();
```

Nested schemas can be written from an existing `ParquetSchema`, including one
read from a file.

## Web

```dart
final source = await openSource('data.parquet'); // or 'opfs://...'
final reader = await ParquetReader.open(source);
final table = await reader.readTable();
await source.close();
```

Browser `Blob`/`File`:

```dart
final source = await openBlobSource(blob);
final reader = await ParquetReader.open(source);
```

IndexedDB is available as an explicit async fallback:

```dart
final source = await openSource('idb://data.parquet');
```

On the Web, `readTable` stays on the current event loop. Native mmap and
worker-pool sources are not available.

## I/O

The reader accepts any `ParquetSource` or `SyncParquetSource`. Built-in
adapters include:

- `MemorySource`
- native files (`dart:io`)
- blit `BytesFile`: native, OPFS, Blob and IndexedDB
- `HttpRangeSource` for callback-based HTTP range requests
- native `MmapSource` and `ParallelBytesFileSource`

Path-based helpers:

```dart
final source = await openSource('data.parquet');
final source = await openSourceWithStrategy(
  'data.parquet',
  strategy: IoStrategy.mmap,
);
```

## Supported data

| Item | Support |
|---|---|
| Physical types | BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY |
| Logical types | string, decimal, date/time/timestamp, integer, enum, JSON, BSON, UUID, float16, LIST/MAP |
| Pages | data page v1/v2, dictionary pages |
| Compression | uncompressed, Snappy, Gzip, LZO, Brotli, Zstd, LZ4_RAW; legacy LZ4 read |
| Encodings | PLAIN, dictionary/RLE, DELTA_*, BYTE_STREAM_SPLIT; PLAIN/dictionary write |
| Schemas | flat and common nested LIST/STRUCT/MAP |
| Metadata | statistics, page indexes, Bloom filters, CRC32 |

Encrypted files, geospatial types, Variant, ALP and CDC are not supported.
Nested shapes outside the supported LIST/STRUCT/MAP matrix are rejected with a
typed exception rather than returning incorrect data.

## Benchmarks

The chart compares Dart JIT, Dart AOT and Rust (arrow-rs `parquet`) on the same
fixtures and operations: full scans, projection, nested data, metadata parsing,
writes and multi-isolate/multi-thread variants. The harness lives in
[`benchmark/`](benchmark/); the Rust counterpart is in
[`benchmark/rust`](benchmark/rust) and is pinned to `parquet = "=60.0.0"`.

v0.1.3 adds zero-copy fast paths for fully-valid flat columns and direct
column-to-page writing for large flat tables, while keeping the public API and
small-table defaults unchanged.

## Development

```sh
dart analyze
dart format --set-exit-if-changed .
dart test -j 4
```

Chrome and Wasm tests run in CI. The Rust benchmark is built and run with:

```sh
cargo build --release --locked --manifest-path benchmark/rust/Cargo.toml
benchmark/rust/target/release/parquet-bench --jsonl rust.jsonl
```

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
