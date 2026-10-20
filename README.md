# parquet

Pure Dart Apache Parquet reader and writer with VM and Web support.

![Parquet for Dart vs Rust benchmarks](https://raw.githubusercontent.com/unk-404/dart_parquet/master/assets/benchmarks.svg)

`parquet` reads and writes Parquet files without native libraries or FFI. It
runs on the Dart VM, dart2js and dart2wasm, and works with in-memory bytes,
native files, blit OPFS/IndexedDB files, browser `Blob`/`File` slices, and HTTP
Range readers.

> **Status: v0.1.2 — final functional/API acceptance.**
> Flat and nested read/write, all codecs, page v1/v2, dictionary/statistics,
> nested projection, row-group pruning, streaming, page indexes, Bloom filters,
> CRC verification, predicate filtering, OPFS write, mmap/batched/parallel
> sources and automatic VM isolate reads are in. v0.1.3 is the performance
> release and must not break this API.

## Features

- **Pure Dart**, no native dependencies.
- **VM + Web**; the shared import graph is free of `dart:io`/`dart:ffi`.
- **All Parquet compression codecs**: uncompressed, Snappy, Gzip, LZO, Brotli,
  Zstandard, LZ4_RAW; legacy LZ4 (Hadoop framing) is read.
- **Read encodings**: PLAIN, PLAIN_DICTIONARY/RLE_DICTIONARY, RLE, BIT_PACKED
  levels, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY,
  BYTE_STREAM_SPLIT.
- **Write encodings**: PLAIN, dictionary (BYTE_ARRAY/FLBA) with automatic
  fallback, RLE levels, dictionary-index hybrid.
- **Data pages v1/v2**, dictionary pages, chunk/page statistics, multiple row
  groups.
- **Nested data**: LIST, STRUCT and MAP in both directions (common shapes; see
  limitations).
- **Writer API**: `ParquetWriterOptions`, streaming `ParquetWriter.add`,
  `writeParquetTable`, flat `ParquetSchemaBuilder`, memory/file/OPFS output.
- **Writer v0.1.2**: optional CRC32 page checksums, OffsetIndex/ColumnIndex and
  Split Block Bloom filters (`writeCrc`, `writePageIndex`, `writeBloomFilter`).
- **Reader API**: typed columns + row view, zero-copy binary values, top-level
  and dotted-path nested projection, statistics row-group filter, per-row-group
  streaming, OffsetIndex/ColumnIndex/Bloom read access, `verifyCrc`.
- **Predicate API**: `ColumnPredicate` + AND/OR/NOT over row-group statistics,
  plus flat-table row filtering via `ParquetTable.filterRows`/`readTable`.
- **Parallel reads**: async `readTable` uses up to 8 isolates (`parallelism:
  auto|1|N`) for in-memory multi-row-group files; sync methods always run on the
  calling isolate. Web stays single-threaded.
- **I/O strategies**: `IoStrategy.auto|sync|mmap|batched|parallel`,
  `openSourceWithStrategy`, read-only `MmapSource` and
  `ParallelBytesFileSource` (native), with OPFS/Blob/IndexedDB on Web.
- **Exact 64-bit handling**: `Int64List` on the VM, two-limb storage on the web,
  with `BigInt` accessors.

## Quick start (read)

```dart
import 'dart:io';

import 'package:parquet/parquet.dart';

void main() {
  final reader = ParquetReader.fromBytes(File('data.parquet').readAsBytesSync());
  print('${reader.numRows} rows, ${reader.numRowGroups} row groups');

  final table = reader.readTableSync();
  final id = table['id'] as Int64Column;
  final name = table['name'] as BinaryColumn;
  print(id.getInt(0));
  print(name.getString(0));
}
```

Projection, pruning, streaming and parallel reads:

```dart
final columns = reader.readTableSync(columns: ['id', 'profile.name']);
final stream = reader.readRowGroups(columns: ['id', 'name']);
final parallel = await ParquetReader.fromBytes(
  bytes,
  options: const ParquetReaderOptions(parallelism: 4),
).readTable();

final filtered = await reader.readTable(
  rowGroupFilter: (group) {
    final stats = group.columnByPath(['id'])?.statistics;
    // Skip groups whose id column is entirely null.
    return stats?.nullCount != group.numRows;
  },
);
```

Page indexes:

```dart
final chunk = reader.metadata.rowGroup(0).column(0);
final offsetIndex = chunk.readOffsetIndex(source);
final columnIndex = chunk.readColumnIndex(source);
```

## Quick start (write)

```dart
final table = reader.readTableSync();
final bytes = writeParquetTable(
  table,
  options: const ParquetWriterOptions(
    compression: CompressionType.zstd,
    dataPageVersion: 2,
  ),
);
File('out.parquet').writeAsBytesSync(bytes);

// Streaming row groups (each add() = one row group)
final writer = ParquetWriter(
  schema: table.schema,
  options: const ParquetWriterOptions(compression: CompressionType.snappy),
);
writer.add(table);
final encoded = writer.finish();
```

Native file / Web OPFS output:

```dart
await writeParquetTableToFile(
  table,
  'out.parquet',
  options: const ParquetWriterOptions(compression: CompressionType.zstd),
);

// Web: the conditional export uses blit OPFS instead of dart:io.
await writeParquetTableToFile(table, 'dir/out.parquet');
```

Create a flat schema when you do not have an existing file:

```dart
final schema = (ParquetSchemaBuilder()
      ..addInt64('id', nullable: false)
      ..addBinary('name', utf8: true)
      ..addFloat64('score'))
    .build();
```

## Quick start (async sources / Web)

```dart
final source = await openSource('data.parquet'); // 'opfs://...' or 'idb://...'
final reader = await ParquetReader.open(source);
final table = await reader.readTable();
await source.close();
```

Web Blob/File and IndexedDB:

```dart
final source = await openBlobSource(blob); // JS Blob or File
final reader = await ParquetReader.open(source);

final idb = await openSource('idb://data.parquet');
```

A `Uint8List` can always be read synchronously without any Web storage API:

```dart
final reader = ParquetReader.fromBytes(bytes);
```

## Supported Parquet features (v0.1.2)

| Feature | Status |
|---|---|
| Physical types (BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY) | read + write |
| Logical types (string, decimal, date/time/timestamp, int/uint, enum, json, bson, uuid, float16, list/map) | read + write |
| Data page v1/v2, dictionary pages | read + write |
| Compression: uncompressed, Snappy, Gzip, LZO, Brotli, Zstd, LZ4_RAW | read + write |
| Legacy LZ4 (Hadoop framing) | read |
| Encodings listed above | read; PLAIN + dictionary write |
| Flat schemas | read + write |
| LIST of leaf or struct; STRUCT; MAP leaf→leaf | read + write |
| Nested projection (dotted paths) | read |
| Statistics row-group pruning | read (`rowGroupFilter`, `predicate`) |
| OffsetIndex / ColumnIndex | read + write |
| Split Block Bloom filters | read + write |
| Page CRC32 | read (`verifyCrc`) + write (`writeCrc`) |
| Predicate API + flat row filtering | read |
| Async per-row-group streaming | read |
| Automatic isolate reads (`parallelism: auto`) | read, VM only |
| mmap / batched / parallel native sources | read |
| Multiple row groups, zero-row files | read + write |
| Encryption, geospatial, Variant, ALP, CDC | no (post-1.0) |
| Page-level predicate skipping / builder tuning | v0.1.3 (performance) |

### Known limitations

- Deeper nesting shapes (list of list, map inside list, list/map inside a
  struct) are rejected with a typed `ParquetUnsupportedException`.
- Bare repeated leaf/group fields are not supported; use the LIST annotation.
- Writer dictionary encoding is used for BYTE_ARRAY/FLBA; numeric columns are
  written PLAIN.
- `maxRows` truncation works for flat columns; nested truncation is not part of
  the v0.1.2 API.
- Predicate row filtering is implemented for flat tables; nested row filtering
  returns a typed unsupported error. Page-level predicate skipping is v0.1.3.
- Parallel reads are `Isolate.run` per row group on VM memory sources; the
  long-lived pool, writer-side parallel encode and Web Workers are v0.1.3.
- Web Blob sources are read-only; IndexedDB is an explicit async fallback
  (`idb://`), not a replacement for OPFS on every browser.
- LZO is a legacy/deprecated Parquet codec; both Hadoop
  `BlockCompressorStream` framing and raw LZO1X blocks are read.
- Encrypted footers (`PARE`) are rejected.

## I/O

`ParquetReader` accepts either a synchronous `SyncParquetSource` or an
asynchronous `ParquetSource`:

```dart
final source = MemorySource.fromUint8List(bytes); // sync
final reader = ParquetReader.fromBytes(bytes);   // convenience

// Or choose an I/O engine explicitly on native:
final opened = await openSourceWithStrategy(
  'data.parquet',
  strategy: IoStrategy.mmap,
);
final mmapReader = opened.canReadSync
    ? ParquetReader.fromSyncSource(opened.sync!)
    : await ParquetReader.open(opened);
```

Adapters live in `lib/src/io/`:

- `MemorySource`
- native file (sync/async, `dart:io`, VM only)
- `BlitSource` (blit `BytesFile`: native files, browser OPFS, Blob, IndexedDB)
- `BlobSliceSource` / `openBlobSource` (browser Blob/File via blit 0.1.6)
- `BlitSource.openIndexedDb` / `openIdbBlitSource` (blit 0.1.6 IDB backend)
- `HttpRangeSource` (callback-based; bring your own HTTP client)
- `openSource` / `openSourceSync` with `file://`, `opfs://` and `idb://` support
- `MmapSource` / `openMmapSourceSync` (native read-only zero-copy mapping)
- `ParallelBytesFileSource` / `openParallelSource` (native worker-pool reads)
- `IoStrategy` + `openSourceWithStrategy` / `OpenedParquetSource` (auto/sync/
  mmap/batched/parallel, plus `.sync` for `ParquetReader.fromSyncSource`)

Writer sinks (`lib/src/writer/file_output*.dart`):

- `writeParquetTableToBytesFile(table, BytesFile)` (native/OPFS/IDB handles)
- `writeParquetTableToFile(table, path)` (native `dart:io` or Web OPFS)

## Benchmarks

The chart above is produced in CI by
[`benchmark_test`](https://pub.dev/packages/benchmark_test) and
[`benchcat`](https://github.com/unk-404/benchcat), and compares:

- Dart VM JIT
- Dart VM AOT
- Rust with the arrow-rs `parquet` crate

Both sides run the same fixtures and the same operations: flat full scans for
Snappy/Zstd/Brotli/LZ4_RAW, i64 projection, nested list/struct, metadata-only
parse, four-isolate/four-thread variants, and flat/nested writes across codecs
and page versions. The Rust harness lives in [`benchmark/rust`](benchmark/rust)
and is pinned to `parquet = "=60.0.0"`.

v0.1.2 locks correctness, API and the comparison harness; the performance gates
and throughput optimization work are v0.1.3 and keep this API unchanged.

Benchmark fixtures are committed under `benchmark/fixtures/`; raw JSONL is
uploaded as a CI artifact, and only the rendered SVG is committed.

## Dependencies

- [blit](https://pub.dev/packages/blit) — zero-copy `Bytes`, OPFS, Blob and IndexedDB `BytesFile` backends
- [snappy_neo](https://pub.dev/packages/snappy_neo)
- [zstd_neo](https://pub.dev/packages/zstd_neo)
- [brotli_neo](https://pub.dev/packages/brotli_neo)
- [lzo_neo](https://pub.dev/packages/lzo_neo)
- [dart_lz4](https://pub.dev/packages/dart_lz4)
- [just_deflate](https://pub.dev/packages/just_deflate)

## Development

Local development runs Windows VM tests only:

```sh
dart analyze
dart format --set-exit-if-changed .
dart test -j 4
```

Linux/macOS, Chrome and Wasm tests run in GitHub Actions. Benchmarks build the
Rust counterpart with `cargo build --release --locked` and run it after the Dart
JIT/AOT suites.

More docs:

- [Web storage and browser matrix](doc/web.md)
- [Wasm matrix and limitations](doc/wasm.md)
- [Benchmark method, v0.1.1 baseline and v0.1.3 gates](doc/benchmark.md)
- [Official parquet-testing fixture provenance](test/fixtures/interop/README.md)

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
