# parquet

Pure Dart Apache Parquet reader with VM and Web support.

![Parquet for Dart benchmarks](https://raw.githubusercontent.com/unk-404/dart_parquet/master/assets/benchmarks.svg)

`parquet` reads Parquet files without native libraries or FFI. It runs on the
Dart VM, dart2js and dart2wasm, and works with in-memory bytes, native files,
blit OPFS files, browser `Blob`/`File` slices, and HTTP Range readers.

> **Status: v0.1.0 — usable read-only release.**
> Writing, async streaming, page-index/bloom filters and isolate parallelism are
> planned for v0.1.1/v0.1.2. The format reader (flat and nested) is complete for
> the supported feature set below.

## Features

- **Pure Dart**, no native dependencies.
- **VM + Web** (dart2js; dart2wasm target planned).
- **All Parquet compression codecs**: Uncompressed, Snappy, Gzip, LZO, Brotli,
  Zstandard, LZ4_RAW; legacy LZ4 (Hadoop framing) is read.
- **Encodings**: PLAIN, PLAIN_DICTIONARY/RLE_DICTIONARY, RLE, BIT_PACKED levels,
  DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY,
  BYTE_STREAM_SPLIT.
- **Data pages v1 and v2**, dictionary pages, statistics metadata.
- **Nested data**: LIST, STRUCT and MAP (common shapes; see limitations).
- **Typed columnar API** plus a row view; zero-copy `Bytes` for binary values.
- **I/O abstraction**: `ParquetSource` / `SyncParquetSource`; no coupling to
  `File`.
- **Exact 64-bit handling**: `Int64List` on the VM, two-limb storage on the web,
  with `BigInt` accessors.

## Quick start

```dart
import 'dart:io';

import 'package:parquet/parquet.dart';

void main() {
  final reader = ParquetReader.fromBytes(File('data.parquet').readAsBytesSync());
  print('${reader.numRows} rows, ${reader.numRowGroups} row groups');
  print(reader.schema.columns.map((c) => c.dotPath));

  final table = reader.readTableSync();
  final id = table['id'] as Int64Column;
  final name = table['name'] as BinaryColumn;
  print(id.getInt(0));
  print(name.getString(0));
}
```

Projection and row groups:

```dart
final table = reader.readTableSync(columns: ['id', 'name']);
final firstGroup = reader.readRowGroupSync(0);
final row = firstGroup.row(0);
print(row.toMap());
```

Asynchronous sources (native file, OPFS, Blob, HTTP Range):

```dart
final source = await openSource('data.parquet'); // or 'opfs://data.parquet'
final reader = await ParquetReader.open(source);
final table = await reader.readTable();
await source.close();
```

Web:

```dart
final bytes = await fetchParquetBytes(); // your Blob/fetch code
final reader = ParquetReader.fromBytes(bytes);
```

## Supported Parquet features (v0.1.0)

| Feature | Status |
|---|---|
| Physical types (BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY) | yes |
| Logical types (string, decimal, date/time/timestamp, int/uint, enum, json, bson, uuid, float16, list/map) | yes |
| Data page v1/v2, dictionary pages | yes |
| Compression: uncompressed, Snappy, Gzip, LZO, Brotli, Zstd, LZ4_RAW | yes |
| Legacy LZ4 (Hadoop framing) | read |
| Encodings listed above | yes |
| Flat schemas | yes |
| LIST of leaf or struct; STRUCT; MAP leaf→leaf | yes |
| Nested projection | no (top-level projection only) |
| Writing | no (v0.1.1) |
| Page index, bloom filters, row-group statistics pruning | no (v0.1.1/v0.1.2) |
| Encryption, geospatial, Variant, ALP, CDC | no (post-1.0) |

### Known limitations

- Deeper nesting shapes (list of list, map inside list, struct containing a list)
  are reported as `ParquetUnsupportedException` in v0.1.0.
- LZO is a legacy/deprecated Parquet codec; both Hadoop `BlockCompressorStream`
  framing and raw LZO1X blocks are read.
- Encrypted footers (`PARE`) are rejected.
- `maxRows` truncation works for flat columns; nested truncation lands in v0.1.1.
- Isolate parallelism is planned for v0.1.1; v0.1.0 is single-isolate.

## I/O

`ParquetReader` accepts either a synchronous `SyncParquetSource` or an
asynchronous `ParquetSource`:

```dart
final source = MemorySource.fromUint8List(bytes); // sync
final reader = ParquetReader.fromBytes(bytes);   // convenience
```

Adapters live in `lib/src/io/`:

- `MemorySource`
- native file (sync/async, `dart:io`, VM only)
- `BlitSource` (blit `BytesFile`: native files and browser OPFS)
- `BlobSliceSource` / `openBlobSource` (browser Blob/File)
- `HttpRangeSource` (callback-based; bring your own HTTP client)
- `openSource` / `openSourceSync` with `file://` and `opfs://` support

## Dependencies

- [blit](https://pub.dev/packages/blit) — zero-copy `Bytes`
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
dart test
```

Linux/macOS, Chrome and Wasm tests run in GitHub Actions. Benchmarks are built
with [`benchmark_test`](https://pub.dev/packages/benchmark_test) and rendered by
[`benchcat`](https://github.com/unk-404/benchcat) in CI; the chart above is
committed to `assets/benchmarks.svg`.

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
