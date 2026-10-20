# Web (dart2js) support and test matrix

Status: v0.1.2. The package has no `dart:io`/`dart:ffi` dependency in its shared
import graph, so reading and writing Parquet works in the browser with
in-memory bytes, OPFS, IndexedDB and `Blob`/`File` sources.

This document covers the **dart2js** browser target. The Wasm target shares the
same code and tests; see [wasm.md](wasm.md).

## Test suites

| File | What it covers |
|---|---|
| `test/web/web_read_test.dart` | Embedded-fixture reader matrix (flat, nested, all codecs, encodings, page v1/v2, projection, row-group filter/streaming, exact int64, page index) plus the in-memory writer matrix (8 codecs × v1/v2, dictionary, nested, CRC, page index, bloom, defaults). |
| `test/web/web_storage_test.dart` | `Blob`/`File` sources (whole-blob cache and `slice()` path), OPFS writer output/readback for nested + all codecs, OPFS truncation on rewrite, IndexedDB writer output through `writeParquetTableToBytesFile`, capability errors and cleanup. |
| `test/web/fixture_expectations.dart` | Shared value assertions/helpers (also used by `test/wasm/`). |
| `test/web/web_fixtures.dart` | Generated base64 fixtures (committed; no file I/O at runtime). |
| `test/fixtures/web/generate_fixtures.py` | PyArrow generator and fixture provenance. |

The suites are self-contained: fixtures are base64 constants, every OPFS/IDB
file uses a unique logical path, and `tearDown` deletes it. Nothing depends on
the package directory being served, a network, or a worker-hosted fixture.

## Coverage matrix

Legend: ✅ covered by a browser test, ◐ partial/indirect, ❌ not covered here.

| Area | dart2js | Notes |
|---|---|---|
| Flat read: uncompressed, Snappy, Gzip, Brotli, LZ4_RAW, Zstd | ✅ | one fixture per codec, identical values |
| PLAIN, PLAIN_DICTIONARY/RLE_DICTIONARY | ✅ | dictionary fixture asserts dict page + encoding |
| DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY | ✅ | INT32/INT64/UTF-8 columns |
| DELTA_LENGTH_BYTE_ARRAY | ✅ | string column |
| BYTE_STREAM_SPLIT | ✅ | FLOAT/DOUBLE on data page v2 |
| Data page v1 / v2 | ✅ | header type asserted for flat and nested |
| Nested LIST / STRUCT / MAP | ✅ | three nested fixtures (v1 uncompressed, v1 Snappy, v2 Snappy) |
| Top-level projection | ✅ | `columns:` |
| Dotted-path nested projection | ✅ | `st.a`, `lst.list.element`, `mp.key_value.value` |
| `maxRows` truncation (flat) | ✅ | sync and per-row-group |
| `rowGroupFilter` before decode | ✅ | callback count asserted |
| `readRowGroupSync` / sync reads | ✅ | in-memory bytes only |
| `readRowGroups` streaming | ✅ | sizes/filter/projection/`maxRows`; also over a Blob source |
| `readTable` async path == sync | ✅ | values compared |
| Exact INT64 (`getBigInt`) | ✅ | 2^53, 2^53 + 1, INT64_MIN |
| `getInt` safe-range contract | ✅ | throws outside ±(2^53−1) |
| OffsetIndex / ColumnIndex read | ✅ | PyArrow fixture and writer output |
| CRC32 write + `verifyCrc` | ✅ | corrupts one page-body byte and expects `ParquetFormatException` |
| Page index write | ✅ | `writePageIndex: true`, multiple pages |
| Bloom filter write + read | ✅ | `writeBloomFilter: true`, membership true/false |
| Predicate API | ✅ | `readTable(predicate:)` and `table.filterRows` |
| Writer: all 8 codecs × page v1/v2 | ✅ | LZO and legacy LZ4 included |
| Writer: dictionary on/off | ✅ | |
| Writer: multiple row groups | ✅ | ordered `add()` calls |
| Writer: zero-row file | ✅ | no `add()` before `finish()` |
| Writer: OPFS path output | ✅ | `writeParquetTableToFile` |
| Writer: IndexedDB output | ✅ | `writeParquetTableToBytesFile` over an IDB handle |
| Blob/File source | ✅ | cached and `slice()`-per-read paths |
| HTTP Range | ◐ | `HttpRangeSource` callback API is VM-unit-tested; a browser test would need an HTTP server with `Range`/CORS |
| Automatic isolate parallelism | ❌ | Web is single-threaded; `parallelism` is ignored |

## Storage backends

`ParquetSource` is async for browser storage. `ParquetReader.fromBytes` is the
only synchronous path; it needs the whole file in memory.

| Priority | Backend | Entry point | Browser notes |
|---|---|---|---|
| 1 | OPFS (blit chunked `BytesFile`) | `openSource('opfs://path')`, `writeParquetTableToFile(table, path)` | Primary persistent backend. Uses a dedicated worker holding `FileSystemSyncAccessHandle`s; the page CSP must allow `blob:` workers. One open instance per logical path (exclusive handle). `flush()`/`close()` persists the manifest. |
| 2 | `Blob` / `File` | `openBlobSource(blob)`, `BlitSource.openBlob(blob, cacheLimit: ...)` | Read-only, session-only. Blobs up to `cacheLimit` (default 8 MiB) are cached once; larger ones use `Blob.slice()` per range read. |
| 3 | HTTP Range | `HttpRangeSource(lengthProvider:, fetchRange:)` | Bring your own `fetch`; server must support CORS and `Accept-Ranges`. Not opened by `openSource('https://...')` by design. |
| 4 | In-memory | `ParquetReader.fromBytes(bytes)` | Everywhere; no storage API needed. |
| 5 | IndexedDB (explicit fallback) | `openSource('idb://path')`, `BlitSource.openIndexedDb(path)` | Chunked key/value fallback for contexts without usable OPFS. Slower than OPFS; subject to quota/eviction. Not selected automatically. |

Writer sinks on the web:

- `writeParquetTableToFile(table, 'dir/out.parquet', create: true)` — OPFS
  (the native implementation writes a `dart:io` file with the same API).
- `writeParquetTableToBytesFile(table, bytesFile)` — any blit `BytesFile`,
  including `BlitSource.openIndexedDb(...).file`. This is the way to write an
  IndexedDB-backed Parquet file; the caller owns/closes the handle.
- `writeParquetTable(table)` / `ParquetWriter` — in-memory `Uint8List`, then
  upload/store it yourself.

`openSourceSync` intentionally rejects `opfs://` and `idb://`; both are
async-only. `openSource('opfs://missing')` throws blit's web filesystem
`NotFoundError` (`WebFileSystemException`), which the storage suite asserts.

## Limitations

- No native filesystem paths: `openFileSource`/plain paths throw
  `UnsupportedError` on the web.
- OPFS requires a worker with a sync access handle. A restrictive CSP that
  blocks `blob:` workers surfaces as `UnsupportedError`; use Blob/IDB/memory
  instead.
- IndexedDB is an **explicit** `idb://` fallback, not an automatic replacement
  for OPFS. It has no file locking and should not be opened by two handles for
  writing at once.
- `Blob` sources are immutable/read-only; writes throw `StateError`.
- Async reads run on the current event loop; `parallelism` has no effect on the
  web (`dart:isolate` is unavailable). Web Workers are post-1.0.
- `maxRows` truncation is flat-only in v0.1.1/v0.1.2; nested truncation remains
  a documented limitation.
- Deep nesting shapes (list of list, map inside list, list/map inside a struct)
  are rejected by the reader/writer; the browser matrix only uses supported
  shapes.
- Encrypted files (`PARE`) are rejected with `ParquetUnsupportedException`.
- Sink handles are caller-owned: `writeParquetTableToBytesFile` does not close
  the `BytesFile`.

## Running the tests

Local policy for this repository: Windows VM only — `dart analyze`,
`dart format`, VM tests, and `dart compile js` checks. **Do not run Chrome or
Wasm locally**; CI covers them.

Quick local gates:

```sh
dart analyze test/web test/wasm doc
dart format --output=none --set-exit-if-changed test/web test/wasm
mkdir -p build
dart compile js test/wasm/compile_smoke.dart -o build/web_smoke.js
```

Chrome (CI, Ubuntu runner with `CHROME_EXECUTABLE=/usr/bin/google-chrome`):

```sh
dart test -j 4 -p chrome test/web
```

Exact command expected in CI after the Web/Wasm matrix is wired:

```sh
dart test -j 4 -p chrome \
  test/unit/compression_test.dart \
  test/unit/encodings_test.dart \
  test/web
```

Notes:

- Use `-p chrome`, not `-p vm`; the suites are annotated
  `@TestOn('browser')`.
- The CI runner must allow `blob:` workers for OPFS. Headless Chrome on the
  GitHub-hosted runners does.
- First run downloads the `test` browser harness; no fixture server is needed.
- If a run is killed mid-test, stale `parquet-web-*` OPFS files/IDB records can
  remain in that browser profile. They never collide because paths include a
  timestamp and counter; clearing the profile removes them.

## Fixtures and provenance

`test/fixtures/web/generate_fixtures.py` writes 15 tiny PyArrow files
(each ~2–4 KiB, 8 rows for flat, 6 for nested, 12 for row groups, 200 for the
page-index file) and regenerates `test/web/web_fixtures.dart` with base64
copies. The committed `.parquet` files are the source of truth; the browser
tests only read the base64 constants. `pyarrow` is a test-time tool only and is
not a package dependency.

The matrix deliberately mirrors the VM fixture matrix in
`test/fixtures/` but with much smaller files so the browser bundle stays small.
LZO cannot be written by PyArrow; LZO web coverage comes from the Dart writer
round-trip matrix (all 8 codecs) instead of a PyArrow fixture.
