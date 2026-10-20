# WebAssembly (dart2wasm) support and test matrix

Status: v0.1.2. The package imports no `dart:io` or `dart:ffi` in its shared
graph and uses `dart:js_interop` for browser APIs, so the same reader, writer,
codecs and storage adapters compile with `dart2wasm`.

## Test suites

`test/wasm/` intentionally reuses the dart2js suites instead of duplicating
them, so the two compilers cannot drift apart:

| File | Role |
|---|---|
| `test/wasm/wasm_smoke_test.dart` | Small, storage-free dart2wasm gate: flat Snappy read, nested v2 read, exact int64, Zstd/v2 writer round trip. |
| `test/wasm/wasm_read_test.dart` | Runs the full `test/web/web_read_test.dart` matrix under dart2wasm (all codecs, encodings, page versions, projection, pruning, streaming, int64, page index, CRC, bloom, predicates, writer). |
| `test/wasm/wasm_storage_test.dart` | Runs `test/web/web_storage_test.dart` under dart2wasm (Blob, OPFS, IndexedDB). |
| `test/wasm/compile_smoke.dart` | Not a test; a `main()` used for `dart compile js` / `dart compile wasm` build checks. |

The shared fixtures and assertions live in `test/web/` (base64 constants; no
runtime file I/O).

## What works under dart2wasm

| Area | Status | Notes |
|---|---|---|
| Footer/metadata/schema, all physical/logical types | ✅ | pure Dart |
| Flat read, all encodings (PLAIN, dictionary, DELTA_*, BSS, RLE levels) | ✅ | shared decoder code |
| Nested LIST/STRUCT/MAP read | ✅ | shared Dremel assembly |
| All codecs: uncompressed, Snappy, Gzip, Brotli, Zstd, LZ4_RAW, legacy LZ4, LZO | ✅ | codec packages are pure Dart |
| Page v1/v2, dictionary pages, statistics | ✅ | |
| Projection (top-level and dotted nested paths) | ✅ | |
| `rowGroupFilter`, predicate API, page index read, CRC verification, bloom read | ✅ | pure Dart |
| Streaming `readRowGroups` and async `readTable` | ✅ | single-threaded; no isolates |
| Writer: flat + nested, all codecs, v1/v2, dictionary, CRC, page index, bloom | ✅ | same writer as dart2js |
| Exact INT64 via `getBigInt` | ✅ | two-limb buffer; `getInt` keeps the ±(2^53−1) safe-range contract |
| In-memory sources | ✅ | `ParquetReader.fromBytes` |
| `Blob`/`File` source | ✅ | `dart:js_interop` + blit |
| OPFS read/write | ✅ | blit chunk backend via worker `FileSystemSyncAccessHandle` |
| IndexedDB read/write | ✅ | `idb://` explicit fallback; `writeParquetTableToBytesFile` |
| HTTP Range | ◐ | `HttpRangeSource` callbacks work; needs a range-capable server, not exercised by the bundled suites |

## What does not work / caveats

- **No isolates.** `dart:isolate` is unavailable on the web, including Wasm.
  `ParquetReaderOptions.parallelism` is forced to the current event loop
  (single-thread); Web Workers are post-1.0.
- **No `dart:io`/`dart:ffi`.** Native files, mmap, IOCP/io_uring and the
  isolate pool are VM-only. Use `ParquetReader.fromBytes`, OPFS, IndexedDB or a
  `Blob`.
- **Synchronous reads need in-memory bytes.** OPFS/IDB/Blob sources are
  async-only; `readTableSync` is available only for `ParquetReader.fromBytes`.
- **Storage is browser-dependent.** OPFS needs a worker with a sync access
  handle and a CSP that permits `blob:` workers. IndexedDB is an explicit
  fallback, not auto-selected. Both are subject to quota/eviction.
- **Same Parquet feature limits as dart2js:** unsupported deep nesting shapes
  and encrypted (`PARE`) files are rejected; `maxRows` nested truncation is
  still a known limitation.
- **Runtime requirements.** `dart2wasm` output needs a WasmGC-capable runtime
  (Chrome 119+ for the CI runner). The Dart SDK must support
  `dart test -c dart2wasm` (3.13.x is used by CI).
- **Performance.** This suite asserts correctness, not throughput. Wasm
  performance is tracked separately by the benchmark suite; do not read the
  browser test timings as benchmarks.

## Running the tests

Local policy: **do not run Chrome or Wasm locally.** Local checks are limited to
`dart analyze`, `dart format`, Windows VM tests and `dart compile js` smoke
builds. CI runs the Wasm suites on an Ubuntu runner with `CHROME_EXECUTABLE`
set.

Local:

```sh
dart analyze test/web test/wasm doc
dart format --output=none --set-exit-if-changed test/wasm test/web
mkdir -p build
dart compile js test/wasm/compile_smoke.dart -o build/web_smoke.js
```

The WebAssembly compile check (CI or a developer machine with a Wasm-capable
toolchain; not run by the local Windows policy):

```sh
dart compile wasm test/wasm/compile_smoke.dart -o build/wasm_smoke.wasm
node build/wasm_smoke.mjs   # runs the same read/write smoke
```

Chrome + dart2wasm (CI):

```sh
# Fast gate
dart test -p chrome -c dart2wasm test/wasm/wasm_smoke_test.dart

# Full matrix (read/write, page index, CRC, bloom, predicates)
dart test -p chrome -c dart2wasm test/wasm

# Storage backends only
dart test -p chrome -c dart2wasm test/wasm/wasm_storage_test.dart
```

Exact command expected in CI after the Web/Wasm matrix is wired:

```sh
dart test -j 4 -p chrome -c dart2wasm test/wasm
```

Notes:

- The current workflow runs `dart test -p chrome -c dart2wasm test/web` with
  `continue-on-error`; switching it to `test/wasm` adds the Wasm-specific smoke
  suite and the shared suites in one command.
- `test/wasm` needs the same environment as the dart2js suite:
  `CHROME_EXECUTABLE=/usr/bin/google-chrome` on the Ubuntu runner.
- The suites are annotated `@TestOn('browser')`; `-p vm` skips them by design.
- OPFS/IDB tests use unique paths and clean up in `tearDown`. A killed run can
  leave stale `parquet-web-*` entries in the browser profile; clear the profile
  to remove them.

## Compile smoke file

`test/wasm/compile_smoke.dart` is not collected by `dart test` (only
`*_test.dart` is). It exists to prove that the complete public import graph —
reader, writer, codecs, fixtures — compiles to JavaScript and to WebAssembly
without a browser, and it performs a tiny read/write/round-trip check when the
compiled output is executed. The local repository policy permits running the
`dart compile js` part only; CI can run the Wasm counterpart.
