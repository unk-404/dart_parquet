# Rust (arrow-rs) benchmark counterpart

Standalone Rust benchmark that mirrors every operation in
[`benchmark/parquet_bench_test.dart`](../parquet_bench_test.dart) using the
[`parquet`](https://crates.io/crates/parquet) crate from
[arrow-rs](https://github.com/apache/arrow-rs), and emits
[`benchmark_test`](https://pub.dev/packages/benchmark_test) JSONL that benchcat
can render next to the Dart JIT/AOT series.

It covers the full Dart suite:

- 7 read rows (`read.flat.snappy`, `.zstd`, `.brotli`, `.lz4`,
  `read.flat.snappy.projection.i64`, `read.nested.snappy`,
  `read.metadata.snappy`)
- 7 write rows (`write.flat.snappy`, `.zstd`, `.brotli`, `.lz4`,
  `.uncompressed`, `.snappy.v2`, `write.nested.snappy`)
- 6 opt-in multi-thread read rows when `--threads N` (N > 1) is passed

The required fixture files are the same ones the Dart suite uses, under
[`benchmark/fixtures/`](../fixtures/). The master/CI benchmark job builds this
crate with `cargo build --release --locked`, runs it with `--threads 4`, and
feeds `build/bench/rust.jsonl` to benchcat alongside the Dart JIT/AOT series.
`benchcat.toml` and the Dart benchmark file are owned by the repo root.

## Pinned versions

| Crate / tool | Version | Notes |
|---|---|---|
| `parquet` | `=60.0.0` | arrow-rs; default features (Arrow reader/writer, snappy, zstd, brotli, lz4, gzip) |
| `arrow-array` | `=60.0.0` | `RecordBatch` / array buffers for checksums |
| `arrow-data` | `=60.0.0` | `ArrayData` recursion (list/struct children) |
| `bytes` | `=1.12.1` | `Bytes` implements `ChunkReader` |
| `serde_json` | `=1.0.154` | JSONL encoding |
| Rust | `1.85+` | `rust-version` in `Cargo.toml` |

`Cargo.lock` is committed so transitive dependencies are reproducible.
`target/` is ignored by the local [`gitignore`](.gitignore).

## Build and run

From the repository root:

```bash
cargo build --release --manifest-path benchmark/rust/Cargo.toml
cargo run --release --manifest-path benchmark/rust/Cargo.toml -- \
    --jsonl build/bench/rust.jsonl
```

From this directory:

```bash
cd benchmark/rust
cargo run --release -- --jsonl ../../build/bench/rust.jsonl
```

The default run executes all 7 read + 7 write rows (about 30 s at 2 s/row).
`--no-writes` runs reads only; `--threads N` adds the multi-thread read rows.

`--fixtures` defaults to the nearest `benchmark/fixtures/` found by walking up
from the current directory, so both invocations above work without extra flags.
`--jsonl` paths are relative to the current directory; parent directories are
created automatically. With no `--jsonl`, JSONL goes to stdout and progress
logs to stderr.

### Options

```
--fixtures <DIR>     Fixture directory (default: <repo>/benchmark/fixtures)
--jsonl <PATH>       Write JSONL to PATH instead of stdout
--threads <N|auto>   Threads for optional `.mt` rows; 1 disables them (default: 1)
--samples <N>        Minimum measured samples per operation (default: 10)
--duration-ms <MS>   Minimum measured time per operation (default: 2000)
--max-samples <N>    Safety cap on samples per operation (default: 1000000)
--warmup <N>         Untimed warmup iterations per operation (default: 1)
--batch-size <N>     Arrow record-batch size for reads (default: 8192)
--only <SUBSTR>      Run only benchmark names containing SUBSTR
--no-writes          Skip the write rows (reads only)
--dump-writes <DIR>  Also write each produced file to DIR for verification
--list               Print benchmark names and exit
```

Short local smoke tests (about 3 s per selection instead of ~2 s per row):

```bash
# all write rows
cargo run --release --manifest-path benchmark/rust/Cargo.toml -- \
    --samples 2 --duration-ms 300 --only "write "

# one read row
cargo run --release --manifest-path benchmark/rust/Cargo.toml -- \
    --samples 2 --duration-ms 300 --only read.flat.snappy

# produce files for pyarrow/arrow-rs inspection
cargo run --release --manifest-path benchmark/rust/Cargo.toml -- \
    --samples 2 --duration-ms 300 --only "write " \
    --dump-writes target/write-verify
```

## Fixture and name mapping

The JSON `name` is the Dart benchmark id prefixed with its `test` group
(`read` / `write`), exactly what benchcat's `benchcat.toml` rows expect
(e.g. `id = "read read.flat.snappy"`).

### Read rows

| Dart benchmark | JSONL `name` | Fixture | Rows | Fixture bytes |
|---|---|---|---:|---:|
| `read.flat.snappy` | `read read.flat.snappy` | `flat_snappy.parquet` | 100,000 | 1,857,633 |
| `read.flat.zstd` | `read read.flat.zstd` | `flat_zstd.parquet` | 100,000 | 806,140 |
| `read.flat.brotli` | `read read.flat.brotli` | `flat_brotli.parquet` | 100,000 | 779,364 |
| `read.flat.lz4` | `read read.flat.lz4` | `flat_lz4.parquet` | 100,000 | 1,790,830 |
| `read.flat.snappy.projection.i64` | `read read.flat.snappy.projection.i64` | `flat_snappy.parquet` | 100,000 | 1,857,633 |
| `read.nested.snappy` | `read read.nested.snappy` | `nested_snappy.parquet` | 50,000 | 612,431 |
| `read.metadata.snappy` | `read read.metadata.snappy` | `flat_snappy.parquet` | 100,000 | 1,857,633 |

### Write rows

The source table is decoded from the listed fixture once, outside timing. The
output is written to a fresh in-memory `Vec<u8>` each iteration.

| Dart benchmark | JSONL `name` | Source fixture | Rows | Writer settings |
|---|---|---|---:|---|
| `write.flat.snappy` | `write write.flat.snappy` | `flat_snappy.parquet` | 100,000 | Snappy, dict on, v1 data pages |
| `write.flat.zstd` | `write write.flat.zstd` | `flat_snappy.parquet` | 100,000 | Zstd level 3, dict on, v1 |
| `write.flat.brotli` | `write write.flat.brotli` | `flat_snappy.parquet` | 100,000 | Brotli quality 5, dict on, v1 |
| `write.flat.lz4` | `write write.flat.lz4` | `flat_snappy.parquet` | 100,000 | `LZ4_RAW`, dict on, v1 |
| `write.flat.uncompressed` | `write write.flat.uncompressed` | `flat_snappy.parquet` | 100,000 | no compression, dict on, v1 |
| `write.flat.snappy.v2` | `write write.flat.snappy.v2` | `flat_snappy.parquet` | 100,000 | Snappy, dict on, `WriterVersion::PARQUET_2_0` |
| `write.nested.snappy` | `write write.nested.snappy` | `nested_snappy.parquet` | 50,000 | Snappy, dict on, v1 |

Schema of `flat_*.parquet`: `i64 INT64, f64 DOUBLE, s BYTE_ARRAY, b BOOLEAN`
(4 row groups). Projection selects leaf `0` (`i64`) with
`ProjectionMask::leaves`, matching Dart's `columns: ['i64']`.
Schema of `nested_snappy.parquet`: `id INT32, lst LIST<element: INT32>,
st STRUCT<a INT32, b STRING>` (5 row groups). The benchmark reads the fixture
schema dynamically, so a regenerated fixture with an extra `map` column is
written without code changes.

Row counts are asserted at runtime; a mismatched fixture fails the run instead
of silently benchmarking the wrong data.

### Multi-thread rows (opt-in)

Passing `--threads N` with `N > 1` appends `.mt` rows. They parse the footer
once and decode disjoint row groups on `N` scoped OS threads
(`std::thread::scope`), then sum per-thread checksums and row counts:

- `read read.flat.snappy.mt`
- `read read.flat.zstd.mt`
- `read read.flat.brotli.mt`
- `read read.flat.lz4.mt`
- `read read.flat.snappy.projection.i64.mt`
- `read read.nested.snappy.mt`

Metadata parsing and the writer stay single-threaded, so there are no `.mt`
rows for those. Dart v0.1.1 is sync/single-thread, so `.mt` rows are disabled by
default and only appear when explicitly requested. With 4 row groups and
`--threads 4`, each worker decodes exactly one row group. `--threads auto` uses
`min(available_parallelism, 8)`.

## Method

### Read rows

- Fixture bytes are read once at startup and reused, matching the Dart suite's
  `setUpAll`; each measured operation builds a new
  `ParquetRecordBatchReader` from the in-memory bytes, which re-parses the
  footer and decodes all pages.
- Full scans iterate every `RecordBatch` (batch size 8192) and fold a checksum
  over every decoded buffer (including nested child arrays), so the decoder
  cannot be optimized away. The checksum is discarded after timing.
- `read.metadata.snappy` constructs `SerializedFileReader` and touches
  footer/schema/row-group metadata only; no page decode.

### Write rows

- The source fixture is decoded into Arrow batches once, outside timing
  (batch size = fixture row count, so the whole table is one `RecordBatch`).
- Each measured iteration allocates a fresh `Vec<u8>` (capacity = source
  uncompressed bytes + 1 MiB) and runs a complete
  `ArrowWriter::try_new_with_options` → `write(batch)`* →
  `close()` pipeline; writer construction, schema conversion, page encoding,
  compression, and footer finalisation are all timed.
- `WriterProperties` mirror the Dart `ParquetWriterOptions` defaults:
  dictionary encoding on, 1 MiB data-page target, 1 MiB dictionary-page target,
  page statistics, a single row group (the fixture is below the 1M-row row-group
  limit), and `WriterVersion::PARQUET_1_0` except for the `.v2` row
  (`PARQUET_2_0`). Dart's writer has only a byte-targeted page limit, so the
  arrow-rs row-count page cap is raised to 1M to keep page splits size-driven.
  The fallback value encoding is pinned to `PLAIN` because Dart always falls
  back to PLAIN when the dictionary does not fit, for both page versions;
  arrow-rs would otherwise choose `DELTA_*` encodings under `PARQUET_2_0`,
  making the `.v2` row differ in both page format and value encoding.
- Arrow IPC schema metadata (`ARROW:schema`) is skipped, because the Dart writer
  produces a plain Parquet schema. pyarrow still reconstructs
  list/struct fields correctly.
- Before timing each write row, one probe write is fully validated in-process:
  the bytes are re-opened with `ParquetRecordBatchReaderBuilder`, row count and
  single-row-group count are checked, every column's compression is checked,
  the first data page is checked to be `DATA_PAGE` vs `DATA_PAGE_V2`, and the
  whole file is decoded again. `--dump-writes DIR` saves that probe file for
  independent tooling (pyarrow/parquet-tools) checks.

### Sampling / JSONL

- One warmup iteration (Dart default), then measure until **2 s** of samples and
  **at least 10 samples**. Each iteration is one sample, same as
  `benchmark_test`'s sampler.
- `relativeMarginOfError` uses the same Student-t table as
  `benchmark_test` (`student_t_table.dart`, `1.96` above 30 degrees of freedom).
- JSONL fields match `BenchmarkResult.toJson()` in `benchmark_test` 0.1.2:
  `formatVersion`, `name`, `compiler: "rust"`, `throughput{value,unit}`,
  `statistics{relativeMarginOfError,samples}`, `latency{mean,unit}`.
  JSON object keys may be alphabetically ordered; benchcat reads by key.

## Output sample

`cargo run --release --manifest-path benchmark/rust/Cargo.toml -- --jsonl target/rust-full.jsonl`
on Windows (14 rows, single-thread default). Representative lines:

```json
{"compiler":"rust","formatVersion":1,"latency":{"mean":3840.2593090211135,"unit":"microseconds"},"name":"read read.flat.snappy","statistics":{"relativeMarginOfError":1.4129819471151412,"samples":521},"throughput":{"unit":"ops/sec","value":260.3990823356408}}
{"compiler":"rust","formatVersion":1,"latency":{"mean":8.032081038632148,"unit":"microseconds"},"name":"read read.metadata.snappy","statistics":{"relativeMarginOfError":0.14636119137304943,"samples":246401},"throughput":{"unit":"ops/sec","value":124500.73588529164}}
{"compiler":"rust","formatVersion":1,"latency":{"mean":23642.65882352941,"unit":"microseconds"},"name":"write write.flat.snappy","statistics":{"relativeMarginOfError":2.036343765559868,"samples":85},"throughput":{"unit":"ops/sec","value":42.29642729542711}}
{"compiler":"rust","formatVersion":1,"latency":{"mean":121075.3411764706,"unit":"microseconds"},"name":"write write.flat.brotli","statistics":{"relativeMarginOfError":6.330554760092571,"samples":17},"throughput":{"unit":"ops/sec","value":8.259320108315638}}
{"compiler":"rust","formatVersion":1,"latency":{"mean":16379.49756097561,"unit":"microseconds"},"name":"write write.flat.snappy.v2","statistics":{"relativeMarginOfError":0.8927039012283967,"samples":123},"throughput":{"unit":"ops/sec","value":61.051933752993406}}
{"compiler":"rust","formatVersion":1,"latency":{"mean":6984.34425087108,"unit":"microseconds"},"name":"write write.nested.snappy","statistics":{"relativeMarginOfError":1.999672827226371,"samples":287},"throughput":{"unit":"ops/sec","value":143.1773641276747}}
```

Those numbers are machine-specific; only the shape is contractual.

### Independent validation of write output

Dumped files were read back with pyarrow 26.0.0 and compared value-for-value
against the source fixtures:

```
write_write.flat.snappy.parquet:      rows=100000 rgs=1 comp=['SNAPPY']       equals_source=True
write_write.flat.zstd.parquet:        rows=100000 rgs=1 comp=['ZSTD']         equals_source=True
write_write.flat.brotli.parquet:      rows=100000 rgs=1 comp=['BROTLI']       equals_source=True
write_write.flat.lz4.parquet:         rows=100000 rgs=1 comp=['LZ4']          equals_source=True
write_write.flat.uncompressed.parquet:rows=100000 rgs=1 comp=['UNCOMPRESSED'] equals_source=True
write_write.flat.snappy.v2.parquet:   rows=100000 rgs=1 comp=['SNAPPY']       equals_source=True
write_write.nested.snappy.parquet:    rows=50000  rgs=1 comp=['SNAPPY']       equals_source=True
```

## Wiring notes for CI / benchcat (Lead)

1. Build/run on the benchmark runner after `dart pub get`:
   ```bash
   cargo build --release --manifest-path benchmark/rust/Cargo.toml
   cargo run --release --manifest-path benchmark/rust/Cargo.toml -- \
       --jsonl build/bench/rust.jsonl
   ```
   Writes are included by default. Use `--no-writes` if only the read series is
   wanted, or `--only "write "` / `--only "read "` to split runs.
2. Add a benchcat series (format `benchmark_test`, e.g. label `Rust`).
3. `benchcat.toml` currently sets `max_rows = 12`, but the default run now emits
   **14 rows** (7 read + 7 write); `.mt` rows would make it 20. Raise
   `max_rows` (≥14, ideally 20) or split the reads/writes into separate
   configs/charts.
4. Runtime is ~30 s for the default 14 rows; writes add ~14 s to the previous
   read-only run.
5. Rust builds are ~2 minutes cold (mostly `zstd-sys`/`brotli`); cache
   `benchmark/rust/target` or use `Swatinem/rust-cache` with
   `workspaces: benchmark/rust -> target`.

## Gaps / notes

- The committed `nested_snappy.parquet` has `id INT32`, `lst LIST<INT32>`, and
  `st STRUCT<a INT32, b STRING>`; there is **no map column** in the current
  fixture, so the nested write benchmark exercises list + struct but not map.
  Because the input schema is read from the fixture, adding a map column (and
  updating the Dart fixture) needs no Rust code change; only the file content
  changes. The Dart benchmark names stay the same.
- No OS-specific I/O acceleration rows (io_uring, IOCP, mmap, OPFS). Both Dart
  read benchmarks and this harness operate on in-memory bytes, and writes go to
  an in-memory `Vec<u8>`, so file-syscall performance is intentionally out of
  scope.
- No write `.mt`/parallel rows: the Dart writer suite is single-threaded, so
  there is no counterpart benchmark to mirror.
- No JIT/AOT split: Rust is compiled once in release mode. The JSONL
  `compiler` field is `rust`; label the benchcat series accordingly.
- The writer sets `created_by` to the arrow-rs default and skips the Arrow IPC
  schema metadata, so produced files differ from Dart's in footer key/value
  metadata. This is metadata volume only and is included in both writers'
  timing; output values and Parquet schema are equivalent.
