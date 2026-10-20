# Benchmark report

The README card is generated in CI from two Dart `benchmark_test` runs (JIT/AOT)
and the Rust arrow-rs counterpart, then rendered by
[`benchcat`](https://github.com/unk-404/benchcat). This document records the
v0.1.1 baseline that v0.1.2 freezes for the v0.1.3 performance work.

## Method

- Fixtures: committed under [`benchmark/fixtures/`](../benchmark/fixtures/)
  (100k-row flat, 50k-row nested), four row groups for the flat case.
- Dart: `dart run benchmark_test --output jsonl --compile jit` and
  `--compile aot`, same benchmark file (`benchmark/parquet_bench_test.dart`).
- Rust: `parquet = "=60.0.0"` (arrow-rs), `benchmark/rust`, `--threads 4`;
  pinned in `Cargo.lock`.
- Each full scan builds a fresh reader, decodes every row, and folds a checksum
  so the work cannot be optimized away. Write rows time a fresh writer and
  footer; input tables are decoded once outside the timed loop.
- Units: `B/s` from the uncompressed source table size; metadata is `ops/s`.
- CI runner: `ubuntu-24.04`, Dart 3.13.5, Rust stable; the chart includes the
  RME per series.

## v0.1.1 baseline

| Operation | Dart JIT | Dart AOT | Rust |
|---|---:|---:|---:|
| Flat read Snappy | 110.7 MiB/s | 137.8 | 897.7 |
| Flat read Zstd | 71.0 | 94.7 | 667.3 |
| Flat read Brotli | 42.1 | 49.5 | 285.2 |
| Flat read LZ4_RAW | 59.3 | 65.6 | 1.03 GiB/s |
| i64 projection | 99.2 | 141.0 | 708.8 |
| Nested read Snappy | 44.9 | 37.9 | 471.3 |
| Snappy isolates | 144.5 | 190.3 | 2.19 GiB/s |
| Zstd isolates | 72.6 | 93.1 | 1.43 GiB/s |
| Brotli isolates | 42.9 | 49.1 | 673.1 |
| LZ4_RAW isolates | 97.8 | 113.3 | 2.46 GiB/s |
| i64 projection isolates | 73.1 | 105.4 | 1.49 GiB/s |
| Nested isolates | 44.5 | 38.3 | 703.7 |
| Metadata parse | 10.6k ops/s | 11.1k | 142.5k |
| Write flat Snappy | 28.8 MiB/s | 29.5 | 173.8 |
| Write flat Zstd | 21.8 | 23.0 | 134.2 |
| Write flat Brotli | 8.5 | 10.1 | 32.4 |
| Write flat LZ4_RAW | 26.4 | 26.7 | 171.6 |
| Write flat uncompressed | 33.5 | 31.8 | 208.4 |
| Write flat Snappy v2 | 29.9 | 28.8 | 174.0 |
| Write nested Snappy | 7.6 | 7.5 | 104.4 |

Observations:

- Rust is 4–8× ahead on full scans and writes, and ~10× on metadata.
- Isolates help Snappy/LZ4/projection but not Zstd/Brotli/nested: per-call
  `Isolate.run` startup and small row groups dominate. A long-lived worker pool
  is the main v0.1.3 target.
- Write throughput is the largest Dart gap (4–10×); the v0.1.3 plan covers
  page/index zero-copy, encoder tables, dictionary hash maps and parallel
  column encoding.

## v0.1.2 scope

v0.1.2 adds correctness/API/interop features (page index, Bloom, CRC, predicate,
I/O strategies, official fixtures) and intentionally does not change the
benchmark operations or chase the performance floors. The same chart is
regenerated from CI so v0.1.3 starts from a locked baseline.

## v0.1.3 performance gates (API-stable)

The original roadmap floors stay the target:

- Flat Snappy read ≥150 MiB/s VM; projection ≥300; Zstd ≥100; Brotli ≥80;
  LZ4_RAW ≥200; nested ≥80.
- Write flat Snappy ≥120; Zstd ≥60; Brotli ≥80.
- 4-core: read ≥2.0× and write ≥1.7× vs `parallelism: 1`; 8-core read ≥2.5×,
  write ≥2.0×.
- No individual v0.1.2 benchmark may regress >10%.
- Web stays single-threaded; Web read targets are reported separately.

## Reproduce

```sh
dart pub get
dart run benchmark_test --output jsonl --compile jit benchmark/parquet_bench_test.dart
dart run benchmark_test --output jsonl --compile aot benchmark/parquet_bench_test.dart
cargo build --release --locked --manifest-path benchmark/rust/Cargo.toml
benchmark/rust/target/release/parquet-bench --threads 4 --jsonl rust.jsonl
```
