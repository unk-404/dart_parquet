# Interop tooling (optional Python, not required by CI)

Bidirectional verification for the Dart Parquet reader/writer.

| File | Purpose |
|---|---|
| `fetch_parquet_testing.ps1` | Re-download the pinned apache/parquet-testing subset over HTTPS and verify SHA-256 provenance. |
| `make_pyarrow_fixture.py` | Regenerate `test/fixtures/interop/generated/*.parquet` with pyarrow (multi-row-group flat+nested, DataPage v1/v2). |
| `gen_expected.py` | Generate `test/fixtures/interop/expected/*.json` (schema, canonical checksums, samples) from pyarrow. |
| `gen_v02_expected.py` | Generate v0.2.0 expectations for the Variant/ALP assets: physical Variant structs + upstream semantic text, ALP PLAIN-reference values, engine support and the v0.2 manifest (including deep-nesting entries). |
| `interop_canon.py` | Python canonical value encoding + FNV-1a checksums (twin of the Dart file below). |
| `interop_canonical.dart` | Dart canonical value encoding + FNV-1a checksums, shared by tests and the writer harness. |
| `write_matrix.dart` | Write flat+nested fixtures with the public Dart writer across all codecs, page versions and dictionary on/off; emits `writer_matrix.json`. |
| `verify_writer.py` | Read Dart writer output with pyarrow, DuckDB and Polars and compare canonical values/schema/row groups. |

## Environment

Dart is the only CI requirement. The Python verification is optional and was run
with the repository scratch venv (`.agents/venv`, Python 3.13):

```powershell
$env:PUB_HOSTED_URL="https://pub.dev"   # required for `dart pub get` in this environment
python -m venv .agents/venv
.agents/venv/Scripts/python.exe -m pip install -r tool/interop/requirements.txt
```

## Commands

```powershell
# Dart side: reader/writer interop tests (VM only, no Python)
dart test test/interop -j4

# Regenerate fixtures/expectations (only when fixtures or pyarrow change)
.agents/venv/Scripts/python.exe tool/interop/make_pyarrow_fixture.py
.agents/venv/Scripts/python.exe tool/interop/gen_expected.py
.agents/venv/Scripts/python.exe tool/interop/gen_v02_expected.py

# Fetch + provenance check (v0.1.2 subset and v0.2.0 Variant/ALP assets)
pwsh tool/interop/fetch_parquet_testing.ps1 -Verify

# Bidirectional writer verification
dart run tool/interop/write_matrix.dart
.agents/venv/Scripts/python.exe tool/interop/verify_writer.py
# outputs (scratch, git-ignored): .agents/interop/writer_out/
```

`write_matrix.dart` writes to `.agents/interop/writer_out` by default; pass
`--out <dir>` to change it. `verify_writer.py --dir <dir>` reads the same
manifest.

## Matrix

`write_matrix.dart` produces 58 files: 2 shapes (flat, nested) x 7 codecs
(uncompressed, snappy, gzip, brotli, zstd, LZ4_RAW, LZO) x 2 page versions x
dictionary on/off, plus a 4-row-group streamed file and a zero-row file. Source
values are a pyarrow-written table (INT64, STRING, DOUBLE incl NaN/-0.0, BOOL,
TIMESTAMP[us], DATE, DECIMAL(10,2), FLBA(4), BYTE_ARRAY, FLOAT16, LIST<INT32>,
STRUCT<a,b>, MAP<STRING,INT32>). The generated source fixture also carries a
UUID(16) column for reader coverage; it is excluded from the writer matrix so
engine-specific UUID representations do not pollute the value comparison.
Pages are forced small (`pageSize: 256`) so
multi-page decoding is exercised.

`verify_writer.py` checks the Dart manifest against three independent readers,
comparing row/row-group counts, leaf schema and per-column canonical checksums.
Result of the recorded v0.1.2 run (pyarrow 26.0.0, DuckDB 1.5.6, Polars 2.0.0):

| Engine | pass | fail | unsupported (LZO) |
|---|---|---|---|
| file metadata/schema | 58 | 0 | 0 |
| pyarrow | 50 | 0 | 8 |
| DuckDB | 50 | 0 | 8 |
| Polars | 50 | 0 | 8 |

LZO is a valid Parquet codec that the Dart writer supports, but none of
pyarrow, DuckDB or Polars ship an LZO decoder, so its 8 cases are reported as
expected-unsupported rather than failures. (The Dart reader test suite reads
Dart-written LZO files back successfully.)

Engine type normalisation: DuckDB widens FLOAT16 to FLOAT; `verify_writer.py`
re-encodes widened values to IEEE half bytes before comparing, so the
comparison stays value-exact. Polars exposes FLOAT16 as raw binary.

## Fixture coverage

`test/fixtures/interop/` contains 66 fixtures from apache/parquet-testing
(59 official, 5 malformed), plus 2 pyarrow-generated files. See
[`test/fixtures/interop/README.md`](../../test/fixtures/interop/README.md) for
provenance, licenses and expected outcomes. `test/interop/` contains:

- `official_values_test.dart` - 50 value-exact fixtures (schema, types,
  per-column checksums, samples);
- `official_unsupported_test.dart` - 10 unsupported + 6 malformed fixtures with
  typed `ParquetException` assertions (plus encrypted/limitation checks);
- `crc_test.dart` - official CRC fixtures with `ParquetReaderOptions.verifyCrc`
  (valid page CRC passes, corrupt page CRC throws `ParquetFormatException`);
- `writer_roundtrip_test.dart` - CI-native Dart writer matrix (59 tests);
- `fuzz_test.dart` - deterministic footer/Thrift/page/level/codec fuzzing
  (~450 bounded mutations; every failure is a typed `ParquetException`);
- `soak_test.dart` - bounded multi-row-group streaming memory/soak proxy:
  400,000 rows / 16 row groups / 5.9 MB file, measured RSS growth 34 MiB
  (bound: 256 MiB), three repeated streaming passes.

`dart test test/interop -j4` runs 143 tests; the full repository suite is
727 tests. `dart analyze` is clean.

## v0.2.0 assets (Variant / ALP)

`gen_v02_expected.py` consumes the committed assets under
`test/fixtures/interop/v0.2/` and writes `v0.2/expected/*.json` plus
`v0.2/manifest.json`. It also records the five existing deep-nesting fixtures
with SHA-256, current typed error and v0.2 target.

Engine gaps found while generating expectations (details in
[`test/fixtures/interop/v0.2/README.md`](../../test/fixtures/interop/v0.2/README.md)):

- Variant: pyarrow 26 reads the physical struct only (no Variant semantics);
  DuckDB 1.5 fails (`Unsupported Arrow type VARIANT`); Polars 2.0 fails
  (`LogicalType union has no variant set`). Semantic expectations therefore
  come from upstream `cases.json`/`.variant.bin`.
- ALP: whole-file reads fail in pyarrow (`Unknown encoding type.`), DuckDB
  (`TProtocolException: Invalid data`) and Polars (panic). pyarrow can read the
  in-file PLAIN reference columns (`float_plain`, `double_plain`); those are the
  expected values for the six ALP columns. The v0.2 Dart reader decodes all six
  ALP columns bit-exactly against those references.

The generated v0.2 files are small: 8 Variant Parquet files (864 B–3.4 KB),
8 `.variant.bin` payloads (4–49 B), one 218,992 B ALP Parquet and ~25 KB of
JSON/manifest.


