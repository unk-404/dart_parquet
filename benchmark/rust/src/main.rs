//! arrow-rs counterpart for `benchmark/parquet_bench_test.dart`.
//!
//! Emits one JSON object per benchmark on stdout (or to `--jsonl <path>`) using
//! the same `benchmark_test` schema that benchcat consumes:
//!
//! ```json
//! {"formatVersion":1,"name":"read read.flat.snappy","compiler":"rust",
//!  "throughput":{"value":123.4,"unit":"ops/sec"},
//!  "statistics":{"relativeMarginOfError":0.7,"samples":812},
//!  "latency":{"mean":8102.5,"unit":"microseconds"}}
//! ```
//!
//! Notes:
//! - Fixtures are read from `benchmark/fixtures/` exactly like the Dart suite.
//! - File bytes are loaded once (the Dart suite reads them in `setUpAll`) and
//!   reused; each measured operation re-parses footer + decodes every row.
//! - Every full scan builds a fresh `ParquetRecordBatchReader` and folds a
//!   checksum over all decoded buffers so the work cannot be optimized away.
//! - The metadata benchmark only parses the footer (no page decode), matching
//!   `ParquetReader.fromBytes(bytes).metadata`.
//! - `--threads N` (N > 1) adds `.mt` rows that split row groups over `N`
//!   scoped OS threads. Dart v0.1.1 has no parallel reader yet, so these rows
//!   are opt-in and are not emitted with the default `--threads 1`.
//! - Write rows mirror the Dart writer suite: the source table is decoded once
//!   outside timing, then every iteration writes the full table to a fresh
//!   `Vec<u8>` through `ArrowWriter` (writer construction, all batches, footer).
//!   Codecs, 1 MiB page targets, dictionary encoding, statistics, and the v2
//!   data-page row match `ParquetWriterOptions`; writes are on by default and
//!   can be skipped with `--no-writes`.

use std::collections::HashMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_data::ArrayData;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
use parquet::arrow::ProjectionMask;
use parquet::basic::{BrotliLevel, Compression, Encoding, ZstdLevel};
use parquet::column::page::{Page, PageReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{SerializedFileReader, SerializedPageReader};
use serde_json::json;

/// Target page size used by the Dart writer defaults
/// (`ParquetWriterOptions.pageSize == 1 MiB`), applied to both data and
/// dictionary pages.
const PAGE_SIZE_LIMIT: usize = 1024 * 1024;

/// Keep the whole flat/nested fixture in one row group, like Dart's
/// `writeParquetTable` (one `writer.add(table)` call).
const MAX_ROW_GROUP_ROWS: usize = 1_000_000;

/// Dart writes pages by byte target only; arrow-rs additionally caps pages at
/// 20_000 rows. Raise the row cap so page splits are driven by the same 1 MiB
/// target.
const DATA_PAGE_ROW_COUNT_LIMIT: usize = 1_000_000;

/// Row counts of the committed fixtures (asserted at runtime).
const FLAT_ROWS: i64 = 100_000;
const NESTED_ROWS: i64 = 50_000;

/// Index of the `i64` leaf column in `flat_*.parquet` (i64, f64, s, b).
const I64_LEAF_INDEX: usize = 0;

#[derive(Clone, Copy, PartialEq, Eq)]
enum OpKind {
    /// Full table scan: every column, every row group.
    FullScan,
    /// Top-level column projection of `i64` (same row count, one column).
    ProjectionI64,
    /// Footer / schema / row-group metadata parse only.
    Metadata,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ThreadMode {
    Single,
    Multi,
}

struct Case {
    /// Exact benchmark name including the Dart `group` prefix.
    name: &'static str,
    fixture: &'static str,
    kind: OpKind,
    expected_rows: i64,
    mode: ThreadMode,
}

impl Case {
    fn single(name: &'static str, fixture: &'static str, kind: OpKind, expected_rows: i64) -> Self {
        Self {
            name,
            fixture,
            kind,
            expected_rows,
            mode: ThreadMode::Single,
        }
    }

    fn multi(name: &'static str, fixture: &'static str, kind: OpKind, expected_rows: i64) -> Self {
        Self {
            name,
            fixture,
            kind,
            expected_rows,
            mode: ThreadMode::Multi,
        }
    }
}

fn read_cases(include_multi_thread: bool) -> Vec<Case> {
    let mut cases = vec![
        Case::single(
            "read read.flat.snappy",
            "flat_snappy.parquet",
            OpKind::FullScan,
            FLAT_ROWS,
        ),
        Case::single(
            "read read.flat.zstd",
            "flat_zstd.parquet",
            OpKind::FullScan,
            FLAT_ROWS,
        ),
        Case::single(
            "read read.flat.brotli",
            "flat_brotli.parquet",
            OpKind::FullScan,
            FLAT_ROWS,
        ),
        Case::single(
            "read read.flat.lz4",
            "flat_lz4.parquet",
            OpKind::FullScan,
            FLAT_ROWS,
        ),
        Case::single(
            "read read.flat.snappy.projection.i64",
            "flat_snappy.parquet",
            OpKind::ProjectionI64,
            FLAT_ROWS,
        ),
        Case::single(
            "read read.nested.snappy",
            "nested_snappy.parquet",
            OpKind::FullScan,
            NESTED_ROWS,
        ),
        Case::single(
            "read read.metadata.snappy",
            "flat_snappy.parquet",
            OpKind::Metadata,
            FLAT_ROWS,
        ),
    ];

    // Multi-thread variants are opt-in so the default 7 rows line up 1:1 with
    // the Dart v0.1.1 sync suite. Metadata parsing is inherently serial and is
    // intentionally not mirrored as `.mt`.
    if include_multi_thread {
        cases.extend([
            Case::multi(
                "read read.flat.snappy.mt",
                "flat_snappy.parquet",
                OpKind::FullScan,
                FLAT_ROWS,
            ),
            Case::multi(
                "read read.flat.zstd.mt",
                "flat_zstd.parquet",
                OpKind::FullScan,
                FLAT_ROWS,
            ),
            Case::multi(
                "read read.flat.brotli.mt",
                "flat_brotli.parquet",
                OpKind::FullScan,
                FLAT_ROWS,
            ),
            Case::multi(
                "read read.flat.lz4.mt",
                "flat_lz4.parquet",
                OpKind::FullScan,
                FLAT_ROWS,
            ),
            Case::multi(
                "read read.flat.snappy.projection.i64.mt",
                "flat_snappy.parquet",
                OpKind::ProjectionI64,
                FLAT_ROWS,
            ),
            Case::multi(
                "read read.nested.snappy.mt",
                "nested_snappy.parquet",
                OpKind::FullScan,
                NESTED_ROWS,
            ),
        ]);
    }

    cases
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteCodec {
    Uncompressed,
    Snappy,
    Zstd,
    Brotli,
    Lz4Raw,
}

impl WriteCodec {
    fn name(self) -> &'static str {
        match self {
            WriteCodec::Uncompressed => "uncompressed",
            WriteCodec::Snappy => "snappy",
            WriteCodec::Zstd => "zstd(3)",
            WriteCodec::Brotli => "brotli(q5)",
            WriteCodec::Lz4Raw => "lz4_raw",
        }
    }

    fn compression(self) -> Result<Compression, String> {
        Ok(match self {
            WriteCodec::Uncompressed => Compression::UNCOMPRESSED,
            WriteCodec::Snappy => Compression::SNAPPY,
            WriteCodec::Zstd => Compression::ZSTD(
                ZstdLevel::try_new(3).map_err(|e| format!("invalid zstd level 3: {e}"))?,
            ),
            WriteCodec::Brotli => Compression::BROTLI(
                BrotliLevel::try_new(5).map_err(|e| format!("invalid brotli level 5: {e}"))?,
            ),
            WriteCodec::Lz4Raw => Compression::LZ4_RAW,
        })
    }

    fn matches(self, actual: Compression) -> bool {
        match self {
            WriteCodec::Uncompressed => matches!(actual, Compression::UNCOMPRESSED),
            WriteCodec::Snappy => matches!(actual, Compression::SNAPPY),
            WriteCodec::Zstd => matches!(actual, Compression::ZSTD(_)),
            WriteCodec::Brotli => matches!(actual, Compression::BROTLI(_)),
            WriteCodec::Lz4Raw => matches!(actual, Compression::LZ4_RAW),
        }
    }
}

#[derive(Clone, Copy)]
struct WriteSpec {
    codec: WriteCodec,
    /// 1 (PARQUET_1_0) or 2 (PARQUET_2_0 data pages).
    data_page_version: u8,
}

struct WriteCase {
    /// Exact benchmark name including the Dart `write` group prefix.
    name: &'static str,
    fixture: &'static str,
    expected_rows: i64,
    spec: WriteSpec,
}

fn write_cases() -> Vec<WriteCase> {
    let flat = |name, codec, data_page_version| WriteCase {
        name,
        fixture: "flat_snappy.parquet",
        expected_rows: FLAT_ROWS,
        spec: WriteSpec {
            codec,
            data_page_version,
        },
    };

    vec![
        flat("write write.flat.snappy", WriteCodec::Snappy, 1),
        flat("write write.flat.zstd", WriteCodec::Zstd, 1),
        flat("write write.flat.brotli", WriteCodec::Brotli, 1),
        flat("write write.flat.lz4", WriteCodec::Lz4Raw, 1),
        flat("write write.flat.uncompressed", WriteCodec::Uncompressed, 1),
        flat("write write.flat.snappy.v2", WriteCodec::Snappy, 2),
        WriteCase {
            name: "write write.nested.snappy",
            fixture: "nested_snappy.parquet",
            expected_rows: NESTED_ROWS,
            spec: WriteSpec {
                codec: WriteCodec::Snappy,
                data_page_version: 1,
            },
        },
    ]
}

/// Input table decoded once outside the timed region, like Dart's `setUpAll`.
struct WriteInput {
    batches: Vec<RecordBatch>,
    /// Sum of row-group `total_byte_size` values; used to pre-size the sink.
    uncompressed_bytes: usize,
}

fn load_write_input(bytes: &Bytes, case: &WriteCase) -> Result<WriteInput, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|e| format!("{}: cannot parse source fixture: {e}", case.name))?;
    let uncompressed_bytes = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|row_group| row_group.total_byte_size().max(0) as usize)
        .sum();

    let mut reader = builder
        .with_batch_size(case.expected_rows as usize)
        .build()
        .map_err(|e| format!("{}: cannot decode source fixture: {e}", case.name))?;

    let mut batches = Vec::new();
    let mut rows = 0i64;
    for batch in &mut reader {
        let batch = batch.map_err(|e| format!("{}: source decode: {e}", case.name))?;
        rows += batch.num_rows() as i64;
        batches.push(batch);
    }
    if rows != case.expected_rows {
        return Err(format!(
            "{}: expected {} source rows, decoded {rows}",
            case.name, case.expected_rows
        ));
    }
    Ok(WriteInput {
        batches,
        uncompressed_bytes,
    })
}

fn writer_properties(spec: WriteSpec) -> Result<WriterProperties, String> {
    let writer_version = if spec.data_page_version == 2 {
        WriterVersion::PARQUET_2_0
    } else {
        WriterVersion::PARQUET_1_0
    };
    Ok(WriterProperties::builder()
        .set_compression(spec.codec.compression()?)
        .set_dictionary_enabled(true)
        .set_data_page_size_limit(PAGE_SIZE_LIMIT)
        .set_dictionary_page_size_limit(PAGE_SIZE_LIMIT)
        .set_data_page_row_count_limit(DATA_PAGE_ROW_COUNT_LIMIT)
        .set_max_row_group_row_count(Some(MAX_ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::Page)
        // Dart falls back to PLAIN when the dictionary does not fit, for both
        // page versions. arrow-rs would pick DELTA_* under PARQUET_2_0, which
        // would make the `.v2` row differ in value encoding as well as page
        // format; pin PLAIN so only the data-page version changes.
        .set_encoding(Encoding::PLAIN)
        .set_writer_version(writer_version)
        .build())
}

/// Writes the whole input table to a fresh `Vec<u8>` and returns
/// `(checksum of the produced bytes, bytes)`.
///
/// The writer is created, all batches are encoded, and the footer is finalised
/// inside the timed region, mirroring Dart's `writeParquetTable`.
fn write_once(
    input: &WriteInput,
    spec: WriteSpec,
    extra_capacity: usize,
) -> Result<(u64, Vec<u8>), String> {
    let schema = input
        .batches
        .first()
        .ok_or_else(|| String::from("write input has no record batches"))?
        .schema();

    let properties = writer_properties(spec)?;
    let options = ArrowWriterOptions::new()
        .with_properties(properties)
        // Dart does not embed Arrow IPC schema metadata; skip it so both
        // writers produce a plain Parquet schema.
        .with_skip_arrow_metadata(true);

    let capacity = input.uncompressed_bytes.saturating_add(extra_capacity);
    let mut buffer: Vec<u8> = Vec::with_capacity(capacity);
    {
        let mut writer = ArrowWriter::try_new_with_options(&mut buffer, schema, options)
            .map_err(|e| format!("create writer ({}): {e}", spec.codec.name()))?;
        for batch in &input.batches {
            writer
                .write(batch)
                .map_err(|e| format!("write batch ({}): {e}", spec.codec.name()))?;
        }
        writer
            .close()
            .map_err(|e| format!("close writer ({}): {e}", spec.codec.name()))?;
    }

    let checksum = checksum_bytes(&buffer);
    Ok((checksum, buffer))
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    mix(&mut checksum, bytes.len() as u64);
    let mut index = 0;
    while index < bytes.len() {
        checksum = checksum.rotate_left(7) ^ u64::from(bytes[index]);
        index += 64;
    }
    if let Some(last) = bytes.last() {
        checksum = checksum.rotate_left(3) ^ u64::from(*last);
    }
    checksum
}

/// Reads the produced bytes back: verifies row count, single row group,
/// per-column codec, data-page version, and a complete batch scan.
fn verify_write_output(case: &WriteCase, bytes: &[u8]) -> Result<(), String> {
    let source = Bytes::copy_from_slice(bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(source.clone())
        .map_err(|e| format!("{}: produced file is unreadable: {e}", case.name))?;

    {
        let metadata = builder.metadata();
        if metadata.file_metadata().num_rows() != case.expected_rows {
            return Err(format!(
                "{}: produced {} rows, expected {}",
                case.name,
                metadata.file_metadata().num_rows(),
                case.expected_rows
            ));
        }
        if metadata.num_row_groups() != 1 {
            return Err(format!(
                "{}: produced {} row groups, expected 1 (Dart writes one table/add)",
                case.name,
                metadata.num_row_groups()
            ));
        }

        let row_group = metadata.row_group(0);
        for column in row_group.columns() {
            if !case.spec.codec.matches(column.compression()) {
                return Err(format!(
                    "{}: column {} uses compression {:?}, expected {}",
                    case.name,
                    column.column_path(),
                    column.compression(),
                    case.spec.codec.name()
                ));
            }
        }

        // Page-level check: first non-dictionary page must be DATA_PAGE or
        // DATA_PAGE_V2 as requested.
        let column = row_group.column(0);
        let mut pages = SerializedPageReader::new(
            Arc::new(source.clone()),
            column,
            row_group.num_rows() as usize,
            None,
        )
        .map_err(|e| format!("{}: cannot open page reader: {e}", case.name))?;

        let mut found_version = None;
        while let Some(page) = pages
            .get_next_page()
            .map_err(|e| format!("{}: cannot read page: {e}", case.name))?
        {
            match page {
                Page::DictionaryPage { .. } => continue,
                Page::DataPage { .. } => {
                    found_version = Some(1);
                    break;
                }
                Page::DataPageV2 { .. } => {
                    found_version = Some(2);
                    break;
                }
            }
        }
        match found_version {
            Some(version) if version == case.spec.data_page_version => {}
            Some(version) => {
                return Err(format!(
                    "{}: produced data page v{version}, expected v{}",
                    case.name, case.spec.data_page_version
                ));
            }
            None => {
                return Err(format!("{}: produced file has no data pages", case.name));
            }
        }
    }

    let mut reader = builder
        .with_batch_size(8192)
        .build()
        .map_err(|e| format!("{}: cannot read produced file: {e}", case.name))?;
    let mut rows = 0i64;
    for batch in &mut reader {
        let batch = batch.map_err(|e| format!("{}: produced decode: {e}", case.name))?;
        rows += batch.num_rows() as i64;
    }
    if rows != case.expected_rows {
        return Err(format!(
            "{}: produced decode yielded {rows} rows, expected {}",
            case.name, case.expected_rows
        ));
    }
    Ok(())
}

fn dump_write_file(directory: &PathBuf, case: &WriteCase, bytes: &[u8]) -> Result<(), String> {
    fs::create_dir_all(directory)
        .map_err(|e| format!("cannot create {}: {e}", directory.display()))?;
    let file_name = format!("{}.parquet", case.name.replace(' ', "_"));
    let path = directory.join(file_name);
    fs::write(&path, bytes).map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    eprintln!("[rust] wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

struct Config {
    fixtures: PathBuf,
    jsonl: Option<PathBuf>,
    min_samples: usize,
    min_duration: Duration,
    max_samples: usize,
    warmup: usize,
    batch_size: usize,
    threads: usize,
    only: Option<String>,
    list: bool,
    no_writes: bool,
    dump_writes: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            fixtures: PathBuf::new(),
            jsonl: None,
            min_samples: 10,
            min_duration: Duration::from_secs(2),
            max_samples: 1_000_000,
            warmup: 1,
            batch_size: 8192,
            threads: 1,
            only: None,
            list: false,
            no_writes: false,
            dump_writes: None,
        }
    }
}

const HELP: &str = "\
parquet-bench - arrow-rs counterpart for benchmark/parquet_bench_test.dart

USAGE:
    cargo run --release -- [OPTIONS]

OPTIONS:
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
    -h, --help           Print this help
";

/// Each worker gets a disjoint subset of row groups. With 4 row groups and 4
/// threads every worker owns exactly one row group.
fn decode_row_groups(
    bytes: &Bytes,
    metadata: &ArrowReaderMetadata,
    kind: OpKind,
    row_groups: &[usize],
    batch_size: usize,
) -> Result<(u64, i64), String> {
    let builder =
        ParquetRecordBatchReaderBuilder::new_with_metadata(bytes.clone(), metadata.clone());
    let builder = match kind {
        OpKind::ProjectionI64 => {
            let mask = ProjectionMask::leaves(builder.parquet_schema(), [I64_LEAF_INDEX]);
            builder.with_projection(mask)
        }
        _ => builder,
    };

    let mut reader = builder
        .with_row_groups(row_groups.to_vec())
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| format!("build reader for row groups {row_groups:?}: {e}"))?;

    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    let mut rows = 0i64;
    for batch in &mut reader {
        let batch = batch.map_err(|e| format!("decode row groups {row_groups:?}: {e}"))?;
        rows += batch.num_rows() as i64;
        checksum_batch(&batch, kind, &mut checksum)?;
    }
    Ok((checksum, rows))
}

fn scan_once(bytes: &Bytes, case: &Case, batch_size: usize) -> Result<u64, String> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|e| format!("{}: footer/schema parse failed: {e}", case.name))?;
    let builder = match case.kind {
        OpKind::ProjectionI64 => {
            let mask = ProjectionMask::leaves(builder.parquet_schema(), [I64_LEAF_INDEX]);
            builder.with_projection(mask)
        }
        _ => builder,
    };

    let mut reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|e| format!("{}: build reader: {e}", case.name))?;

    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    let mut rows = 0i64;
    for batch in &mut reader {
        let batch = batch.map_err(|e| format!("{}: decode: {e}", case.name))?;
        rows += batch.num_rows() as i64;
        checksum_batch(&batch, case.kind, &mut checksum)?;
    }

    validate_rows(case, rows)?;
    Ok(checksum)
}

fn scan_mt_once(
    bytes: &Bytes,
    case: &Case,
    threads: usize,
    batch_size: usize,
) -> Result<u64, String> {
    // Parse footer once, then decode independent row groups in parallel. This
    // mirrors how a parallel reader would be built (single metadata parse).
    let metadata = ArrowReaderMetadata::load(bytes, Default::default())
        .map_err(|e| format!("{}: footer/schema parse failed: {e}", case.name))?;
    let num_row_groups = metadata.metadata().num_row_groups();
    if num_row_groups == 0 {
        return Err(format!("{}: fixture has no row groups", case.name));
    }

    let mut assignments: Vec<Vec<usize>> = (0..threads).map(|_| Vec::new()).collect();
    for row_group in 0..num_row_groups {
        assignments[row_group % threads].push(row_group);
    }

    let mut checksum = 0u64;
    let mut rows = 0i64;
    let mut errors: Vec<String> = Vec::new();

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for row_groups in assignments {
            if row_groups.is_empty() {
                continue;
            }
            let bytes = bytes.clone();
            let metadata = metadata.clone();
            let kind = case.kind;
            handles.push(scope.spawn(move || {
                decode_row_groups(&bytes, &metadata, kind, &row_groups, batch_size)
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(Ok((partial_checksum, partial_rows))) => {
                    checksum = checksum.wrapping_add(partial_checksum);
                    rows += partial_rows;
                }
                Ok(Err(error)) => errors.push(error),
                Err(_) => errors.push(format!("{}: worker thread panicked", case.name)),
            }
        }
    });

    if let Some(error) = errors.into_iter().next() {
        return Err(error);
    }
    validate_rows(case, rows)?;
    Ok(checksum)
}

fn validate_rows(case: &Case, rows: i64) -> Result<(), String> {
    if rows != case.expected_rows {
        return Err(format!(
            "{}: expected {} rows in {}, decoded {rows}",
            case.name, case.expected_rows, case.fixture
        ));
    }
    Ok(())
}

fn checksum_batch(batch: &RecordBatch, kind: OpKind, checksum: &mut u64) -> Result<(), String> {
    if kind == OpKind::ProjectionI64 && batch.num_columns() != 1 {
        return Err(format!(
            "projection i64 decoded {} columns, expected 1",
            batch.num_columns()
        ));
    }
    mix(checksum, batch.num_rows() as u64);
    mix(checksum, batch.num_columns() as u64);
    for column in batch.columns() {
        checksum_array(&column.to_data(), checksum);
    }
    Ok(())
}

/// Touch the decoded buffers so the optimizer must keep the decode work.
/// Sampling every 16th byte keeps the checksum far below the decode cost while
/// still depending on the actual values produced.
fn checksum_array(data: &ArrayData, checksum: &mut u64) {
    mix(checksum, data.len() as u64);
    mix(checksum, data.null_count() as u64);
    for buffer in data.buffers() {
        let bytes = buffer.as_slice();
        let mut i = 0;
        while i < bytes.len() {
            *checksum = checksum.rotate_left(7) ^ u64::from(bytes[i]);
            i += 16;
        }
        if let Some(last) = bytes.last() {
            *checksum = checksum.rotate_left(3) ^ u64::from(*last);
        }
    }
    for child in data.child_data() {
        checksum_array(child, checksum);
    }
}

#[inline]
fn mix(checksum: &mut u64, value: u64) {
    *checksum ^= value
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(*checksum << 6)
        .wrapping_add(*checksum >> 2);
}

fn metadata_once(bytes: &Bytes, expected_rows: i64) -> Result<u64, String> {
    let reader = SerializedFileReader::new(bytes.clone())
        .map_err(|e| format!("metadata parse failed: {e}"))?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();

    let mut checksum = 0xcbf2_9ce4_8422_2325u64;
    mix(&mut checksum, file_metadata.version() as u64);
    mix(&mut checksum, file_metadata.num_rows() as u64);
    mix(&mut checksum, metadata.num_row_groups() as u64);
    mix(
        &mut checksum,
        file_metadata.schema_descr().num_columns() as u64,
    );
    if let Some(created_by) = file_metadata.created_by() {
        for byte in created_by.bytes() {
            checksum = checksum.rotate_left(5) ^ u64::from(byte);
        }
    }
    for row_group in metadata.row_groups() {
        mix(&mut checksum, row_group.num_rows() as u64);
        mix(&mut checksum, row_group.num_columns() as u64);
        mix(&mut checksum, row_group.total_byte_size() as u64);
    }

    if file_metadata.num_rows() != expected_rows {
        return Err(format!(
            "metadata: expected {} rows, footer reports {}",
            expected_rows,
            file_metadata.num_rows()
        ));
    }
    Ok(checksum)
}

struct Stats {
    samples: usize,
    mean_micros: f64,
    relative_margin_of_error: f64,
}

fn measure<F>(config: &Config, mut operation: F) -> Result<Stats, String>
where
    F: FnMut() -> Result<u64, String>,
{
    for _ in 0..config.warmup {
        operation()?;
    }

    let mut durations_ns: Vec<f64> = Vec::new();
    let measured_start = Instant::now();
    loop {
        let start = Instant::now();
        // The checksum is intentionally discarded after timing: it exists to
        // force the decode to happen, not to be validated across iterations.
        let _checksum = operation()?;
        durations_ns.push(start.elapsed().as_nanos() as f64);

        if durations_ns.len() < config.min_samples {
            continue;
        }
        if measured_start.elapsed() >= config.min_duration {
            break;
        }
        if durations_ns.len() >= config.max_samples {
            break;
        }
    }

    Ok(statistics(&durations_ns))
}

fn statistics(durations_ns: &[f64]) -> Stats {
    let samples = durations_ns.len();
    let mean_ns = durations_ns.iter().sum::<f64>() / samples as f64;
    let mean_micros = mean_ns / 1_000.0;

    let relative_margin_of_error = if samples < 2 || mean_ns == 0.0 {
        0.0
    } else {
        let variance = durations_ns
            .iter()
            .map(|duration| {
                let delta = duration - mean_ns;
                delta * delta
            })
            .sum::<f64>()
            / (samples as f64 - 1.0);
        let standard_error = variance.sqrt() / (samples as f64).sqrt();
        let critical = student_t(samples - 1);
        (standard_error * critical / mean_ns) * 100.0
    };

    Stats {
        samples,
        mean_micros,
        relative_margin_of_error,
    }
}

/// Critical values used by `benchmark_test`'s `student_t_table.dart`.
fn student_t(degrees_of_freedom: usize) -> f64 {
    const TABLE: [f64; 31] = [
        12.706, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201,
        2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064,
        2.060, 2.056, 2.052, 2.048, 2.045, 2.042,
    ];
    if degrees_of_freedom < TABLE.len() {
        TABLE[degrees_of_freedom]
    } else {
        1.96
    }
}

fn load_fixture(fixtures_dir: &PathBuf, name: &str) -> Result<Bytes, String> {
    let path = fixtures_dir.join(name);
    let data =
        fs::read(&path).map_err(|e| format!("failed to read fixture {}: {e}", path.display()))?;
    if data.is_empty() {
        return Err(format!("fixture {} is empty", path.display()));
    }
    Ok(Bytes::from(data))
}

fn default_fixtures_dir() -> Result<PathBuf, String> {
    let mut directory =
        env::current_dir().map_err(|e| format!("cannot determine current directory: {e}"))?;
    loop {
        let candidate = directory.join("benchmark").join("fixtures");
        if candidate.is_dir() {
            return Ok(candidate);
        }
        if !directory.pop() {
            break;
        }
    }
    Err(String::from(
        "could not locate benchmark/fixtures from the current directory; \
         run from the repository root or pass --fixtures <DIR>",
    ))
}

fn parse_args() -> Result<Config, String> {
    let mut config = Config::default();
    let mut args = env::args().skip(1);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--fixtures" => {
                config.fixtures = PathBuf::from(next_value(&mut args, "--fixtures")?);
            }
            "--jsonl" => {
                config.jsonl = Some(PathBuf::from(next_value(&mut args, "--jsonl")?));
            }
            "--threads" => {
                let value = next_value(&mut args, "--threads")?;
                config.threads = if value == "auto" {
                    std::thread::available_parallelism()
                        .map(|count| count.get())
                        .unwrap_or(1)
                        .min(8)
                } else {
                    parse_number(&value, "--threads")?.max(1)
                };
            }
            "--samples" => {
                let value = next_value(&mut args, "--samples")?;
                config.min_samples = parse_number(&value, "--samples")?.max(1);
            }
            "--duration-ms" => {
                let value = next_value(&mut args, "--duration-ms")?;
                config.min_duration =
                    Duration::from_millis(parse_number(&value, "--duration-ms")? as u64);
            }
            "--max-samples" => {
                let value = next_value(&mut args, "--max-samples")?;
                config.max_samples = parse_number(&value, "--max-samples")?.max(1);
            }
            "--warmup" => {
                let value = next_value(&mut args, "--warmup")?;
                config.warmup = parse_number(&value, "--warmup")?;
            }
            "--batch-size" => {
                let value = next_value(&mut args, "--batch-size")?;
                config.batch_size = parse_number(&value, "--batch-size")?.max(1);
            }
            "--only" => {
                config.only = Some(next_value(&mut args, "--only")?);
            }
            "--list" => config.list = true,
            "--no-writes" => config.no_writes = true,
            "--dump-writes" => {
                config.dump_writes = Some(PathBuf::from(next_value(&mut args, "--dump-writes")?));
            }
            // Kept as a compatibility no-op: writes are on by default now.
            "--include-writes" => {}
            "-h" | "--help" => {
                print!("{HELP}");
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument `{other}`; use --help for usage"));
            }
        }
    }

    if config.fixtures.as_os_str().is_empty() {
        config.fixtures = default_fixtures_dir()?;
    }
    if config.max_samples < config.min_samples {
        config.max_samples = config.min_samples;
    }
    Ok(config)
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}"))
}

fn parse_number(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid number `{value}` for {flag}"))
}

fn write_json_line(writer: &mut dyn Write, name: &str, stats: &Stats) -> Result<(), String> {
    let mean_micros = if stats.mean_micros.is_finite() && stats.mean_micros > 0.0 {
        stats.mean_micros
    } else {
        0.0
    };
    let operations_per_second = if mean_micros > 0.0 {
        1_000_000.0 / mean_micros
    } else {
        0.0
    };

    let line = json!({
        "formatVersion": 1,
        "name": name,
        "compiler": "rust",
        "throughput": {
            "value": operations_per_second,
            "unit": "ops/sec",
        },
        "statistics": {
            "relativeMarginOfError": stats.relative_margin_of_error,
            "samples": stats.samples,
        },
        "latency": {
            "mean": mean_micros,
            "unit": "microseconds",
        },
    });

    serde_json::to_writer(&mut *writer, &line)
        .map_err(|e| format!("failed to encode JSON for {name}: {e}"))?;
    writeln!(writer).map_err(|e| format!("failed to write JSONL: {e}"))?;
    writer
        .flush()
        .map_err(|e| format!("failed to flush JSONL: {e}"))?;
    Ok(())
}

fn run() -> Result<(), String> {
    let config = parse_args()?;

    if config.list {
        for case in read_cases(false) {
            println!("{}", case.name);
        }
        for case in read_cases(true) {
            if case.mode == ThreadMode::Multi {
                println!("{}", case.name);
            }
        }
        for case in write_cases() {
            println!("{}", case.name);
        }
        return Ok(());
    }

    let mut cases = read_cases(config.threads > 1);
    let mut writes = if config.no_writes {
        Vec::new()
    } else {
        write_cases()
    };
    if let Some(filter) = &config.only {
        cases.retain(|case| case.name.contains(filter.as_str()));
        writes.retain(|case| case.name.contains(filter.as_str()));
    }
    if cases.is_empty() && writes.is_empty() {
        return Err(String::from(
            "no benchmarks selected (check --only/--threads/--no-writes)",
        ));
    }

    let mut fixtures: HashMap<&'static str, Bytes> = HashMap::new();
    for case in &cases {
        if !fixtures.contains_key(case.fixture) {
            fixtures.insert(case.fixture, load_fixture(&config.fixtures, case.fixture)?);
        }
    }

    // Decode write inputs once, outside the timed region.
    let mut write_inputs: HashMap<&'static str, WriteInput> = HashMap::new();
    for case in &writes {
        if !write_inputs.contains_key(case.fixture) {
            let bytes = load_fixture(&config.fixtures, case.fixture)?;
            write_inputs.insert(case.fixture, load_write_input(&bytes, case)?);
        }
    }

    let mut writer: Box<dyn Write> = match &config.jsonl {
        Some(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)
                        .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
                }
            }
            let file =
                File::create(path).map_err(|e| format!("cannot create {}: {e}", path.display()))?;
            Box::new(BufWriter::new(file))
        }
        None => Box::new(BufWriter::new(io::stdout())),
    };

    eprintln!(
        "[rust] parquet 60.0.0 (arrow-rs) | fixtures: {} | read rows: {} | write rows: {} | \
         batch size: {} | threads for .mt: {}",
        config.fixtures.display(),
        cases.len(),
        writes.len(),
        config.batch_size,
        if config.threads > 1 {
            config.threads.to_string()
        } else {
            String::from("disabled")
        }
    );

    for case in &cases {
        let bytes = fixtures
            .get(case.fixture)
            .ok_or_else(|| format!("fixture {} not loaded", case.fixture))?;

        let stats = match (case.mode, case.kind) {
            (ThreadMode::Multi, _) => measure(&config, || {
                scan_mt_once(bytes, case, config.threads, config.batch_size)
            })?,
            (ThreadMode::Single, OpKind::Metadata) => {
                measure(&config, || metadata_once(bytes, case.expected_rows))?
            }
            (ThreadMode::Single, _) => {
                measure(&config, || scan_once(bytes, case, config.batch_size))?
            }
        };

        write_json_line(writer.as_mut(), case.name, &stats)?;
        eprintln!(
            "[rust] {:<44} {:>8} samples  {:>12.0} ops/s  +/-{:.2}%",
            case.name,
            stats.samples,
            1_000_000.0 / stats.mean_micros,
            stats.relative_margin_of_error
        );
    }

    // Writes: probe once outside timing to prove the produced file is valid,
    // then measure full table -> bytes writes.
    for case in &writes {
        let input = write_inputs
            .get(case.fixture)
            .ok_or_else(|| format!("write input {} not loaded", case.fixture))?;
        let extra_capacity = PAGE_SIZE_LIMIT;

        let (_, probe_bytes) = write_once(input, case.spec, extra_capacity)
            .map_err(|e| format!("{}: {e}", case.name))?;
        verify_write_output(case, &probe_bytes).map_err(|e| format!("{}: {e}", case.name))?;
        if let Some(directory) = &config.dump_writes {
            dump_write_file(directory, case, &probe_bytes)?;
        }
        eprintln!(
            "[rust] verified {} ({}, {} bytes)",
            case.name,
            case.spec.codec.name(),
            probe_bytes.len()
        );
        drop(probe_bytes);

        let stats = measure(&config, || {
            write_once(input, case.spec, extra_capacity).map(|(checksum, _produced_bytes)| checksum)
        })?;

        write_json_line(writer.as_mut(), case.name, &stats)?;
        eprintln!(
            "[rust] {:<44} {:>8} samples  {:>12.0} ops/s  +/-{:.2}%",
            case.name,
            stats.samples,
            1_000_000.0 / stats.mean_micros,
            stats.relative_margin_of_error
        );
    }

    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
