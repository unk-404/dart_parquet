# Reproducibly fetch the apache/parquet-testing subset used by the Dart interop
# tests over plain HTTPS (no git/gh, no clone).
#
#   pwsh tool/interop/fetch_parquet_testing.ps1
#   pwsh tool/interop/fetch_parquet_testing.ps1 -OutDir .agents/parquet-testing -Verify
#
# The committed fixtures under test/fixtures/interop/ are copied from this
# download; their SHA-256 hashes are recorded in
# test/fixtures/interop/expected/manifest.json. This script is a convenience for
# refreshing/auditing the subset, not a CI dependency.
param(
  [string]$OutDir = ".agents/parquet-testing",
  [switch]$Verify
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$commit = '3acde70509f990c4ddfe34bf3ab05890657e766c'
$base = "https://raw.githubusercontent.com/apache/parquet-testing/$commit/"
$files = @(
  'LICENSE.txt',
  'data/alltypes_dictionary.parquet',
  'data/alltypes_plain.parquet',
  'data/alltypes_tiny_pages.parquet',
  'data/binary.parquet',
  'data/bson.parquet',
  'data/byte_array_decimal.parquet',
  'data/byte_stream_split.zstd.parquet',
  'data/byte_stream_split_extended.gzip.parquet',
  'data/column_chunk_key_value_metadata.parquet',
  'data/concatenated_gzip_members.parquet',
  'data/data_index_bloom_encoding_stats.parquet',
  'data/data_index_bloom_encoding_with_length.parquet',
  'data/datapage_v1-corrupt-checksum.parquet',
  'data/datapage_v1-uncompressed-checksum.parquet',
  'data/datapage_v2.snappy.parquet',
  'data/datapage_v2_empty_datapage.snappy.parquet',
  'data/delta_binary_packed.parquet',
  'data/delta_byte_array.parquet',
  'data/delta_encoding_optional_column.parquet',
  'data/delta_encoding_required_column.parquet',
  'data/delta_length_byte_array.parquet',
  'data/encrypt_columns_and_footer.parquet.encrypted',
  'data/fixed_length_decimal.parquet',
  'data/fixed_length_decimal_legacy.parquet',
  'data/flba12_timestamp.parquet',
  'data/float16_nonzeros_and_nans.parquet',
  'data/float16_zeros_and_nans.parquet',
  'data/hadoop_lz4_compressed.parquet',
  'data/incorrect_map_schema.parquet',
  'data/int32_decimal.parquet',
  'data/int32_with_null_pages.parquet',
  'data/int32_with_uuid_logical_type.parquet',
  'data/int64_decimal.parquet',
  'data/int96_from_spark.parquet',
  'data/int96_timestamp_order.parquet',
  'data/json.parquet',
  'data/list_columns.parquet',
  'data/lz4_raw_compressed.parquet',
  'data/map_no_value.parquet',
  'data/nan_in_stats.parquet',
  'data/nation.dict-malformed.parquet',
  'data/nested_lists.snappy.parquet',
  'data/nested_maps.snappy.parquet',
  'data/nested_structs.rust.parquet',
  'data/non_hadoop_lz4_compressed.parquet',
  'data/nonnullable.impala.parquet',
  'data/null_list.parquet',
  'data/nullable.impala.parquet',
  'data/nulls.snappy.parquet',
  'data/old_list_structure.parquet',
  'data/page_v2_empty_compressed.parquet',
  'data/repeated_no_annotation.parquet',
  'data/repeated_primitive_no_list.parquet',
  'data/rle-dict-snappy-checksum.parquet',
  'data/rle_boolean_encoding.parquet',
  'data/single_nan.parquet',
  'data/sort_columns.parquet',
  'data/unknown-logical-type.parquet',
  'data/uniform_encryption.parquet.encrypted',
  'bad_data/ARROW-GH-45185.parquet',
  'bad_data/ARROW-RS-GH-11261-FLBA-DICT.parquet',
  'bad_data/ARROW-RS-GH-6229-DICTHEADER.parquet',
  'bad_data/ARROW-RS-GH-6229-LEVELS.parquet',
  'bad_data/PARQUET-1481.parquet'
)

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$total = 0
foreach ($file in $files) {
  $dest = Join-Path $OutDir $file
  New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dest) | Out-Null
  if (-not (Test-Path $dest)) {
    Invoke-WebRequest -Uri ($base + $file) -OutFile $dest -TimeoutSec 120
  }
  $total += (Get-Item $dest).Length
}
Write-Host ("downloaded {0} files, {1:N0} bytes from parquet-testing@{2}" -f `
  $files.Count, $total, $commit)

if ($Verify) {
  $manifestPath = Join-Path $PSScriptRoot '../../test/fixtures/interop/expected/manifest.json'
  $manifest = Get-Content $manifestPath -Raw | ConvertFrom-Json
  $failures = 0
  foreach ($entry in $manifest.fixtures) {
    $rel = $entry.file
    if ($rel -like 'generated/*') { continue }
    if ($rel -like 'official/*') { $rel = 'data/' + $rel.Substring('official/'.Length) }
    $local = Join-Path $OutDir $rel
    if (-not (Test-Path $local)) {
      Write-Host "MISSING $rel"
      $failures++
      continue
    }
    $actual = (Get-FileHash -Algorithm SHA256 $local).Hash.ToLowerInvariant()
    if ($actual -ne $entry.sha256) {
      Write-Host "HASH MISMATCH $rel"
      $failures++
    }
  }
  if ($failures -gt 0) {
    Write-Error "$failures fixture(s) failed provenance verification"
  }
  Write-Host "provenance verified against expected/manifest.json"
}
