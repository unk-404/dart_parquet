import 'dart:io';
import 'dart:typed_data';

import 'package:benchmark_test/benchmark_test.dart';
import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

Uint8List _read(String name) =>
    File('benchmark/fixtures/$name').readAsBytesSync();

void main() {
  late Uint8List flatSnappy;
  late Uint8List flatZstd;
  late Uint8List flatBrotli;
  late Uint8List flatLz4;
  late Uint8List nestedSnappy;
  late ParquetTable flatTable;
  late ParquetTable nestedTable;

  setUpAll(() {
    flatSnappy = _read('flat_snappy.parquet');
    flatZstd = _read('flat_zstd.parquet');
    flatBrotli = _read('flat_brotli.parquet');
    flatLz4 = _read('flat_lz4.parquet');
    nestedSnappy = _read('nested_snappy.parquet');
    flatTable = ParquetReader.fromBytes(flatSnappy).readTableSync();
    nestedTable = ParquetReader.fromBytes(nestedSnappy).readTableSync();
  });

  group('read', () {
    benchmark(
      'read.flat.snappy',
      () => ParquetReader.fromBytes(flatSnappy).readTableSync(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.zstd',
      () => ParquetReader.fromBytes(flatZstd).readTableSync(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.brotli',
      () => ParquetReader.fromBytes(flatBrotli).readTableSync(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.lz4',
      () => ParquetReader.fromBytes(flatLz4).readTableSync(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.snappy.projection.i64',
      () => ParquetReader.fromBytes(flatSnappy).readTableSync(columns: ['i64']),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.nested.snappy',
      () => ParquetReader.fromBytes(nestedSnappy).readTableSync(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.metadata.snappy',
      () => ParquetReader.fromBytes(flatSnappy).metadata,
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.snappy.mt',
      () => ParquetReader.fromBytes(
        flatSnappy,
        options: const ParquetReaderOptions(parallelism: 4),
      ).readTable(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.zstd.mt',
      () => ParquetReader.fromBytes(
        flatZstd,
        options: const ParquetReaderOptions(parallelism: 4),
      ).readTable(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.brotli.mt',
      () => ParquetReader.fromBytes(
        flatBrotli,
        options: const ParquetReaderOptions(parallelism: 4),
      ).readTable(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.lz4.mt',
      () => ParquetReader.fromBytes(
        flatLz4,
        options: const ParquetReaderOptions(parallelism: 4),
      ).readTable(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.flat.snappy.projection.i64.mt',
      () => ParquetReader.fromBytes(
        flatSnappy,
        options: const ParquetReaderOptions(columns: ['i64'], parallelism: 4),
      ).readTable(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'read.nested.snappy.mt',
      () => ParquetReader.fromBytes(
        nestedSnappy,
        options: const ParquetReaderOptions(parallelism: 4),
      ).readTable(),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
  });

  group('write', () {
    benchmark(
      'write.flat.snappy',
      () => writeParquetTable(
        flatTable,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
        ),
      ),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'write.flat.zstd',
      () => writeParquetTable(
        flatTable,
        options: const ParquetWriterOptions(compression: CompressionType.zstd),
      ),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'write.flat.brotli',
      () => writeParquetTable(
        flatTable,
        options: const ParquetWriterOptions(
          compression: CompressionType.brotli,
        ),
      ),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'write.flat.lz4',
      () => writeParquetTable(
        flatTable,
        options: const ParquetWriterOptions(
          compression: CompressionType.lz4Raw,
        ),
      ),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'write.flat.uncompressed',
      () => writeParquetTable(flatTable),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'write.flat.snappy.v2',
      () => writeParquetTable(
        flatTable,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
        ),
      ),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
    benchmark(
      'write.nested.snappy',
      () => writeParquetTable(
        nestedTable,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
        ),
      ),
      minDuration: const Duration(seconds: 2),
      minSamples: 10,
    );
  });
}
