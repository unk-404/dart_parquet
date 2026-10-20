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

  setUpAll(() {
    flatSnappy = _read('flat_snappy.parquet');
    flatZstd = _read('flat_zstd.parquet');
    flatBrotli = _read('flat_brotli.parquet');
    flatLz4 = _read('flat_lz4.parquet');
    nestedSnappy = _read('nested_snappy.parquet');
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
  });
}
