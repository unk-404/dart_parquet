/// v0.2 Web/Wasm writer-side streaming matrix.
///
/// Covers `ParquetStreamWriter` (OPFS + IndexedDB), `finish(close: false)`,
/// concurrent adds, zero-add files, resume from a crash-safe prefix + metadata
/// sidecar, `ParquetWriter.resume`, and web `IoStrategy` resolution/stubs.
/// Shared with `test/wasm/wasm_stream_test.dart`.
@TestOn('browser')
library;

import 'dart:typed_data';

import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'fixture_expectations.dart';
import 'web_fixtures.dart';

final List<String> _opfsPaths = <String>[];
final List<String> _idbPaths = <String>[];
var _counter = 0;

String _unique(String prefix) =>
    '$prefix-${DateTime.now().microsecondsSinceEpoch}-${_counter++}';

Future<Uint8List> _readAll(ParquetSource source) async {
  final length = await source.length;
  return (await source.read(0, length)).asUint8List();
}

ParquetTable _flatTable() =>
    ParquetReader.fromBytes(webFixture('flat_snappy.parquet')).readTableSync();

ParquetTable _multiTable() =>
    ParquetReader.fromBytes(webFixture('multi_rowgroup.parquet'))
        .readTableSync();

void main() {
  tearDown(() async {
    for (final path in _opfsPaths) {
      try {
        final handle = await BlitSource.open(
          path,
          create: false,
          readOnly: false,
        );
        await handle.file.delete();
        await handle.close();
      } catch (_) {
        // Already gone or OPFS unavailable.
      }
    }
    for (final path in _idbPaths) {
      try {
        final handle = await BlitSource.openIndexedDb(
          path,
          create: false,
          readOnly: false,
        );
        await handle.file.delete();
        await handle.close();
      } catch (_) {
        // Already gone or IndexedDB unavailable.
      }
    }
    _opfsPaths.clear();
    _idbPaths.clear();
  });

  group('ParquetStreamWriter on OPFS', () {
    test(
      'nested v2 output with page index, CRC and Bloom, close:false',
      () async {
        final path = _unique('v02-stream-opfs');
        _opfsPaths.add(path);
        final table = ParquetReader.fromBytes(webFixture('nested.parquet'))
            .readTableSync();
        final handle = await BlitSource.open(
          path,
          create: true,
          readOnly: false,
        );
        final writer = ParquetStreamWriter(
          table.schema,
          handle.file,
          options: const ParquetWriterOptions(
            compression: CompressionType.snappy,
            dataPageVersion: 2,
            pageSize: 64,
            writeCrc: true,
            writePageIndex: true,
            writeBloomFilter: true,
          ),
        );
        try {
          await writer.add(table);
          await writer.add(table);
          await writer.finish(close: false);
          expect(handle.file.isClosed, isFalse);
          expect(writer.numRows, table.numRows * 2);
          expect(writer.numRowGroups, 2);
          expect(() => writer.fileMetaData, throwsStateError);
        } finally {
          await handle.close();
        }

        final source = await openSource('opfs://$path');
        try {
          final reader = await ParquetReader.open(
            source,
            options: const ParquetReaderOptions(verifyCrc: true),
          );
          expect(reader.numRows, table.numRows * 2);
          expect(reader.numRowGroups, 2);
          expectNestedTable(await reader.readRowGroup(0));
          final bytes = await _readAll(source);
          final memory = MemorySource.fromUint8List(bytes);
          for (final group in reader.metadata.rowGroups) {
            for (final chunk in group.columns) {
              expect(chunk.readOffsetIndex(memory), isNotNull);
              expect(chunk.readBloomFilter(memory), isNotNull);
            }
          }
        } finally {
          await source.close();
        }
      },
    );

    test('concurrent add() calls are serialised', () async {
      final path = _unique('v02-stream-concurrent');
      _opfsPaths.add(path);
      final table = _flatTable();
      final handle = await BlitSource.open(path, create: true, readOnly: false);
      final writer = ParquetStreamWriter(table.schema, handle.file);
      try {
        final first = writer.add(table);
        final second = writer.add(table);
        await Future.wait(<Future<void>>[first, second]);
        await writer.finish(close: true);
      } finally {
        if (!handle.file.isClosed) await handle.close();
      }

      final source = await openSource('opfs://$path');
      try {
        final reader = await ParquetReader.open(source);
        expect(reader.numRowGroups, 2);
        expect(reader.numRows, table.numRows * 2);
        expectFlatTable(await reader.readRowGroup(0));
      } finally {
        await source.close();
      }
    });

    test('finish without adding rows writes a readable empty file', () async {
      final path = _unique('v02-stream-empty');
      _opfsPaths.add(path);
      final table = _flatTable();
      final handle = await BlitSource.open(path, create: true, readOnly: false);
      final writer = ParquetStreamWriter(table.schema, handle.file);
      await writer.finish(close: true);

      final source = await openSource('opfs://$path');
      try {
        final reader = await ParquetReader.open(source);
        expect(reader.numRows, 0);
        expect(reader.numRowGroups, 0);
        expect((await reader.readTable()).numRows, 0);
      } finally {
        await source.close();
      }
    });
  });

  group('ParquetStreamWriter on IndexedDB', () {
    test('writes and reads back through idb://', () async {
      final path = _unique('v02-stream-idb');
      _idbPaths.add(path);
      final table = _flatTable();
      final handle = await BlitSource.openIndexedDb(
        path,
        create: true,
        readOnly: false,
      );
      final writer = ParquetStreamWriter(
        table.schema,
        handle.file,
        options: const ParquetWriterOptions(
          compression: CompressionType.zstd,
          dataPageVersion: 2,
          writeCrc: true,
        ),
      );
      try {
        await writer.add(table);
        await writer.finish(close: false);
        expect(handle.file.isClosed, isFalse);
      } finally {
        await handle.close();
      }

      final source = await openSource('idb://$path');
      try {
        final reader = await ParquetReader.open(
          source,
          options: const ParquetReaderOptions(verifyCrc: true),
        );
        expect(reader.numRows, table.numRows);
        expectFlatTable(await reader.readTable());
      } finally {
        await source.close();
      }
    });
  });

  group('resume from a crash-safe prefix and sidecar', () {
    test(
      'ParquetStreamWriter.resume appends and keeps indexes/blooms',
      () async {
        final path = _unique('v02-resume-prefix');
        final sidecarPath = _unique('v02-resume-meta');
        _opfsPaths
          ..add(path)
          ..add(sidecarPath);
        final table = _flatTable();
        const options = ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
          pageSize: 64,
          writeCrc: true,
          writePageIndex: true,
          writeBloomFilter: true,
        );

        // First writer "crashes" after one row group: no footer is written.
        final handle = await BlitSource.open(
          path,
          create: true,
          readOnly: false,
        );
        final writer = ParquetStreamWriter(
          table.schema,
          handle.file,
          options: options,
        );
        late final FileMetaData metadata;
        late final Uint8List prefix;
        try {
          await writer.add(table);
          await handle.file.flush();
          metadata = writer.fileMetaData;
          expect(metadata.numRows, table.numRows);
          expect(metadata.rowGroups.length, 1);
          prefix = await _readAll(BlitSource(handle.file));
        } finally {
          await handle.close();
        }

        // Persist the metadata sidecar through the same OPFS backend.
        final sidecar = writeFileMetaData(metadata);
        expect(sidecar, isNotEmpty);
        final metaHandle = await BlitSource.open(
          sidecarPath,
          create: true,
          readOnly: false,
        );
        try {
          await metaHandle.file.writeAt(0, sidecar);
          await metaHandle.file.truncate(sidecar.length);
          await metaHandle.file.flush();
        } finally {
          await metaHandle.close();
        }

        final metaSource = await openSource('opfs://$sidecarPath');
        late final FileMetaData restored;
        try {
          restored = readFileMetaData(await _readAll(metaSource));
        } finally {
          await metaSource.close();
        }
        expect(restored.numRows, metadata.numRows);
        expect(restored.rowGroups.length, metadata.rowGroups.length);
        for (var i = 0; i < restored.rowGroups.length; i++) {
          expect(
            restored.rowGroups[i].columns.length,
            metadata.rowGroups[i].columns.length,
          );
        }

        final reopened = await BlitSource.open(path, readOnly: false);
        final resumed = await ParquetStreamWriter.resume(
          table.schema,
          reopened.file,
          restored,
          options: options,
        );
        try {
          expect(resumed.position, prefix.length);
          expect(resumed.numRows, table.numRows);
          expect(resumed.numRowGroups, 1);
          await resumed.add(table);
          await resumed.finish(close: true);
        } finally {
          if (!reopened.file.isClosed) await reopened.close();
        }

        final source = await openSource('opfs://$path');
        try {
          final bytes = await _readAll(source);
          expect(
            bytes.sublist(0, prefix.length),
            prefix,
            reason: 'prefix kept',
          );
          final reader = await ParquetReader.open(
            source,
            options: const ParquetReaderOptions(verifyCrc: true),
          );
          expect(reader.numRowGroups, 2);
          expect(reader.numRows, table.numRows * 2);
          expectFlatTable(await reader.readRowGroup(0));
          final memory = MemorySource.fromUint8List(bytes);
          for (final group in reader.metadata.rowGroups) {
            for (final chunk in group.columns) {
              expect(chunk.readOffsetIndex(memory), isNotNull);
              expect(chunk.readBloomFilter(memory), isNotNull);
            }
          }
        } finally {
          await source.close();
        }
      },
    );

    test(
      'ParquetWriter.resume appends in memory from the same prefix',
      () async {
        final path = _unique('v02-resume-memory');
        _opfsPaths.add(path);
        final table = _multiTable();
        final handle = await BlitSource.open(
          path,
          create: true,
          readOnly: false,
        );
        final stream = ParquetStreamWriter(table.schema, handle.file);
        late final FileMetaData metadata;
        late final Uint8List prefix;
        try {
          await stream.add(table);
          await handle.file.flush();
          metadata = stream.fileMetaData;
          prefix = await _readAll(BlitSource(handle.file));
        } finally {
          await handle.close();
        }

        final resumed = ParquetWriter.resume(prefix, metadata);
        resumed.add(table);
        final bytes = resumed.finish();
        expect(bytes.sublist(0, prefix.length), prefix, reason: 'prefix kept');
        final reader = ParquetReader.fromBytes(bytes);
        expect(reader.numRowGroups, 2);
        expect(reader.numRows, table.numRows * 2);
        for (var group = 0; group < 2; group++) {
          final decoded = reader.readRowGroupSync(group);
          expect(decoded.numRows, table.numRows);
          expect((decoded['id'] as Int64Column).getInt(0), 0);
        }
      },
    );

    test('resume rejects a complete file that already has a footer', () async {
      final path = _unique('v02-resume-complete');
      _opfsPaths.add(path);
      final table = _flatTable();
      final handle = await BlitSource.open(path, create: true, readOnly: false);
      final writer = ParquetStreamWriter(table.schema, handle.file);
      late final Uint8List complete;
      try {
        await writer.add(table);
        final metadata = writer.fileMetaData;
        await writer.finish(close: false);
        final metadataBytes = writeFileMetaData(metadata);
        complete = await _readAll(BlitSource(handle.file));
        expect(complete.length, greaterThan(8));
        // OPFS takes an exclusive handle per path: close before reopening.
        await handle.close();
        final reopened = await BlitSource.open(path, readOnly: false);
        try {
          await expectLater(
            ParquetStreamWriter.resume(
              table.schema,
              reopened.file,
              readFileMetaData(metadataBytes),
            ),
            throwsA(
              isA<ParquetFormatException>().having(
                (error) => error.toString(),
                'message',
                contains('footer'),
              ),
            ),
          );
        } finally {
          await reopened.close();
        }
      } finally {
        if (!handle.file.isClosed) await handle.close();
      }
    });
  });

  group('web IoStrategy', () {
    test('auto resolves to batched and opens OPFS', () async {
      final path = _unique('v02-io-strategy');
      _opfsPaths.add(path);
      await writeParquetTableToFile(_flatTable(), path);
      expect(
        resolveIoStrategy(IoStrategy.auto, fileLength: 4096),
        IoStrategy.batched,
      );
      final opened = await openSourceWithStrategy(
        'opfs://$path',
        strategy: IoStrategy.auto,
      );
      try {
        expect(opened.resolved, IoStrategy.batched);
        expect(opened.canReadSync, isFalse);
        final reader = await ParquetReader.open(opened);
        expect(reader.numRows, 8);
        expectFlatTable(await reader.readTable());
      } finally {
        await opened.close();
      }
    });

    test('sync/mmap are rejected on the web', () async {
      for (final strategy in <IoStrategy>[IoStrategy.sync, IoStrategy.mmap]) {
        await expectLater(
          openSourceWithStrategy('opfs://whatever', strategy: strategy),
          throwsA(isA<ParquetUnsupportedException>()),
        );
      }
      await expectLater(
        openSourceWithStrategy(
          'https://example.com/data.parquet',
          strategy: IoStrategy.auto,
        ),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });
  });
}
