/// Browser storage adapters: OPFS, IndexedDB and Blob/File sources.
///
/// These tests need a real browser; `dart test -p chrome` provides OPFS and
/// IndexedDB in a fresh profile. Files use unique logical paths and are deleted
/// in `tearDown`, so the suite is safe to re-run.
@TestOn('browser')
library;

import 'dart:js_interop';
import 'dart:typed_data';

// ignore: implementation_imports
import 'package:blit/src/io/web/js.dart' show JsBlob;
import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

import 'fixture_expectations.dart';
import 'web_fixtures.dart';

final List<String> _opfsPaths = <String>[];
final List<String> _idbPaths = <String>[];
var _pathCounter = 0;

String _uniquePath(String prefix) =>
    '$prefix-${DateTime.now().microsecondsSinceEpoch}-${_pathCounter++}';

JsBlob _blob(Uint8List bytes) => JsBlob(<JSAny>[bytes.toJS].toJS);

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
        // Already deleted, or the backend is unavailable in this browser.
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
        // Already deleted, or the backend is unavailable in this browser.
      }
    }
    _opfsPaths.clear();
    _idbPaths.clear();
  });

  group('Blob/File source', () {
    test('read-only Blob source decodes the flat matrix', () async {
      final source = await openBlobSource(
        _blob(webFixture('flat_zstd.parquet')),
      );
      try {
        final reader = await ParquetReader.open(source);
        expect(reader.numRows, 8);
        expect(
          reader.metadata.rowGroup(0).column(0).compression,
          CompressionType.zstd,
        );
        expectFlatTable(await reader.readTable());
      } finally {
        await source.close();
      }
    });

    test('Blob slice path with cacheLimit: 0', () async {
      final bytes = webFixture('flat_delta.parquet');
      final source = await BlitSource.openBlob(_blob(bytes), cacheLimit: 0);
      try {
        final reader = await ParquetReader.open(source);
        expectFlatTable(await reader.readTable());
      } finally {
        await source.close();
      }
    });

    test('Blob source streams row groups', () async {
      final bytes = webFixture('multi_rowgroup.parquet');
      final source = await openBlobSource(_blob(bytes));
      try {
        final reader = await ParquetReader.open(source);
        final sizes = <int>[];
        await for (final group in reader.readRowGroups(columns: ['id'])) {
          expect(group.columns.map((c) => c.name).toList(), <String>['id']);
          sizes.add(group.numRows);
        }
        expect(sizes, <int>[4, 4, 4]);
      } finally {
        await source.close();
      }
    });

    test('Blob source decodes nested columns', () async {
      final source = await openBlobSource(
        _blob(webFixture('nested_snappy.parquet')),
      );
      try {
        final reader = await ParquetReader.open(source);
        expectNestedTable(await reader.readTable());
      } finally {
        await source.close();
      }
    });
  });

  group('OPFS writer output', () {
    test('writes and reads back a nested file', () async {
      final path = _uniquePath('parquet-web-nested');
      _opfsPaths.add(path);
      final table = ParquetReader.fromBytes(webFixture('nested.parquet'))
          .readTableSync();

      await writeParquetTableToFile(
        table,
        path,
        options: const ParquetWriterOptions(
          compression: CompressionType.snappy,
          dataPageVersion: 2,
        ),
      );

      final source = await openSource('opfs://$path');
      try {
        final reader = await ParquetReader.open(source);
        expect(reader.numRows, 6);
        expectNestedTable(await reader.readTable());
      } finally {
        await source.close();
      }
    });

    for (final compression in allCompressionTypes) {
      test('writes and reads back ${compression.name}', () async {
        final path = _uniquePath('parquet-web-${compression.name}');
        _opfsPaths.add(path);
        final table = ParquetReader.fromBytes(webFixture('flat_snappy.parquet'))
            .readTableSync();

        await writeParquetTableToFile(
          table,
          path,
          options: ParquetWriterOptions(
            compression: compression,
            dataPageVersion: compression.index.isEven ? 1 : 2,
          ),
        );

        final source = await openSource('opfs://$path');
        try {
          final reader = await ParquetReader.open(source);
          expect(
            reader.metadata.rowGroup(0).column(0).compression,
            compression,
          );
          expectFlatTable(await reader.readTable());
        } finally {
          await source.close();
        }
      });
    }

    test('a shorter rewrite truncates the previous file', () async {
      final path = _uniquePath('parquet-web-truncate');
      _opfsPaths.add(path);
      final syncReader = ParquetReader.fromBytes(
        webFixture('multi_rowgroup.parquet'),
      );
      final full = syncReader.readTableSync();
      final firstGroup = syncReader.readRowGroupSync(0);
      await writeParquetTableToFile(full, path);

      final firstSource = await openSource('opfs://$path');
      final fullLength = await firstSource.length;
      await firstSource.close();
      expect(fullLength, greaterThan(0));

      // The second file is much shorter; writeParquetTableToFile must truncate
      // instead of leaving the previous footer bytes behind.
      await writeParquetTableToFile(
        firstGroup,
        path,
        options: const ParquetWriterOptions(),
      );

      final source = await openSource('opfs://$path');
      try {
        expect(await source.length, lessThan(fullLength));
        final outTable = await (await ParquetReader.open(source)).readTable();
        expect(outTable.numRows, 4);
        final id = outTable['id'] as Int64Column;
        expect(
          <int>[for (var i = 0; i < outTable.numRows; i++) id.getInt(i)],
          <int>[0, 1, 2, 3],
        );
      } finally {
        await source.close();
      }
    });
  });

  group('IndexedDB writer output', () {
    test('writeParquetTableToBytesFile writes an IDB-backed file', () async {
      final path = _uniquePath('parquet-web-idb');
      _idbPaths.add(path);
      final table = ParquetReader.fromBytes(webFixture('flat_zstd.parquet'))
          .readTableSync();

      final handle = await BlitSource.openIndexedDb(
        path,
        create: true,
        readOnly: false,
      );
      try {
        await writeParquetTableToBytesFile(
          table,
          handle.file,
          options: const ParquetWriterOptions(
            compression: CompressionType.zstd,
            dataPageVersion: 2,
          ),
        );
        expect(handle.file.isClosed, isFalse);
      } finally {
        await handle.close();
      }

      final source = await openSource('idb://$path');
      try {
        final reader = await ParquetReader.open(source);
        expect(reader.numRows, 8);
        expect(
          reader.metadata.rowGroup(0).column(0).compression,
          CompressionType.zstd,
        );
        expectFlatTable(await reader.readTable());
      } finally {
        await source.close();
      }
    });

    test('IDB handles nested output and repeat reads', () async {
      final path = _uniquePath('parquet-web-idb-nested');
      _idbPaths.add(path);
      final table = ParquetReader.fromBytes(webFixture('nested_snappy.parquet'))
          .readTableSync();

      final handle = await BlitSource.openIndexedDb(
        path,
        create: true,
        readOnly: false,
      );
      try {
        await writeParquetTableToBytesFile(table, handle.file);
      } finally {
        await handle.close();
      }

      for (var attempt = 0; attempt < 2; attempt++) {
        final source = await openSource('idb://$path');
        try {
          expectNestedTable(
            await (await ParquetReader.open(source)).readTable(),
          );
        } finally {
          await source.close();
        }
      }
    });
  });

  group('capability helpers', () {
    test('openSource explains missing OPFS files', () async {
      final path = _uniquePath('parquet-web-missing');
      await expectLater(
        openSource('opfs://$path'),
        throwsA(
          predicate<Object>(
            (error) => error.toString().contains('NotFound'),
            'a NotFoundError-style web file-system error',
          ),
        ),
      );
    });

    test('openSourceSync rejects browser-only schemes', () {
      expect(
        () => openSourceSync('opfs://whatever'),
        throwsA(isA<ParquetUnsupportedException>()),
      );
      expect(
        () => openSourceSync('idb://whatever'),
        throwsA(isA<ParquetUnsupportedException>()),
      );
    });
  });
}
