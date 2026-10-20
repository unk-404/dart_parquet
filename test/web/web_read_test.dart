@TestOn('browser')
library;

import 'dart:convert';
import 'dart:js_interop';

// ignore: implementation_imports
import 'package:blit/src/io/web/js.dart' show JsBlob;
import 'package:parquet/parquet.dart';
import 'package:test/test.dart';

const _fixtureBase64 =
    'UEFSMRUEFUAVNkwVCBUAEgAAIAQBAAkBCQhEIAD+/////////yoAAAAAAAAAFQAVFBUYLBUIFRAVBhUGHBgIAQAAAAAAIAAYCP7/////////FgAoCAEAAAAAACAAGAj+/////////xERAAAACiQCAAAACAECA+QAFQQVPBVATBUIFQASAAAedAUAAABhbHBoYQQAAABiZXRhAAAAAAUAAABkZWx0YRUAFRQVGCwVCBUQFQYVBhw2ACgFZGVsdGEYABERAAAACiQCAAAACAECA+QAFQAVDhUSLBUIFQAVBhUGHBgBARgBABYCKAEBGAEAEREAAAAHGAIAAAADCwUVBBUoFSxMFQoVABIAABRMAAAAAAEAAAAHAAAACAAAAAkAAAAVABUkFSgsFQ4VEBUGFQYcGAQJAAAAGAQAAAAAFgIoBAkAAAAYBAAAAAAREQAAABJEAgAAAANoAwAAAAP9PwMDQDQCFQQVGBUcTBUGFQASAAAMLAEAAAACAAAAAwAAABUAFRYVGiwVCBUQFQYVBhwYBAMAAAAYBAEAAAAWAigEAwAAABgEAQAAABERAAAACygDAAAAA6gAAgMkABUEFRQVGEwVBBUAEgAACiQBAAAAeAEAAAB5FQAVFBUYLBUIFRAVBhUGHDYEKAF5GAF4EREAAAAKJAMAAAADaAABAwIVBBmsNQAYBnNjaGVtYRUKABUEJQIYA2k2NAAVDCUCGAFzJQBMHAAAABUAJQIYAWIANQIYA2xzdBUCFQZMPAAAADUEGARsaXN0FQIAFQIlAhgHZWxlbWVudAA1AhgCc3QVBAAVAiUCGAFhABUMJQIYAWIlAEwcAAAAFggZHBlsJgAcFQQZNQAGEBkYA2k2NBUCFggW7gEW6AEmWiYIHBgIAQAAAAAAIAAYCP7/////////FgAoCAEAAAAAACAAGAj+/////////xERABksFQQVABUCABUAFRAVAgA8KQYZJgAIAAAAJgAcFQwZNQAGEBkYAXMVAhYIFqwBFrQBJswCJvABHDYAKAVkZWx0YRgAEREAGSwVBBUAFQIAFQAVEBUCADwWHBkGGSYACAAAACYAHBUAGSUGABkYAWIVAhYIFlQWWCakAzwYAQEYAQAWAigBARgBABERABkcFQAVABUCADwpBhkmAgYAAAAmABwVAhk1AAYQGTgDbHN0BGxpc3QHZWxlbWVudBUCFg4WxgEWzgEmxAQm/AMcGAQJAAAAGAQAAAAAFgIoBAkAAAAYBAAAAAAREQAZLBUEFQAVAgAVABUQFQIAPCkmCAYZRgACAAwAAAAmABwVAhk1AAYQGSgCc3QBYRUCFggWqAEWsAEmggYmygUcGAQDAAAAGAQBAAAAFgIoBAMAAAAYBAEAAAAREQAZLBUEFQAVAgAVABUQFQIAPCkGGTYCAAYAAAAmABwVDBk1AAYQGSgCc3QBYhUCFggWfhaGASauByb6Bhw2BCgBeRgBeBERABksFQQVABUCABUAFRAVAgA8FgQZBhk2AgIEAAAAFtoHFggmCBb4BwAZHBgMQVJST1c6c2NoZW1hGMAELy8vLy82Z0JBQUFRQUFBQUFBQUtBQXdBQmdBRkFBZ0FDZ0FBQUFBQkJBQU1BQUFBQ0FBSUFBQUFCQUFJQUFBQUJBQUFBQVVBQUFCRUFRQUFDQUVBQU9BQUFBQ0VBQUFBQkFBQUFPRCsvLzhBQUFFTkdBQUFBQndBQUFBRUFBQUFBZ0FBQURnQUFBQVFBQUFBQWdBQUFITjBBQUFRLy8vL0RQLy8vd0FBQVFVUUFBQUFGQUFBQUFRQUFBQUFBQUFBQVFBQUFHSUFBQUEwLy8vL01QLy8vd0FBQVFJUUFBQUFGQUFBQUFRQUFBQUFBQUFBQVFBQUFHRUFBQUFnLy8vL0FBQUFBU0FBQUFCYy8vLy9BQUFCREJRQUFBQVlBQUFBQkFBQUFBRUFBQUFRQUFBQUF3QUFBR3h6ZEFDSS8vLy9oUC8vL3dBQUFRSVFBQUFBR0FBQUFBUUFBQUFBQUFBQUJBQUFBR2wwWlcwQUFBQUFlUC8vL3dBQUFBRWdBQUFBdFAvLy93QUFBUVlRQUFBQUZBQUFBQVFBQUFBQUFBQUFBUUFBQUdJQUFBRGMvLy8vMlAvLy93QUFBUVVRQUFBQUdBQUFBQVFBQUFBQUFBQUFBUUFBQUhNQUFBQUVBQVFBQkFBQUFCQUFGQUFJQUFZQUJ3QU1BQUFBRUFBUUFBQUFBQUFCQWhBQUFBQWNBQUFBQkFBQUFBQUFBQUFEQUFBQWFUWTBBQWdBREFBSUFBY0FDQUFBQUFBQUFBRkFBQUFBABggcGFycXVldC1jcHAtYXJyb3cgdmVyc2lvbiAyNi4wLjAZbBwAABwAABwAABwAABwAABwAAAANBQAAUEFSMQ==';

void main() {
  test('web: snappy + int64 + nested read', () {
    final reader = ParquetReader.fromBytes(base64Decode(_fixtureBase64));
    expect(reader.numRows, 4);
    final table = reader.readTableSync();
    expect(table.numRows, 4);

    final i64 = table['i64'] as Int64Column;
    expect(i64.getInt(0), 1);
    expect(i64.getBigInt(1), BigInt.parse('9007199254740993'));
    expect(i64.getInt(2), -2);
    expect(i64.getInt(3), 42);

    final s = table['s'] as BinaryColumn;
    expect(
      [for (var i = 0; i < 4; i++) s.getString(i)],
      ['alpha', 'beta', '', 'delta'],
    );

    final b = table['b'] as BooleanColumn;
    expect(b.getBool(0), isTrue);
    expect(b.getBool(1), isFalse);
    expect(b.isNull(2), isTrue);
    expect(b.getBool(3), isTrue);

    final list = table['lst'] as ListColumn;
    expect(list.getList(0), isEmpty);
    expect(list.getList(1), [0]);
    expect(list.getList(2), [0, 1]);
    expect(list.getList(3), [7, 8, 9]);

    final st = table['st'] as StructColumn;
    expect(st.isNull(0), isTrue);
    expect(st.getStruct(1), {'a': 1, 'b': 'x'});
    expect(st.getStruct(2), {'a': 2, 'b': 'y'});
    expect(st.getStruct(3), {'a': 3, 'b': null});
  });

  test('web: blit Blob backend (read-only)', () async {
    final bytes = base64Decode(_fixtureBase64);
    final blob = JsBlob([bytes.toJS].toJS);
    final source = await openBlobSource(blob);
    try {
      final reader = await ParquetReader.open(source);
      final table = await reader.readTable();
      expect(table.numRows, 4);
      expect((table['i64'] as Int64Column).getInt(2), -2);
      expect((table['s'] as BinaryColumn).getString(1), 'beta');
    } finally {
      await source.close();
    }
  });

  test('web: writer writes OPFS and reads back', () async {
    final bytes = base64Decode(_fixtureBase64);
    final table = ParquetReader.fromBytes(bytes).readTableSync();
    final path = 'parquet-opfs-test-${DateTime.now().microsecondsSinceEpoch}';
    addTearDown(() async {
      try {
        final cleanup = await BlitSource.open(
          path,
          create: false,
          readOnly: false,
        );
        await cleanup.file.delete();
        await cleanup.close();
      } catch (_) {
        // Best-effort cleanup.
      }
    });

    await writeParquetTableToFile(
      table,
      path,
      options: const ParquetWriterOptions(compression: CompressionType.snappy),
    );
    final source = await openSource('opfs://$path');
    try {
      final out = await ParquetReader.open(source);
      final result = await out.readTable();
      expect(result.numRows, 4);
      expect((result['i64'] as Int64Column).getInt(2), -2);
      expect((result['s'] as BinaryColumn).getString(1), 'beta');
      expect((result['lst'] as ListColumn).getList(2), [0, 1]);
    } finally {
      await source.close();
    }
  });

  test('web: blit IndexedDB backend', () async {
    final bytes = base64Decode(_fixtureBase64);
    final path = 'parquet-web-test-${DateTime.now().microsecondsSinceEpoch}';
    addTearDown(() async {
      try {
        final cleanup = await BlitSource.openIndexedDb(
          path,
          create: false,
          readOnly: false,
        );
        await cleanup.file.delete();
        await cleanup.close();
      } catch (_) {
        // Best-effort cleanup; the test already failed for a real reason.
      }
    });

    final writer = await BlitSource.openIndexedDb(
      path,
      create: true,
      readOnly: false,
    );
    try {
      await writer.file.writeAt(0, bytes);
      await writer.file.flush();
    } finally {
      await writer.close();
    }

    final source = await openSource('idb://$path');
    try {
      final reader = await ParquetReader.open(source);
      final table = await reader.readTable();
      expect(table.numRows, 4);
      expect((table['i64'] as Int64Column).getInt(2), -2);
      expect((table['lst'] as ListColumn).getList(2), [0, 1]);
    } finally {
      await source.close();
    }
  });
}
