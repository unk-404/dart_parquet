/// Pure Dart Apache Parquet reader with VM and Web support.
///
/// ```dart
/// import 'dart:io';
/// import 'package:parquet/parquet.dart';
///
/// final bytes = File('data.parquet').readAsBytesSync();
/// final reader = ParquetReader.fromBytes(bytes);
/// final table = reader.readTableSync();
/// print(table['name'].value(0));
/// ```
library;

export 'src/format/enums.dart'
    show
        BoundaryOrder,
        CompressionType,
        ConvertedType,
        Encoding,
        FieldRepetitionType,
        PageType,
        PhysicalType;
export 'src/format/logical_type.dart';
export 'src/io/io.dart';
export 'src/io/source.dart';
export 'src/metadata/file_metadata_codec.dart';
export 'src/metadata/metadata.dart';
export 'src/predicate/predicate.dart';
export 'src/reader.dart';
export 'src/schema/schema.dart';
export 'src/table/column.dart';
export 'src/table/table.dart';
export 'src/writer/writer.dart';
export 'src/util/bytes.dart' show Bytes, Uint64;
export 'src/util/errors.dart';
