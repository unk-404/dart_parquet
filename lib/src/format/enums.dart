/// Parquet format enums (values are the on-disk Thrift values).
library;

enum PhysicalType {
  boolean(0),
  int32(1),
  int64(2),
  int96(3),
  float(4),
  double(5),
  byteArray(6),
  fixedLenByteArray(7);

  const PhysicalType(this.value);
  final int value;

  static PhysicalType fromValue(int v) => values.firstWhere(
    (e) => e.value == v,
    orElse: () => throw ArgumentError.value(v, 'physicalType'),
  );
}

enum FieldRepetitionType {
  required(0),
  optional(1),
  repeated(2);

  const FieldRepetitionType(this.value);
  final int value;

  static FieldRepetitionType fromValue(int v) => values.firstWhere(
    (e) => e.value == v,
    orElse: () => throw ArgumentError.value(v, 'repetition'),
  );
}

enum Encoding {
  plain(0),
  plainDictionary(2),
  rle(3),
  bitPacked(4),
  deltaBinaryPacked(5),
  deltaLengthByteArray(6),
  deltaByteArray(7),
  rleDictionary(8),
  byteStreamSplit(9),
  alp(10);

  const Encoding(this.value);
  final int value;

  static Encoding fromValue(int v) => values.firstWhere(
    (e) => e.value == v,
    orElse: () => throw ArgumentError.value(v, 'encoding'),
  );
}

enum CompressionType {
  uncompressed(0),
  snappy(1),
  gzip(2),
  lzo(3),
  brotli(4),
  lz4(5),
  zstd(6),
  lz4Raw(7);

  const CompressionType(this.value);
  final int value;

  static CompressionType fromValue(int v) => values.firstWhere(
    (e) => e.value == v,
    orElse: () => throw ArgumentError.value(v, 'compression'),
  );
}

enum PageType {
  dataPage(0),
  indexPage(1),
  dictionaryPage(2),
  dataPageV2(3);

  const PageType(this.value);
  final int value;

  static PageType fromValue(int v) => values.firstWhere(
    (e) => e.value == v,
    orElse: () => throw ArgumentError.value(v, 'pageType'),
  );
}

/// Legacy converted types. `null` in metadata means "not set".
enum ConvertedType {
  utf8(0),
  map(1),
  mapKeyValue(2),
  list(3),
  enumType(4),
  decimal(5),
  date(6),
  timeMillis(7),
  timeMicros(8),
  timestampMillis(9),
  timestampMicros(10),
  uint8(11),
  uint16(12),
  uint32(13),
  uint64(14),
  int8(15),
  int16(16),
  int32(17),
  int64(18),
  json(19),
  bson(20),
  interval(21);

  const ConvertedType(this.value);
  final int value;

  static ConvertedType? fromValue(int v) {
    for (final e in values) {
      if (e.value == v) return e;
    }
    return null;
  }
}

enum BoundaryOrder {
  unordered(0),
  ascending(1),
  descending(2);

  const BoundaryOrder(this.value);
  final int value;

  static BoundaryOrder fromValue(int v) => values.firstWhere(
    (e) => e.value == v,
    orElse: () => throw ArgumentError.value(v, 'boundaryOrder'),
  );
}
