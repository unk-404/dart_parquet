/// Variant binary encoding: metadata dictionary.
///
/// The Variant metadata is a version header followed by a dictionary of
/// UTF-8 strings used for object field names. See the
/// [Variant binary encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md#metadata-encoding).
library;

import 'dart:convert';
import 'dart:typed_data';

import '../util/errors.dart';

/// Version of the Variant binary encoding implemented by this library.
const int variantMetadataVersion = 1;

/// Compares two Variant field names by unsigned UTF-8 byte order.
///
/// The Variant spec requires object field ids to be ordered by the UTF-8 bytes
/// of their names. This is *not* the same as Dart's [String.compareTo], which
/// compares UTF-16 code units and therefore orders supplementary-plane
/// characters before some BMP characters.
int compareVariantStrings(String a, String b) {
  final aBytes = utf8.encode(a);
  final bBytes = utf8.encode(b);
  final common = aBytes.length < bBytes.length ? aBytes.length : bBytes.length;
  for (var i = 0; i < common; i++) {
    final difference = aBytes[i] - bBytes[i];
    if (difference != 0) return difference;
  }
  return aBytes.length - bBytes.length;
}

/// A decoded Variant metadata dictionary.
///
/// The dictionary is shared by a Variant value and all of its nested values.
/// When [isSorted] is `true` the strings are unique and sorted by unsigned
/// UTF-8 byte order, which allows binary-search lookups by field name.
final class VariantMetadata {
  VariantMetadata._(
    this.strings, {
    required this.isSorted,
    this.version = variantMetadataVersion,
  });

  /// Decodes a Variant metadata binary.
  ///
  /// Throws [ParquetFormatException] when the metadata is malformed and
  /// [ParquetUnsupportedException] when its version is newer than this
  /// implementation supports.
  factory VariantMetadata.fromBytes(List<int> bytes) {
    final data = bytes is Uint8List ? bytes : Uint8List.fromList(bytes);
    if (data.isEmpty) {
      throw ParquetFormatException('Variant metadata is empty');
    }

    final header = data[0];
    final version = header & 0x0F;
    final sorted = (header & 0x10) != 0;
    final offsetSize = ((header >> 6) & 0x03) + 1;

    if (version > variantMetadataVersion) {
      throw ParquetUnsupportedException(
        'Variant metadata version $version is not supported '
        '(maximum $variantMetadataVersion)',
      );
    }
    if (version != variantMetadataVersion) {
      throw ParquetFormatException('invalid Variant metadata version $version');
    }

    final dictionarySize = _readUnsigned(data, 1, offsetSize);
    final offsetsStart = 1 + offsetSize;
    final offsetCount = dictionarySize + 1;
    if (offsetCount > (data.length - offsetsStart) ~/ offsetSize) {
      throw ParquetFormatException(
        'Variant metadata is truncated: dictionary size $dictionarySize '
        'needs $offsetCount offsets of $offsetSize bytes',
      );
    }
    final dataStart = offsetsStart + offsetCount * offsetSize;
    final dataLength = data.length - dataStart;

    final offsets = List<int>.generate(
      offsetCount,
      (index) =>
          _readUnsigned(data, offsetsStart + index * offsetSize, offsetSize),
      growable: false,
    );
    if (offsets.first != 0) {
      throw ParquetFormatException(
        'Variant metadata first offset is ${offsets.first}, expected 0',
      );
    }
    if (offsets.last > dataLength) {
      throw ParquetFormatException(
        'Variant metadata last offset ${offsets.last} exceeds the '
        '$dataLength bytes of string data',
      );
    }

    final strings = <String>[];
    for (var i = 0; i < dictionarySize; i++) {
      final start = offsets[i];
      final end = offsets[i + 1];
      if (start > end) {
        throw ParquetFormatException(
          'Variant metadata offsets are not monotonic at entry $i '
          '($start > $end)',
        );
      }
      strings.add(
        _decodeUtf8(data, dataStart + start, end - start, 'entry $i'),
      );
    }

    if (sorted) {
      for (var i = 1; i < strings.length; i++) {
        if (compareVariantStrings(strings[i - 1], strings[i]) >= 0) {
          throw ParquetFormatException(
            'Variant metadata is marked sorted but entries are not unique '
            'and lexicographically ordered',
          );
        }
      }
    }

    return VariantMetadata._(
      List<String>.unmodifiable(strings),
      isSorted: sorted,
      version: version,
    );
  }

  /// Alias for [VariantMetadata.fromBytes].
  static VariantMetadata decode(List<int> bytes) =>
      VariantMetadata.fromBytes(bytes);

  /// Creates metadata from [strings].
  ///
  /// When [sort] is `true` (the default) the strings are sorted by unsigned
  /// UTF-8 byte order and duplicates are removed, and the metadata is marked
  /// sorted. When `false`, the order is preserved, duplicates are kept and the
  /// sorted flag is cleared.
  factory VariantMetadata.fromStrings(
    Iterable<String> strings, {
    bool sort = true,
  }) {
    final list = strings.toList();
    if (sort) {
      list.sort(compareVariantStrings);
      var write = 0;
      for (final value in list) {
        if (write == 0 || compareVariantStrings(list[write - 1], value) != 0) {
          list[write++] = value;
        }
      }
      list.length = write;
    }
    return VariantMetadata._(
      List<String>.unmodifiable(list),
      // The sorted flag has no meaning for an empty dictionary and omitting
      // it keeps the canonical empty encoding to three bytes.
      isSorted: sort && list.isNotEmpty,
    );
  }

  /// Creates empty metadata.
  factory VariantMetadata.empty() =>
      VariantMetadata._(const <String>[], isSorted: false);

  /// Dictionary strings in field-id order.
  final List<String> strings;

  /// Whether [strings] is marked sorted and unique.
  final bool isSorted;

  /// Metadata encoding version.
  final int version;

  /// Number of dictionary entries.
  int get length => strings.length;

  /// Whether the dictionary is empty.
  bool get isEmpty => strings.isEmpty;

  /// Returns the string for [fieldId].
  String operator [](int fieldId) {
    if (fieldId < 0 || fieldId >= strings.length) {
      throw ParquetRangeException(
        'Variant field id $fieldId out of range '
        '(dictionary size ${strings.length})',
      );
    }
    return strings[fieldId];
  }

  /// Alias for `this[fieldId]`.
  String stringAt(int fieldId) => this[fieldId];

  /// Returns the id of [fieldName], or `null` when it is not in the dictionary.
  ///
  /// Sorted dictionaries use binary search; unsorted dictionaries use a linear
  /// scan and return the first matching id.
  int? idOf(String fieldName) {
    if (isSorted) {
      var low = 0;
      var high = strings.length - 1;
      while (low <= high) {
        final middle = (low + high) >> 1;
        final comparison = compareVariantStrings(strings[middle], fieldName);
        if (comparison == 0) return middle;
        if (comparison < 0) {
          low = middle + 1;
        } else {
          high = middle - 1;
        }
      }
      return null;
    }
    for (var i = 0; i < strings.length; i++) {
      if (strings[i] == fieldName) return i;
    }
    return null;
  }

  /// Alias for [idOf].
  int? fieldId(String fieldName) => idOf(fieldName);

  /// Encodes the metadata, choosing the smallest valid offset size.
  Uint8List toBytes() {
    if (version != variantMetadataVersion) {
      throw ArgumentError.value(
        version,
        'version',
        'unsupported Variant metadata version',
      );
    }
    if (isSorted) {
      for (var i = 1; i < strings.length; i++) {
        if (compareVariantStrings(strings[i - 1], strings[i]) >= 0) {
          throw ArgumentError(
            'Variant metadata is marked sorted but entries are not unique '
            'and lexicographically ordered',
          );
        }
      }
    }

    final encodedStrings = <List<int>>[
      for (final value in strings) utf8.encode(value),
    ];
    var dataLength = 0;
    for (final encoded in encodedStrings) {
      dataLength += encoded.length;
    }
    if (dataLength > 0xFFFFFFFF) {
      throw ArgumentError('Variant metadata string data exceeds 4 GiB');
    }
    if (strings.length > 0xFFFFFFFF) {
      throw ArgumentError('Variant metadata dictionary exceeds 2^32 entries');
    }

    final offsetSize = _unsignedSize(
      dataLength > strings.length ? dataLength : strings.length,
    );
    final builder = BytesBuilder(copy: false);

    var header = variantMetadataVersion;
    if (isSorted && strings.isNotEmpty) header |= 0x10;
    header |= (offsetSize - 1) << 6;
    builder.addByte(header);

    _writeUnsigned(builder, strings.length, offsetSize);
    var offset = 0;
    _writeUnsigned(builder, 0, offsetSize);
    for (final encoded in encodedStrings) {
      offset += encoded.length;
      _writeUnsigned(builder, offset, offsetSize);
    }
    for (final encoded in encodedStrings) {
      builder.add(encoded);
    }
    return builder.takeBytes();
  }

  /// Alias for [toBytes].
  Uint8List get bytes => toBytes();

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    if (other is! VariantMetadata ||
        other.isSorted != isSorted ||
        other.version != version ||
        other.strings.length != strings.length) {
      return false;
    }
    for (var i = 0; i < strings.length; i++) {
      if (other.strings[i] != strings[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode {
    var hash = Object.hash(isSorted, version);
    for (final value in strings) {
      hash = Object.hash(hash, value);
    }
    return hash;
  }

  @override
  String toString() => 'VariantMetadata($strings, sorted: $isSorted)';
}

int _unsignedSize(int value) {
  if (value <= 0xFF) return 1;
  if (value <= 0xFFFF) return 2;
  if (value <= 0xFFFFFF) return 3;
  return 4;
}

int _readUnsigned(Uint8List bytes, int offset, int size) {
  if (offset < 0 || size < 1 || size > 4 || offset + size > bytes.length) {
    throw ParquetFormatException(
      'cannot read $size-byte unsigned value at $offset',
    );
  }
  var value = 0;
  var multiplier = 1;
  for (var i = 0; i < size; i++) {
    value += bytes[offset + i] * multiplier;
    multiplier *= 256;
  }
  return value;
}

void _writeUnsigned(BytesBuilder builder, int value, int size) {
  var remaining = value;
  for (var i = 0; i < size; i++) {
    builder.addByte(remaining & 0xFF);
    remaining ~/= 256;
  }
}

String _decodeUtf8(Uint8List bytes, int offset, int length, String context) {
  try {
    return utf8.decode(
      Uint8List.sublistView(bytes, offset, offset + length),
      allowMalformed: false,
    );
  } on FormatException catch (error) {
    throw ParquetFormatException(
      'Variant metadata $context is not valid UTF-8',
      error,
    );
  }
}
