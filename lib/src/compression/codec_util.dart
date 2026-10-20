/// Validation and error-wrapping helpers shared by the codec implementations.
///
/// Codecs are fed page bodies taken from a file, so every length they trust is
/// attacker-controlled. Two rules apply to all of them:
///
///  * never allocate more than the page header's `uncompressed_page_size`;
///  * turn third-party codec errors into [ParquetFormatException].
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';

/// Validates the `uncompressed_page_size` field of a page header.
///
/// Returns [uncompressedSize] so it can be used inline.
int checkUncompressedSize(int uncompressedSize) {
  if (uncompressedSize < 0) {
    throw ParquetFormatException(
      'negative uncompressed size: $uncompressedSize',
    );
  }
  return uncompressedSize;
}

/// Throws [ParquetFormatException] unless [actual] matches [expected].
void checkDecodedLength(String codec, int actual, int expected) {
  if (actual != expected) {
    throw ParquetFormatException(
      '$codec decoded $actual bytes, page header declares $expected',
    );
  }
}

/// Runs [body], converting third-party codec failures into
/// [ParquetFormatException].
///
/// [ParquetException]s are rethrown untouched so nested validation errors keep
/// their specific type. Only [Exception]s are converted; [Error]s (including
/// out-of-memory and programmer errors) propagate unchanged.
T guardDecode<T>(String codec, T Function() body) {
  try {
    return body();
  } on ParquetException {
    rethrow;
  } on Exception catch (e, stackTrace) {
    throw ParquetFormatException('$codec decompression failed', e, stackTrace);
  }
}

/// Runs [body], converting third-party codec failures into [ParquetException].
T guardEncode<T>(String codec, T Function() body) {
  try {
    return body();
  } on ParquetException {
    rethrow;
  } on Exception catch (e, stackTrace) {
    throw ParquetException('$codec compression failed', e, stackTrace);
  }
}

/// Wraps codec output without copying it.
Bytes ownBytes(Uint8List data) => Bytes.fromUint8List(data);
