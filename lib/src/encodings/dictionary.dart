/// Dictionary-encoded value index decoding.
///
/// A dictionary-encoded value stream starts with a single byte containing the
/// bit width of the dictionary indices, followed by an RLE / bit-packed hybrid
/// stream (no length prefix). `PLAIN_DICTIONARY` and `RLE_DICTIONARY` share this
/// layout; only the way the dictionary page itself is decoded differs.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes;

import '../util/errors.dart';
import 'rle.dart' show decodeRleBitPacked;

/// Decodes [numValues] dictionary indices.
///
/// The first byte of [data] is the index bit width (`0..32`). Indices are
/// returned as an [Int32List]; the caller is responsible for checking them
/// against the dictionary length.
Int32List decodeDictionaryIndices(Bytes data, {required int numValues}) {
  if (numValues < 0) {
    throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
  }
  if (data.isEmpty) {
    if (numValues == 0) return Int32List(0);
    throw ParquetEofException(
      'dictionary index stream is empty but $numValues values are expected',
    );
  }
  final bitWidth = data.getUint8Unchecked(0);
  if (bitWidth > 32) {
    throw ParquetFormatException(
      'invalid dictionary index bit width $bitWidth (expected 0..32)',
    );
  }
  if (numValues == 0) return Int32List(0);

  final values = decodeRleBitPacked(
    data.slice(1),
    bitWidth: bitWidth,
    numValues: numValues,
  );
  final out = Int32List(numValues);
  for (var i = 0; i < numValues; i++) {
    out[i] = values[i];
  }
  return out;
}
