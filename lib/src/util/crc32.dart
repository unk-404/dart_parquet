/// CRC-32 (IEEE 802.3) used by Parquet page headers.
library;

import 'dart:typed_data';

final Uint32List _table = _makeTable();

Uint32List _makeTable() {
  final table = Uint32List(256);
  for (var i = 0; i < 256; i++) {
    var c = i;
    for (var bit = 0; bit < 8; bit++) {
      c = (c & 1) != 0 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1;
    }
    table[i] = c;
  }
  return table;
}

/// Computes CRC-32 over [bytes] (optionally continuing from [crc]).
///
/// The result is an unsigned 32-bit value.
int crc32(Uint8List bytes, {int crc = 0}) {
  var value = crc ^ 0xFFFFFFFF;
  for (var i = 0; i < bytes.length; i++) {
    value = _table[(value ^ bytes[i]) & 0xFF] ^ (value >>> 8);
  }
  return (value ^ 0xFFFFFFFF) & 0xFFFFFFFF;
}
