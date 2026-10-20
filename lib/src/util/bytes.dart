/// Byte-level helpers shared by the format, codec and encoding layers.
///
/// The package uses blit's zero-copy [Bytes] as its canonical buffer type.
library;

import 'dart:typed_data';

import 'package:blit/blit.dart' show Bytes, BytesMut;

import 'errors.dart';

export 'package:blit/blit.dart' show Bytes, BytesMut, Uint64;

const int _mask32 = 0xFFFFFFFF;

/// Little-endian / varint reader over a zero-copy [Bytes] view.
final class ByteReader {
  ByteReader(this.bytes, {this.position = 0, int? limit})
    : limit = limit ?? bytes.length {
    if (position < 0 || position > this.limit || this.limit > bytes.length) {
      throw ParquetRangeException(
        'invalid reader range: position=$position limit=${this.limit} '
        'length=${bytes.length}',
      );
    }
  }

  final Bytes bytes;
  int position;
  final int limit;

  int get remaining => limit - position;
  bool get isEOF => position >= limit;

  void _need(int n) {
    if (n < 0 || position + n > limit) {
      throw ParquetEofException(
        'need $n bytes at $position, only $remaining available',
      );
    }
  }

  int readUint8() {
    _need(1);
    return bytes.getUint8Unchecked(position++);
  }

  Uint8List readUint8List(int length) {
    _need(length);
    final out = Uint8List.fromList(
      bytes.asUint8List().sublist(position, position + length),
    );
    position += length;
    return out;
  }

  /// Zero-copy slice; the returned view shares the reader's backing buffer.
  Bytes readSlice(int length) {
    _need(length);
    final out = bytes.slice(position, position + length);
    position += length;
    return out;
  }

  int readUint16LE() {
    _need(2);
    final i = position;
    position += 2;
    final b = bytes.asUint8List();
    return b[i] | (b[i + 1] << 8);
  }

  int readUint32LE() {
    _need(4);
    final i = position;
    position += 4;
    final b = bytes.asUint8List();
    return (b[i] | (b[i + 1] << 8) | (b[i + 2] << 16) | (b[i + 3] << 24)) &
        _mask32;
  }

  int readInt32LE() {
    final v = readUint32LE();
    return v >= 0x80000000 ? v - 0x100000000 : v;
  }

  /// Reads an unsigned 64-bit little-endian value as a Dart int.
  ///
  /// Values above 2^53 are not exact on dart2js; callers that need exact
  /// 64-bit data must use [readUint64Limbs] instead.
  int readUint64LE() {
    final lo = readUint32LE();
    final hi = readUint32LE();
    if (hi > 0x1FFFFF) {
      throw ParquetRangeException(
        'u64 value does not fit exactly in 53 bits: hi=$hi',
      );
    }
    return hi * 0x100000000 + lo;
  }

  int readInt64LE() {
    final lo = readUint32LE();
    final hi = readUint32LE();
    final unsigned =
        BigInt.from(hi) * BigInt.from(0x100000000) + BigInt.from(lo);
    final signed = (hi & 0x80000000) != 0
        ? unsigned - (BigInt.one << 64)
        : unsigned;
    if (signed.abs() > BigInt.from(0x1FFFFFFFFFFFFF)) {
      throw ParquetRangeException(
        'i64 value does not fit exactly in 53 bits: hi=$hi',
      );
    }
    return signed.toInt();
  }

  /// Reads raw u64 limbs (little-endian); safe on every platform.
  (int lo, int hi) readUint64Limbs() {
    final lo = readUint32LE();
    final hi = readUint32LE();
    return (lo, hi);
  }

  int readVarint32() {
    var result = 0;
    var shift = 0;
    while (true) {
      final b = readUint8();
      result |= (b & 0x7F) << shift;
      if (b & 0x80 == 0) return result & _mask32;
      shift += 7;
      if (shift >= 35) {
        throw ParquetFormatException('varint32 overflow at $position');
      }
    }
  }

  int readVarint64() {
    var lo = 0;
    var hi = 0;
    var shift = 0;
    while (true) {
      final b = readUint8();
      final bits = b & 0x7F;
      if (shift < 32) {
        lo |= (bits << shift) & _mask32;
        if (shift + 7 > 32) {
          hi |= bits >> (32 - shift);
        }
      } else {
        hi |= (bits << (shift - 32)) & _mask32;
      }
      if (b & 0x80 == 0) break;
      shift += 7;
      if (shift >= 70) {
        throw ParquetFormatException('varint64 overflow at $position');
      }
    }
    final value = hi * 0x100000000 + lo;
    if (hi > 0x1FFFFF) {
      throw ParquetRangeException(
        'u64 varint does not fit exactly in 53 bits: hi=$hi',
      );
    }
    return value;
  }

  int readZigZag32() {
    final n = readVarint32();
    return (n >>> 1) ^ -(n & 1);
  }

  int readZigZag64() {
    final n = readVarint64();
    return (n >>> 1) ^ -(n & 1);
  }

  void skip(int n) {
    _need(n);
    position += n;
  }
}

/// Small growable writer used by tests and the eventual writer path.
final class ByteWriter {
  final BytesMut _buffer;

  ByteWriter([int capacity = 64]) : _buffer = BytesMut(capacity);

  int get length => _buffer.length;

  Bytes get bytes => _buffer.freeze();

  void writeUint8(int v) => _buffer.putUint8(v & 0xFF);

  void writeBytes(Bytes b) => _buffer.putBytes(b);

  void writeUint8List(List<int> b) => _buffer.putSlice(b);

  void writeUint16LE(int v) {
    _buffer
      ..putUint8(v & 0xFF)
      ..putUint8((v >>> 8) & 0xFF);
  }

  void writeUint32LE(int v) {
    _buffer
      ..putUint8(v & 0xFF)
      ..putUint8((v >>> 8) & 0xFF)
      ..putUint8((v >>> 16) & 0xFF)
      ..putUint8((v >>> 24) & 0xFF);
  }

  void writeVarint32(int v) {
    var x = v & _mask32;
    while (x >= 0x80) {
      _buffer.putUint8((x & 0x7F) | 0x80);
      x >>>= 7;
    }
    _buffer.putUint8(x);
  }

  void writeZigZag32(int v) => writeVarint32(((v << 1) ^ (v >> 31)) & _mask32);

  void writeZigZag64(int v) {
    final value = BigInt.from(v);
    var encoded = (value << 1) ^ (value >> 63);
    while (encoded >= BigInt.from(0x80)) {
      _buffer.putUint8(
        ((encoded & BigInt.from(0x7F)) | BigInt.from(0x80)).toInt(),
      );
      encoded >>= 7;
    }
    _buffer.putUint8(encoded.toInt());
  }
}
