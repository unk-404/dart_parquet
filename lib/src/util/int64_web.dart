import 'dart:typed_data';

const int _mask32 = 0xFFFFFFFF;
const int _maxExact = 0x1FFFFFFFFFFFFF; // 2^53 - 1
const int _two32 = 0x100000000;

/// Exact 64-bit buffer stored as two 32-bit limbs (dart2js).
final class Int64Buffer {
  Int64Buffer(int length) : _lo = Uint32List(length), _hi = Uint32List(length);

  final Uint32List _lo;
  final Uint32List _hi;

  bool get isNative => false;
  int get length => _lo.length;
  Int64List? get int64List => null;

  int getInt(int index) {
    final hi = _hi[index];
    final lo = _lo[index];
    if ((hi & 0x80000000) == 0) {
      if (hi > 0x1FFFFF) {
        _throwNotExact(index);
      }
      return hi * _two32 + lo;
    }
    // Negative values: compute the magnitude from the two's complement.
    // Subtracting from 2^64 in doubles rounds to zero for values such as -2.
    final nhi = (~hi) & _mask32;
    final nlo = (~lo) & _mask32;
    if (nhi > 0x1FFFFF) {
      _throwNotExact(index);
    }
    final magnitude = nhi * _two32 + nlo + 1;
    if (magnitude > _maxExact) {
      _throwNotExact(index);
    }
    return -magnitude;
  }

  Never _throwNotExact(int index) {
    throw StateError(
      'int64 value at $index is not exactly representable on the web; '
      'use getBigInt()',
    );
  }

  BigInt getBigInt(int index) {
    final unsigned =
        BigInt.from(_hi[index]) * BigInt.from(0x100000000) +
        BigInt.from(_lo[index]);
    return (_hi[index] & 0x80000000) != 0
        ? unsigned - (BigInt.one << 64)
        : unsigned;
  }

  void setInt(int index, int value) {
    if (value > _maxExact || value < -_maxExact) {
      throw ArgumentError.value(value, 'value', 'not exact on the web');
    }
    if (value < 0) {
      setBigInt(index, BigInt.from(value));
      return;
    }
    _lo[index] = value % 0x100000000;
    _hi[index] = (value ~/ 0x100000000) & _mask32;
  }

  void setBigInt(int index, BigInt value) {
    final unsigned = value < BigInt.zero ? value + (BigInt.one << 64) : value;
    if (unsigned < BigInt.zero || unsigned >= (BigInt.one << 64)) {
      throw RangeError('value does not fit in 64 bits: $value');
    }
    _lo[index] = (unsigned & BigInt.from(_mask32)).toInt();
    _hi[index] = ((unsigned >> 32) & BigInt.from(_mask32)).toInt();
  }

  (int lo, int hi) getLimbs(int index) => (_lo[index], _hi[index]);

  void setLimbs(int index, int lo, int hi) {
    _lo[index] = lo & _mask32;
    _hi[index] = hi & _mask32;
  }
}
