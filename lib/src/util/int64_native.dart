import 'dart:typed_data';

/// Exact 64-bit buffer backed by `Int64List` (Dart VM).
final class Int64Buffer {
  Int64Buffer(int length) : _values = Int64List(length);

  final Int64List _values;

  bool get isNative => true;
  int get length => _values.length;
  Int64List get int64List => _values;

  int getInt(int index) => _values[index];

  BigInt getBigInt(int index) => BigInt.from(_values[index]);

  void setInt(int index, int value) => _values[index] = value;

  void setBigInt(int index, BigInt value) => _values[index] = value.toInt();

  /// Reads the raw two's-complement limbs.
  (int lo, int hi) getLimbs(int index) {
    final v = _values[index];
    return (v & 0xFFFFFFFF, (v >> 32) & 0xFFFFFFFF);
  }

  /// Writes raw two's-complement limbs (unsigned 32-bit each).
  void setLimbs(int index, int lo, int hi) {
    // Bitwise assembly yields the signed two's-complement int on the VM.
    _values[index] = ((hi & 0xFFFFFFFF) << 32) | (lo & 0xFFFFFFFF);
  }
}
