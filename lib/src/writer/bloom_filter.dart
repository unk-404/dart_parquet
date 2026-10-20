/// Parquet Split Block Bloom Filter (SBBF) with XXH64 hashing.
///
/// The filter follows the parquet-format Bloom filter specification:
///
/// * the bitset is a power-of-two number of bytes, at least 32 bytes, split
///   into 256-bit blocks (32 bytes each);
/// * a value is hashed with XXH64 (seed 0) over its PLAIN-encoded bytes (for
///   `BYTE_ARRAY` without the 4-byte length prefix);
/// * the upper 32 bits of the hash select the block and eight salted probes
///   derived from the lower 32 bits set one bit in each of the block's eight
///   32-bit words.
///
/// Everything is implemented with blit's exact [Uint64] limbs so the filter is
/// identical on the VM, dart2js and dart2wasm.
library;

import 'dart:math' as math;
import 'dart:typed_data';

import '../format/thrift.dart';
import '../util/bytes.dart';
import '../util/errors.dart';

/// A value hash used by the Bloom filter.
typedef BloomHash = Uint64;

const Uint64 _prime1 = Uint64(0x9E3779B1, 0x85EBCA87);
const Uint64 _prime2 = Uint64(0xC2B2AE3D, 0x27D4EB4F);
const Uint64 _prime3 = Uint64(0x165667B1, 0x9E3779F9);
const Uint64 _prime4 = Uint64(0x85EBCA77, 0xC2B2AE63);
const Uint64 _prime5 = Uint64(0x27D4EB2F, 0x165667C5);

/// Eight 32-bit salts from the Parquet Bloom filter specification.
const List<int> _salts = <int>[
  0x47B6137B,
  0x44974D91,
  0x8824AD5B,
  0xA2B7289D,
  0x705495C7,
  0x2DF1424B,
  0x9EFC4947,
  0x5C6BFB31,
];

Uint64 _rotl(Uint64 value, int bits) =>
    (value << bits) | (value >> (64 - bits));

Uint64 _xxhRound(Uint64 acc, Uint64 input) =>
    _rotl(acc + input * _prime2, 31) * _prime1;

Uint64 _xxhMergeRound(Uint64 acc, Uint64 value) =>
    (acc ^ _xxhRound(Uint64.zero, value)) * _prime1 + _prime4;

/// Reads an unsigned little-endian 32-bit integer exactly on every platform.
int _readUint32(Uint8List bytes, int offset) {
  final value =
      bytes[offset] |
      (bytes[offset + 1] << 8) |
      (bytes[offset + 2] << 16) |
      (bytes[offset + 3] << 24);
  // Bitwise operators are signed 32-bit on the web.
  return value < 0 ? value + 0x100000000 : value;
}

Uint64 _readUint64(Uint8List bytes, int offset) =>
    Uint64(_readUint32(bytes, offset + 4), _readUint32(bytes, offset));

/// Computes XXH64 of [bytes] with an optional [seed] (default 0).
///
/// The result is the exact unsigned 64-bit digest.
Uint64 xxHash64(Uint8List bytes, {int seed = 0}) {
  if (seed < 0) {
    throw ArgumentError.value(seed, 'seed', 'must not be negative');
  }
  final seedU = Uint64.fromInt(seed);
  final length = bytes.length;
  var offset = 0;
  Uint64 hash;
  if (length >= 32) {
    var v1 = seedU + _prime1 + _prime2;
    var v2 = seedU + _prime2;
    var v3 = seedU;
    var v4 = seedU - _prime1;
    final limit = length - 32;
    while (offset <= limit) {
      v1 = _xxhRound(v1, _readUint64(bytes, offset));
      v2 = _xxhRound(v2, _readUint64(bytes, offset + 8));
      v3 = _xxhRound(v3, _readUint64(bytes, offset + 16));
      v4 = _xxhRound(v4, _readUint64(bytes, offset + 24));
      offset += 32;
    }
    hash = _rotl(v1, 1) + _rotl(v2, 7) + _rotl(v3, 12) + _rotl(v4, 18);
    hash = _xxhMergeRound(hash, v1);
    hash = _xxhMergeRound(hash, v2);
    hash = _xxhMergeRound(hash, v3);
    hash = _xxhMergeRound(hash, v4);
  } else {
    hash = seedU + _prime5;
  }

  hash = hash + Uint64.fromInt(length);
  while (offset + 8 <= length) {
    final k1 = _xxhRound(Uint64.zero, _readUint64(bytes, offset));
    hash = _rotl(hash ^ k1, 27) * _prime1 + _prime4;
    offset += 8;
  }
  if (offset + 4 <= length) {
    final value = Uint64(0, _readUint32(bytes, offset));
    hash = _rotl(hash ^ (value * _prime1), 23) * _prime2 + _prime3;
    offset += 4;
  }
  while (offset < length) {
    hash = _rotl(hash ^ (Uint64(0, bytes[offset]) * _prime5), 11) * _prime1;
    offset++;
  }

  hash = hash ^ (hash >> 33);
  hash = hash * _prime2;
  hash = hash ^ (hash >> 29);
  hash = hash * _prime3;
  hash = hash ^ (hash >> 32);
  return hash;
}

/// Parsed `BloomFilterHeader`.
final class BloomFilterHeader {
  /// Creates a parsed header.
  const BloomFilterHeader({required this.numBytes, required this.headerLength});

  /// Size of the bitset in bytes.
  final int numBytes;

  /// Number of bytes the serialized header occupied.
  final int headerLength;
}

/// One Parquet Split Block Bloom Filter.
final class ParquetBloomFilter {
  /// Wraps an existing bitset (its length must be a power-of-two multiple of
  /// 32 bytes).
  ParquetBloomFilter.fromBitset(this.bitset)
    : numBytes = bitset.length,
      _blockCount = bitset.length >> 5 {
    _validateSize(numBytes);
  }

  /// Creates an empty filter sized for [numValues] distinct values at
  /// [fpp] false-positive probability, capped at [maxBytes].
  factory ParquetBloomFilter.create({
    required int numValues,
    double fpp = 0.01,
    int maxBytes = 1024 * 1024,
  }) {
    if (numValues < 0) {
      throw ArgumentError.value(numValues, 'numValues', 'must not be negative');
    }
    if (!(fpp > 0 && fpp < 1)) {
      throw ArgumentError.value(fpp, 'fpp', 'must be between 0 and 1');
    }
    if (maxBytes < 32) {
      throw ArgumentError.value(maxBytes, 'maxBytes', 'must be at least 32');
    }
    final bytes = bloomFilterByteSize(
      numValues: numValues,
      fpp: fpp,
      maxBytes: maxBytes,
    );
    return ParquetBloomFilter.fromBitset(Uint8List(bytes));
  }

  /// Bitset bytes (a multiple of 32, at most `bloomFilterMaxBytes`).
  final Uint8List bitset;

  /// Number of bytes in [bitset].
  final int numBytes;

  final int _blockCount;

  /// Number of 256-bit blocks.
  int get blockCount => _blockCount;

  /// Marks [value] (PLAIN-encoded bytes) as present.
  void addBytes(Uint8List value) {
    final hash = xxHash64(value);
    final base = _blockIndex(hash) * 32;
    final low = hash.lo;
    for (var word = 0; word < 8; word++) {
      final product = (Uint64(0, low) * Uint64(0, _salts[word])).lo;
      final bit = product >> 27;
      // Each probe sets one bit in its own little-endian 32-bit word.
      bitset[base + word * 4 + (bit >> 3)] |= 1 << (bit & 7);
    }
  }

  /// Whether [value] may be present (no false negatives).
  bool mightContainBytes(Uint8List value) {
    final hash = xxHash64(value);
    final base = _blockIndex(hash) * 32;
    final low = hash.lo;
    for (var word = 0; word < 8; word++) {
      final product = (Uint64(0, low) * Uint64(0, _salts[word])).lo;
      final bit = product >> 27;
      final byteIndex = base + word * 4 + (bit >> 3);
      if ((bitset[byteIndex] & (1 << (bit & 7))) == 0) return false;
    }
    return true;
  }

  int _blockIndex(Uint64 hash) {
    if (_blockCount == 1) return 0;
    final product = Uint64(0, hash.hi) * Uint64.fromInt(_blockCount);
    return (product >> 32).toInt();
  }

  /// Serializes the header and bitset, as stored at `bloom_filter_offset`.
  Uint8List toBytes() {
    final header = encodeBloomFilterHeader(numBytes);
    final out = Uint8List(header.length + numBytes);
    out.setRange(0, header.length, header);
    out.setRange(header.length, out.length, bitset);
    return out;
  }

  /// Parses a `BloomFilterHeader` from the start of [bytes].
  static BloomFilterHeader parseHeader(Bytes bytes) {
    final reader = ThriftCompactReader.fromBytes(bytes);
    final fields = reader.readStruct();
    final numBytes = fields[1];
    if (numBytes is! int || numBytes <= 0) {
      throw ParquetFormatException('BloomFilterHeader.numBytes is missing');
    }
    _requireEmptyUnion(fields[2], 'algorithm');
    _requireEmptyUnion(fields[3], 'hash');
    _requireEmptyUnion(fields[4], 'compression');
    return BloomFilterHeader(numBytes: numBytes, headerLength: reader.position);
  }

  /// Reads a filter from `bloom_filter_length` bytes (header followed by the
  /// bitset).
  static ParquetBloomFilter read(Bytes bytes) {
    final header = parseHeader(bytes);
    final start = header.headerLength;
    if (start + header.numBytes > bytes.length) {
      throw ParquetFormatException(
        'Bloom filter declares ${header.numBytes} bitset bytes but only '
        '${bytes.length - start} remain',
      );
    }
    final view = bytes.slice(start, start + header.numBytes);
    return ParquetBloomFilter.fromBitset(view.asUint8List());
  }

  static void _validateSize(int numBytes) {
    if (numBytes < 32 || (numBytes & (numBytes - 1)) != 0) {
      throw ArgumentError.value(
        numBytes,
        'numBytes',
        'Bloom filter size must be a power of two and at least 32 bytes',
      );
    }
  }

  static void _requireEmptyUnion(Object? raw, String name) {
    if (raw is! Map<int, Object?>) {
      throw ParquetFormatException('BloomFilterHeader.$name is missing');
    }
    final payload = raw[1];
    if (raw.length != 1 ||
        payload is! Map<int, Object?> ||
        payload.isNotEmpty) {
      throw ParquetUnsupportedException(
        'unsupported Bloom filter $name in header',
      );
    }
  }
}

/// Serializes a `BloomFilterHeader` for a [numBytes] bitset.
Uint8List encodeBloomFilterHeader(int numBytes) {
  final writer = ThriftCompactWriter();
  writer.writeStruct({
    1: ThriftI32(numBytes),
    2: <int, Object?>{
      1: <int, Object?>{}, // SplitBlockAlgorithm.BLOCK
    },
    3: <int, Object?>{
      1: <int, Object?>{}, // XxHash.XXHASH
    },
    4: <int, Object?>{
      1: <int, Object?>{}, // Uncompressed.UNCOMPRESSED
    },
  });
  return writer.toBytes().asUint8List();
}

/// Chooses the bitset size in bytes for [numValues] at [fpp], capped at
/// [maxBytes].
///
/// The result is a power of two and at least 32 bytes; values larger than the
/// cap are clamped to the largest power of two that fits.
int bloomFilterByteSize({
  required int numValues,
  double fpp = 0.01,
  int maxBytes = 1024 * 1024,
}) {
  if (maxBytes < 32) {
    throw ArgumentError.value(maxBytes, 'maxBytes', 'must be at least 32');
  }
  var limit = 32;
  while (limit * 2 <= maxBytes) {
    limit *= 2;
  }
  if (numValues <= 0) return 32;

  // Optimal bit count from the standard Bloom filter formula, then round the
  // byte size up to a power of two.
  final bits = -numValues * math.log(fpp) / (math.ln2 * math.ln2);
  var requiredBytes = (bits / 8).ceil();
  if (requiredBytes < 32) requiredBytes = 32;
  var bytes = 32;
  while (bytes < requiredBytes && bytes < limit) {
    bytes <<= 1;
  }
  return bytes;
}
