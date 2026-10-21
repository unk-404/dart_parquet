/// Reserved feature surface for Parquet capabilities that are detected but not
/// implemented yet.
library;

import 'util/errors.dart';

/// A Parquet capability that can be recognized independently of whether the
/// current build implements it.
enum ParquetFeature {
  /// Encrypted footer (`PARE`) or column encryption.
  encryption,

  /// GEOMETRY / GEOGRAPHY logical types and WKB payloads.
  geospatial,

  /// VARIANT logical groups and variant binary encoding.
  variant,

  /// ALP floating-point encoding.
  alp,

  /// Change-data-capture conventions.
  cdc,
}

/// Thrown when a file uses a recognized feature that is intentionally reserved
/// for a later release.
///
/// This is a [ParquetUnsupportedException] subtype, so existing callers keep
/// their behavior while new callers can inspect [feature] and [path].
class ParquetUnsupportedFeatureException extends ParquetUnsupportedException {
  ParquetUnsupportedFeatureException(
    this.feature,
    String message, {
    this.path,
    Object? cause,
    StackTrace? stackTrace,
  }) : super(message, cause, stackTrace);

  /// Reserved feature that triggered the error.
  final ParquetFeature feature;

  /// Schema path or file region, when known.
  final String? path;

  @override
  String toString() {
    final suffix = path == null ? '' : ' at $path';
    return '${super.toString()} (feature: ${feature.name}$suffix)';
  }
}

/// Extension point for future Parquet encryption implementations.
///
/// No provider ships in v0.2.0; encrypted files are detected and rejected with
/// [ParquetUnsupportedFeatureException].
abstract interface class EncryptionProvider {
  /// Human-readable algorithm/runtime identifier.
  String get algorithm;

  /// Future hook for decrypting a page/column chunk.
  Future<List<int>> decrypt(List<int> ciphertext, {required List<int> aad});
}

/// Extension point for future GEOMETRY / GEOGRAPHY implementations.
///
/// No codec ships in v0.2.0; geospatial logical types are preserved as WKB
/// bytes and can be passed to a codec later.
abstract interface class GeospatialCodec {
  /// WKB bytes -> opaque geometry value.
  Object decode(List<int> wkb);

  /// Opaque geometry value -> WKB bytes.
  List<int> encode(Object value);
}
