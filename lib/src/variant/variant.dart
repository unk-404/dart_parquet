/// Apache Parquet Variant binary encoding.
///
/// A Parquet `VARIANT` column (a group annotated with `VariantLogicalType`)
/// stores an unshredded value as two `BYTE_ARRAY` fields:
///
/// ```text
/// optional group variant_name (VARIANT(1)) {
///   required binary metadata;
///   required binary value;
/// }
/// ```
///
/// [Variant] models that pair: [Variant.metadataBytes] decodes to
/// [VariantMetadata] and [Variant.valueBytes] decodes to [VariantValue].
/// This library implements the unshredded representation described by the
/// [Variant binary encoding specification](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md).
///
/// ## Parquet schema compatibility
///
/// The containing Parquet group must be annotated with `VariantLogicalType`
/// (`VARIANT`) and contain `metadata` and `value` binary fields. Use
/// [supportsVariantSpecificationVersion] with
/// `VariantLogicalType.specificationVersion` before decoding. A missing
/// version defaults to version 1; files with a newer version are not
/// supported.
///
/// ## Shredding (phase 2)
///
/// Shredded (`typed_value`) columns described by the
/// [shredding specification](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)
/// are not reconstructed here. Callers should pass only the unshredded
/// `metadata` and `value` payloads; a reader that encounters `typed_value`
/// must either reconstruct the Variant first (not yet implemented) or reject
/// the column. The Variant codec itself has no knowledge of Parquet schemas or
/// column shredding.
library;

import 'dart:typed_data';

import 'variant_codec.dart';
import 'variant_metadata.dart';
import 'variant_value.dart';

export 'variant_codec.dart' show VariantCodec, maxVariantNestingDepth;
export 'variant_metadata.dart'
    show VariantMetadata, compareVariantStrings, variantMetadataVersion;
export 'variant_value.dart';

/// Variant specification version implemented by this codec.
///
/// This corresponds to `VariantLogicalType(specificationVersion)` in the
/// Parquet schema. Files without an explicit version use version 1.
const int variantSpecificationVersion = 1;

/// Whether [specificationVersion] is supported by this codec.
///
/// A `null` version means the Parquet schema omitted it, which defaults to
/// version 1.
bool supportsVariantSpecificationVersion(int? specificationVersion) =>
    specificationVersion == null ||
    specificationVersion == variantSpecificationVersion;

/// A Variant value together with its metadata dictionary.
///
/// This is the in-memory form of the two binary fields of a Parquet VARIANT
/// group. Use [decode] to read a stored pair, [fromJson] to build one from
/// JSON-compatible Dart values, or the [Variant.metadata] / [Variant.value]
/// constructor to supply an explicit pair.
final class Variant {
  /// Creates a Variant from an explicit metadata/value pair.
  const Variant({required this.metadata, required this.value});

  /// Decodes a stored Variant group.
  ///
  /// Both inputs are the complete `BYTE_ARRAY` payloads from the Parquet
  /// `metadata` and `value` columns.
  factory Variant.decode(
    List<int> metadataBytes,
    List<int> valueBytes, {
    int maxDepth = maxVariantNestingDepth,
  }) {
    final metadata = VariantMetadata.fromBytes(metadataBytes);
    final value = VariantCodec.decode(valueBytes, metadata, maxDepth: maxDepth);
    return Variant(metadata: metadata, value: value);
  }

  /// Builds a Variant from a JSON-compatible value.
  ///
  /// Object field names are collected recursively and encoded as a sorted
  /// metadata dictionary. JSON numbers use the smallest integer type that fits
  /// or a double, so the original JSON can be recovered with [toJson] but the
  /// exact Variant primitive type is chosen by this library.
  ///
  /// JSON conversion is not type-preserving: typed values (decimal, date,
  /// time, timestamp, UUID) are rendered as strings by [toJson] and become
  /// plain Variant strings when passed back to this constructor. Use the typed
  /// [VariantValue] subclasses together with [Variant.fromValue] when the wire
  /// type matters.
  factory Variant.fromJson(
    Object? json, {
    int maxDepth = maxVariantNestingDepth,
  }) {
    final value = VariantValue.fromJson(json, maxDepth: maxDepth);
    return Variant.fromValue(value);
  }

  /// Builds a Variant around [value].
  ///
  /// When [metadata] is omitted, a sorted dictionary is created from all
  /// object field names contained in [value].
  factory Variant.fromValue(VariantValue value, {VariantMetadata? metadata}) {
    if (metadata != null) {
      return Variant(metadata: metadata, value: value);
    }
    final fieldNames = <String>{};
    value.collectFieldNames(fieldNames);
    return Variant(
      metadata: VariantMetadata.fromStrings(fieldNames),
      value: value,
    );
  }

  /// Shared metadata dictionary.
  final VariantMetadata metadata;

  /// Decoded value.
  final VariantValue value;

  /// Encoded Parquet `metadata` binary.
  Uint8List get metadataBytes => metadata.toBytes();

  /// Encoded Parquet `value` binary.
  Uint8List get valueBytes => VariantCodec.encode(value, metadata);

  /// JSON-compatible representation, see [VariantValue.toJson].
  Object? toJson() => value.toJson();

  @override
  String toString() => 'Variant(${value.toJson()})';
}
