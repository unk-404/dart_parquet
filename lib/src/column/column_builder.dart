/// Builds typed columns from decoded column-chunk data.
library;

import 'dart:typed_data';

import '../encodings/plain.dart' show ByteArrayData, FixedLenData;
import '../schema/schema.dart';
import '../util/errors.dart';
import '../util/int64.dart';
import 'page_reader.dart';
import '../table/column.dart';

/// Builds a flat (non-repeated) column from [decoded].
///
/// Nested schemas (maxRepLevel > 0) are assembled by the nested assembler.
ParquetColumn buildFlatColumn(
  ColumnDescriptor descriptor,
  DecodedColumnChunk decoded,
) {
  if (descriptor.maxRepLevel != 0) {
    throw ParquetUnsupportedException(
      'nested column ${descriptor.dotPath} requires the record assembler',
    );
  }
  final numValues = decoded.numValues;
  final maxDef = descriptor.maxDefLevel;
  final validity = Validity(numValues);

  if (maxDef > 0 && decoded.defLevels != null) {
    final def = decoded.defLevels!;
    for (var i = 0; i < numValues; i++) {
      if (def[i] < maxDef) validity.setNull(i);
    }
  }
  return buildColumnFromCompact(descriptor, decoded.values, validity);
}

/// Expands compact present values into a full-length column.
///
/// [validity] supplies the target length; [values] holds only the present
/// values in slot order.
ParquetColumn buildColumnFromCompact(
  ColumnDescriptor descriptor,
  Object values,
  Validity validity,
) {
  final numValues = validity.length;
  final isUtf8 = _isUtf8(descriptor);

  // Fully valid pages already have one value per row; reuse the compact
  // buffers instead of copying them into fresh typed arrays.
  if (!validity.hasNulls) {
    if (values is Int32List && values.length == numValues) {
      return Int32Column(
        name: descriptor.name,
        values: values,
        validity: validity,
      );
    }
    if (values is Int64Buffer && values.length == numValues) {
      return Int64Column(
        name: descriptor.name,
        values: values,
        validity: validity,
      );
    }
    if (values is Float32List && values.length == numValues) {
      return Float32Column(
        name: descriptor.name,
        values: values,
        validity: validity,
      );
    }
    if (values is Float64List && values.length == numValues) {
      return Float64Column(
        name: descriptor.name,
        values: values,
        validity: validity,
      );
    }
    if (values is ByteArrayData && values.numValues == numValues) {
      return BinaryColumn(
        name: descriptor.name,
        data: values.data,
        offsets: values.offsets,
        validity: validity,
        isUtf8: isUtf8,
      );
    }
    if (values is FixedLenData && values.numValues == numValues) {
      if (descriptor.physicalType == PhysicalType.int96) {
        return Int96Column(
          name: descriptor.name,
          data: values.data,
          validity: validity,
        );
      }
      return FixedLenColumn(
        name: descriptor.name,
        data: values.data,
        width: values.width,
        validity: validity,
      );
    }
  }

  if (values is Int32List) {
    final out = Int32List(numValues);
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i)) out[i] = values[src++];
    }
    return Int32Column(name: descriptor.name, values: out, validity: validity);
  }

  if (values is Int64Buffer) {
    final out = Int64Buffer(numValues);
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i)) {
        final limbs = values.getLimbs(src++);
        out.setLimbs(i, limbs.$1, limbs.$2);
      }
    }
    return Int64Column(name: descriptor.name, values: out, validity: validity);
  }

  if (values is Float32List) {
    final out = Float32List(numValues);
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i)) out[i] = values[src++];
    }
    return Float32Column(
      name: descriptor.name,
      values: out,
      validity: validity,
    );
  }

  if (values is Float64List) {
    final out = Float64List(numValues);
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i)) out[i] = values[src++];
    }
    return Float64Column(
      name: descriptor.name,
      values: out,
      validity: validity,
    );
  }

  if (values is Uint8List) {
    // One byte per present boolean.
    final bits = Uint8List((numValues + 7) >> 3);
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i) && values[src++] != 0) {
        bits[i >> 3] |= 1 << (i & 7);
      }
    }
    return BooleanColumn(name: descriptor.name, bits: bits, validity: validity);
  }

  if (values is ByteArrayData) {
    final data = Uint8List(_sumBytes(values));
    final offsets = Uint32List(numValues + 1);
    var dataPos = 0;
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i)) {
        final start = values.offsets[src];
        final end = values.offsets[src + 1];
        data.setRange(dataPos, dataPos + (end - start), values.data, start);
        dataPos += end - start;
        src++;
      }
      offsets[i + 1] = dataPos;
    }
    return BinaryColumn(
      name: descriptor.name,
      data: data,
      offsets: offsets,
      validity: validity,
      isUtf8: isUtf8,
    );
  }

  if (values is FixedLenData) {
    final width = values.width;
    final data = Uint8List(numValues * width);
    var src = 0;
    for (var i = 0; i < numValues; i++) {
      if (!validity.isNull(i)) {
        data.setRange(i * width, (i + 1) * width, values.data, src * width);
        src++;
      }
    }
    if (descriptor.physicalType == PhysicalType.int96) {
      return Int96Column(name: descriptor.name, data: data, validity: validity);
    }
    return FixedLenColumn(
      name: descriptor.name,
      data: data,
      width: width,
      validity: validity,
    );
  }

  throw ParquetUnsupportedException(
    'unsupported value container ${values.runtimeType} for '
    '${descriptor.dotPath}',
  );
}

int _sumBytes(ByteArrayData data) {
  var total = 0;
  for (var i = 0; i < data.numValues; i++) {
    total += data.offsets[i + 1] - data.offsets[i];
  }
  return total;
}

bool _isUtf8(ColumnDescriptor descriptor) {
  final kind = descriptor.logicalType?.kind;
  if (kind == LogicalTypeKind.string ||
      kind == LogicalTypeKind.enumType ||
      kind == LogicalTypeKind.json) {
    return true;
  }
  final converted = descriptor.convertedType;
  return converted == ConvertedType.utf8 ||
      converted == ConvertedType.enumType ||
      converted == ConvertedType.json;
}
