/// Parquet file footer parsing.
///
/// A Parquet file starts and ends with a four-byte magic (`PAR1`, or `PARE`
/// for an encrypted footer) and stores the length of the Thrift-serialised
/// [FileMetaData] in the last eight bytes:
///
/// ```text
/// +---------+----------------------+------------------+--------+
/// | "PAR1"  | column chunks/pages  | FileMetaData ... | length | "PAR1" |
/// +---------+----------------------+------------------+--------+
/// ```
library;

import 'dart:typed_data';

import '../feature.dart';
import '../util/bytes.dart';
import '../util/errors.dart';
import 'parquet_format.dart';
import 'thrift.dart';

/// Number of bytes at the end of a file reserved for the footer length + magic.
const int parquetFooterTrailerSize = 8;

/// Plaintext footer magic (`PAR1`).
const String parquetMagic = 'PAR1';

/// Encrypted footer magic (`PARE`).
const String parquetEncryptedMagic = 'PARE';

/// A parsed file footer plus where it lives.
final class Footer {
  /// Creates a parsed footer.
  const Footer({
    required this.fileMetadata,
    required this.footerLength,
    required this.metadataOffset,
    this.encrypted = false,
  });

  /// Parsed Thrift file metadata.
  final FileMetaData fileMetadata;

  /// Declared length of the serialised metadata in bytes.
  final int footerLength;

  /// Offset of the metadata within the buffer passed to [parseFooter].
  ///
  /// For a whole-file buffer this is the absolute file offset; for an exact
  /// trailer buffer it is zero.
  final int metadataOffset;

  /// Whether the file uses an encrypted footer. Encrypted footers are
  /// currently rejected, so this is always `false` for a successful parse.
  final bool encrypted;

  /// Offset one past the end of the serialised metadata.
  int get metadataEnd => metadataOffset + footerLength;
}

/// Alias kept for callers that prefer an explicit result-type name.
typedef FooterParseResult = Footer;

/// Reads the four bytes of magic starting at [offset] as ASCII.
String _magicAt(Uint8List bytes, int offset) =>
    String.fromCharCodes(bytes, offset, offset + 4);

/// Reads the declared footer length from a buffer whose last eight bytes are
/// the trailer. The trailing magic is not validated by this helper.
int readFooterLength(Bytes tail) {
  if (tail.length < parquetFooterTrailerSize) {
    throw ParquetFormatException(
      'buffer is too short for a Parquet trailer: ${tail.length} bytes',
    );
  }
  final reader = ByteReader(
    tail,
    position: tail.length - parquetFooterTrailerSize,
  );
  return reader.readUint32LE();
}

/// Whether [tail] ends with the encrypted-footer magic `PARE`.
bool isEncryptedFooter(Bytes tail) {
  if (tail.length < 4) return false;
  return _magicAt(tail.asUint8List(), tail.length - 4) == parquetEncryptedMagic;
}

/// Parses the footer from the whole file or from an exact trailer buffer.
///
/// * When a whole file is passed, the leading `PAR1` magic is validated as
///   well as the trailing magic and both ends must agree.
/// * When only the last `footerLength + 8` bytes are passed, the leading magic
///   is not available and is skipped (the trailer length must account for
///   exactly that many bytes).
///
/// `PARE` at either end means the footer is encrypted; encrypted files are not
/// supported and yield a [ParquetUnsupportedException].
Footer parseFooter(Bytes tailOrWholeFile) {
  final length = tailOrWholeFile.length;
  if (length < parquetFooterTrailerSize) {
    throw ParquetFormatException(
      'not a Parquet file: only $length bytes available',
    );
  }

  final bytes = tailOrWholeFile.asUint8List();
  final footerLength = readFooterLength(tailOrWholeFile);
  final trailingMagic = _magicAt(bytes, length - 4);

  if (trailingMagic == parquetEncryptedMagic) {
    throw ParquetUnsupportedFeatureException(
      ParquetFeature.encryption,
      'Parquet files with an encrypted footer (PARE) are not supported',
    );
  }
  if (trailingMagic != parquetMagic) {
    throw ParquetFormatException('bad Parquet footer magic: "$trailingMagic"');
  }
  if (footerLength <= 0) {
    throw ParquetFormatException(
      'invalid Parquet footer length: $footerLength',
    );
  }

  final metadataOffset = length - parquetFooterTrailerSize - footerLength;
  if (metadataOffset < 0) {
    throw ParquetFormatException(
      'Parquet footer length $footerLength exceeds file size $length',
    );
  }

  // A whole file has at least the four-byte leading magic before the footer.
  // An exact trailer starts at offset zero. Anything in between is a truncated
  // header, which cannot be a valid file or the expected exact trailer.
  if (metadataOffset >= 4) {
    final leadingMagic = _magicAt(bytes, 0);
    if (leadingMagic == parquetEncryptedMagic) {
      throw ParquetUnsupportedFeatureException(
        ParquetFeature.encryption,
        'Parquet files with an encrypted footer (PARE) are not supported',
      );
    }
    if (leadingMagic != parquetMagic) {
      throw ParquetFormatException(
        'bad Parquet file header magic: "$leadingMagic"',
      );
    }
  } else if (metadataOffset != 0) {
    throw ParquetFormatException(
      'truncated Parquet header: metadata starts at offset $metadataOffset',
    );
  }

  final metadataBytes = tailOrWholeFile.slice(
    metadataOffset,
    metadataOffset + footerLength,
  );
  final metadata = parseFileMetadata(metadataBytes);
  return Footer(
    fileMetadata: metadata,
    footerLength: footerLength,
    metadataOffset: metadataOffset,
  );
}

/// [parseFooter] under the more explicit name used by some callers.
Footer parseFileFooter(Bytes tailOrWholeFile) => parseFooter(tailOrWholeFile);

/// Parses Thrift `FileMetaData` from the raw footer metadata bytes.
///
/// [footerBytes] must contain exactly the serialised struct (i.e. the slice
/// identified by the footer length), not the trailing magic.
FileMetaData parseFileMetadata(Bytes footerBytes) {
  if (footerBytes.isEmpty) {
    throw ParquetFormatException('empty Parquet footer metadata');
  }
  final reader = ThriftCompactReader.fromBytes(footerBytes);
  final fields = reader.readStruct();
  return FileMetaData.fromThrift(fields);
}
