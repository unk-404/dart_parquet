/// Errors thrown by the Parquet package.
library;

/// Base class for all Parquet-related failures.
class ParquetException implements Exception {
  ParquetException(this.message, [this.cause, this.stackTrace]);

  final String message;
  final Object? cause;
  final StackTrace? stackTrace;

  @override
  String toString() =>
      '$runtimeType: $message${cause == null ? '' : ' (cause: $cause)'}';
}

/// The input is not a valid Parquet file / stream.
class ParquetFormatException extends ParquetException {
  ParquetFormatException(super.message, [super.cause, super.stackTrace]);
}

/// The input ended before a complete structure could be read.
class ParquetEofException extends ParquetFormatException {
  ParquetEofException(super.message, [super.cause, super.stackTrace]);
}

/// A valid Parquet feature that this package does not implement (yet).
class ParquetUnsupportedException extends ParquetException {
  ParquetUnsupportedException(super.message, [super.cause, super.stackTrace]);
}

/// A value/offset was outside the valid range.
class ParquetRangeException extends ParquetException {
  ParquetRangeException(super.message, [super.cause, super.stackTrace]);
}
