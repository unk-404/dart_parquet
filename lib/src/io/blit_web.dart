/// Platform-selected blit Web extensions (Blob and IndexedDB backends).
///
/// The VM stub throws a clear [UnsupportedError]; browsers use the real blit
/// Web backends. Import this library, not `blit_web_web.dart`, from shared code.
library;

export 'blit_web_stub.dart' if (dart.library.js_interop) 'blit_web_web.dart';
