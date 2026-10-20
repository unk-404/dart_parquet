/// Platform-selected parallel (isolate-pool) source.
///
/// On the VM this exports the native `blit` isolate-pool adapter; on the web it
/// exports stubs that throw a clear [UnsupportedError]. Import this library,
/// never `parallel_source_native.dart`, from code that must also compile to
/// JavaScript.
library;

export 'parallel_source_native.dart'
    if (dart.library.js_interop) 'parallel_source_web.dart';
