/// Platform-selected native mmap source.
///
/// On the VM this exports the `blit`-backed whole-file mapping; on the web it
/// exports stubs that throw a clear [UnsupportedError]. Import this library,
/// never `mmap_source_native.dart`, from code that must also compile to
/// JavaScript.
library;

export 'mmap_source_native.dart'
    if (dart.library.js_interop) 'mmap_source_web.dart';
