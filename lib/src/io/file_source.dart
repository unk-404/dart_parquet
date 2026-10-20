/// Platform-selected filesystem source factories.
///
/// On the VM this exports `native_file.dart`; on the web it exports the stub
/// that throws [UnsupportedError]. Import this library, never `native_file.dart`
/// directly, from code that must also compile to JavaScript.
library;

export 'native_file.dart' if (dart.library.js_interop) 'web_file.dart';
