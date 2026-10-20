/// Platform-selected exact 64-bit integer buffer.
///
/// On the VM this is a thin wrapper over `Int64List`. On dart2js there is no
/// 64-bit typed array, so it stores two 32-bit limbs. Values are exact on both
/// platforms; the `int` accessor throws when the value is not exactly
/// representable on the web (|v| > 2^53).
library;

export 'int64_native.dart' if (dart.library.js_interop) 'int64_web.dart';
