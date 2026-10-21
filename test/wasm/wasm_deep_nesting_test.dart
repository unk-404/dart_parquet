/// v0.2 deep-nesting matrix under dart2wasm.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_deep_nesting_test.dart
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_deep_nesting_test.dart' as web;

void main() => web.main();
