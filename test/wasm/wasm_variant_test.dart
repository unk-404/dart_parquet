/// v0.2 Variant matrix under dart2wasm.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_variant_test.dart
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_variant_test.dart' as web;

void main() => web.main();
