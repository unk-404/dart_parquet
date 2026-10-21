/// v0.2 ALP matrix under dart2wasm.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_alp_test.dart
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_alp_test.dart' as web;

void main() => web.main();
