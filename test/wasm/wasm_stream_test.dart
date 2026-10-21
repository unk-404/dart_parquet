/// v0.2 writer-side streaming/resume matrix under dart2wasm.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_stream_test.dart
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_stream_test.dart' as web;

void main() => web.main();
