/// Full browser read/write matrix compiled with dart2wasm.
///
/// The suite is intentionally shared with `test/web/web_read_test.dart` so the
/// dart2js and dart2wasm runs cannot drift apart. Run with:
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_read_test.dart' as web;

void main() => web.main();
