/// Browser storage (OPFS, IndexedDB, Blob) compiled with dart2wasm.
///
/// These reuse the dart2js storage suite; `doc/wasm.md` documents the known
/// Web-storage caveats that apply to both compilers.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_storage_test.dart
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_storage_test.dart' as web;

void main() => web.main();
