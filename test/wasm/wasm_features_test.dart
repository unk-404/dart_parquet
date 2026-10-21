/// v0.2 feature detection (CDC/PARE/geospatial) under dart2wasm.
///
/// ```sh
/// dart test -p chrome -c dart2wasm test/wasm/wasm_features_test.dart
/// ```
@TestOn('browser')
library;

import 'package:test/test.dart' show TestOn;

import '../web/web_features_test.dart' as web;

void main() => web.main();
