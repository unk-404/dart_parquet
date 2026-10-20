/// Blob/File sources: callback-based everywhere, JavaScript `Blob` on the web.
///
/// Import this library, not `blob_source_web.dart`, from code that must compile
/// on the VM as well.
library;

export 'blob_slice_source.dart';
export 'blob_source_stub.dart'
    if (dart.library.js_interop) 'blob_source_web.dart';
