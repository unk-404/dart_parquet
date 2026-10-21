import 'package:blit/blit.dart';

Future<BlitFile> openBytesFile(
  String path, {
  bool create = true,
  bool readOnly = false,
}) => blit.open(
  path,
  options: FileOptions(
    mode: readOnly ? FileMode.read : FileMode.readWrite,
    create: create,
  ),
);
