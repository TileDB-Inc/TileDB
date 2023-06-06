#include <iostream>
#include <tiledb/tiledb>

int main() {
  const std::string array_bucket = "s3://tiledb-s3-test/";

  tiledb::Config cfg;
  cfg["vfs.s3.endpoint_override"] = "localhost:9999";
  cfg["vfs.s3.scheme"] = "https";
  cfg["vfs.s3.use_virtual_addressing"] = "false";
  cfg["vfs.s3.verify_ssl"] = "false";

  tiledb::Context ctx(cfg);
  if (!ctx.is_supported_fs(TILEDB_S3))
    return 0;

  // Create bucket on s3
  tiledb::VFS vfs(ctx);
  if (vfs.is_bucket(array_bucket)) {
    vfs.remove_bucket(array_bucket);
  }
  vfs.create_bucket(array_bucket);

  vfs.touch(array_bucket + "foo");
  vfs.touch(array_bucket + "foo/bar/baz");

  /* Manually verified here the following structure:
    s3://tiledb-s3-test/foo
      xl.meta
      bar
        baz
          xl.meta
  */

  // Exists but prints nothing
  auto x = vfs.ls("foo/");
  for (auto u : x) {
    std::cerr << u.c_str() << std::endl;
  }

  // Exists but prints nothing
  auto y = vfs.ls("foo/bar");
  for (auto u : x) {
    std::cerr << u.c_str() << std::endl;
  }

  // Entire folder is deleted here
  vfs.remove_file(array_bucket + "foo");

  // Doesn't exist, prints nothing
  auto z = vfs.ls("foo/bar");
  for (auto u : x) {
    std::cerr << u.c_str() << std::endl;
  }

  // Clean up
  if (vfs.is_bucket(array_bucket)) {
    vfs.remove_bucket(array_bucket);
  }

  return 0;
}