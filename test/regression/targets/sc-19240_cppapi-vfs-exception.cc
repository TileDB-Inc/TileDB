#include <iostream>
#include <tiledb/tiledb>

#include <test/support/tdb_catch.h>

TEST_CASE(
    "C++ API: test exception handling in VFSFileBuf interface",
    "[cppapi][regression]") {
  tiledb::Context context;

  tiledb::VFS vfs{context};
  tiledb::VFS::filebuf fb{vfs};

  const std::string uri{"/dir/not/exists/hello.txt"};

  /* currently segfaults (11/July/2022) */
  REQUIRE(fb.open(uri, std::ios::out));
  fb.close();
}