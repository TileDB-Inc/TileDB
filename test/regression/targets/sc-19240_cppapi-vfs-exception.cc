#include <iostream>
#include <tiledb/tiledb>

#include <catch2/catch_test_macros.hpp>

TEST_CASE(
    "C++ API: test exception handling in VFSFileBuf interface",
    "[cppapi][regression]") {
  tiledb::Context context;

  tiledb::VFS vfs{context};
  tiledb::VFS::filebuf fb{vfs};

  const std::string uri{"/dir/not/exists/hello.txt"};

  fb.open(uri, std::ios::out);
  // Previously segfaulted, before fix in PR 3360,
  // now expected to throw. The throw happens here
  // because VFS::filebuf is a streambuf API.
  REQUIRE_THROWS(fb.close());
}
