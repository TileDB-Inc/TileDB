#include <catch.hpp>

using namespace tiledb;

#ifdef _WIN32
#define win32_or_posix(uri) REQUIRE((uri).is_win32())
#else
#define win32_or_posix(uri) REQUIRE((uri).is_posix())
#endif

#ifdef _WIN32
#define win32_or_invalid(uri) REQUIRE((uri).is_win32())
#else
#define win32_or_invalid(uri) REQUIRE((uri).is_invalid())
#endif


TEST_CASE("URI: Test detection of Windows paths", "[uri]") {
  URI uri;
  uri = "noslashes";
  REQUIRE(!uri.is_invalid());
  win32_or_posix(uri);
  uri = "";
  REQUIRE(uri.is_invalid());
  uri = "C:\\dirname";
  win32_or_invalid(uri);
  uri = "g:\\dirname";
  win32_or_invalid(uri);
  uri = "C:\\Program Files (x86)\\TileDB\\";
  win32_or_invalid(uri);
  uri = "file:///path/to/file";
  REQUIRE(uri.is_posix());
  uri = "hdfs://sub.domain.com:8080/some/path";
  REQUIRE(uri.is_hdfs());
  uri = "s3://sub.domain.com:8080/some/path";
  REQUIRE(uri.is_s3());
}
