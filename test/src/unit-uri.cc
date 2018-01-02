#include <uri.h>
#include <catch.hpp>

using namespace tiledb;

TEST_CASE("URI: Test file URIs", "[uri]") {
  URI uri;
  uri = URI("noslashes");
  REQUIRE(!uri.is_invalid());
  CHECK(uri.is_file());
  uri = URI("");
  REQUIRE(uri.is_invalid());
  // uri = "C:\\dirname";
  // win32_or_invalid(uri);
  // uri = "g:\\dirname";
  // win32_or_invalid(uri);
  // uri = "C:\\Program Files (x86)\\TileDB\\";
  // win32_or_invalid(uri);
  // uri = "file:///path/to/file";
  // REQUIRE(uri.is_posix());
  // uri = "hdfs://sub.domain.com:8080/some/path";
  // REQUIRE(uri.is_hdfs());
  // uri = "s3://sub.domain.com:8080/some/path";
  // REQUIRE(uri.is_s3());
}
