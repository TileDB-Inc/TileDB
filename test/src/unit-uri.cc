#include <uri.h>
#include <catch.hpp>

using namespace tiledb;

TEST_CASE("URI: Test file URIs", "[uri]") {
  URI uri("file:///path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  uri = URI("path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  uri = URI("");
  CHECK(uri.is_invalid());
  uri = URI("file://path");
  CHECK(uri.is_invalid());
  uri = URI("file:///path/../relative");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///relative");
  uri = URI("file:///path/.././relative/./path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///relative/path");
}
