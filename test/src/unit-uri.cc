#include <uri.h>
#include <catch.hpp>

#ifdef _WIN32
#include <win32_filesystem.h>
#endif

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

#ifdef _WIN32

TEST_CASE("URI: Test Windows paths", "[uri]") {
  URI uri("C:\\path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  // Windows file URIs keep the drive letter to remain fully qualified.
  CHECK(uri.to_string() == "file:///C:/path");
  uri = URI("g:\\path\\..\\relative\\");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///g:/relative/");
  uri = URI("C:\\mixed/slash\\types");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///C:/mixed/slash/types");
  uri = URI("C:/mixed/slash/types");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///C:/mixed/slash/types");
  uri = URI("C:\\Program Files (x86)\\TileDB\\");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///C:/Program%20Files%20(x86)/TileDB/");
}

#endif
