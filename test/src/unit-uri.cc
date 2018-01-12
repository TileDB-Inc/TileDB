#include <uri.h>
#include <catch.hpp>

#ifdef _WIN32
#include <win_filesystem.h>
#else
#include <posix_filesystem.h>
#endif

using namespace tiledb;

#ifdef _WIN32
static const char PATH_SEPARATOR = '\\';
static std::string current_dir() {
  return win::current_dir();
}
#else
static const char PATH_SEPARATOR = '/';
static std::string current_dir() {
  return posix::current_dir();
}
#endif

TEST_CASE("URI: Test file URIs", "[uri]") {
  URI uri("file:///path");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string() == "file:///path");
  uri = URI("file://path");
  CHECK(uri.is_invalid());
  uri =
      URI("file:///path/is/too/long/long/long/long/long/long/long/long/long/"
          "long/long/long/long/long/long/long/long/long/long/long/long/long/"
          "long/long/long/long/long/long/long/long/long/long/long/long/long/"
          "long/long/long/long/long/long/long/long/long/long/long/long/long");
  CHECK(uri.is_invalid());
  uri = URI("");
  CHECK(uri.is_invalid());

  // TODO: re-enable these checks if appropriate for posix_filesystem.
  // uri = URI("file:///path/../relative");
  // CHECK(!uri.is_invalid());
  // CHECK(URI::is_file(uri.to_string()));
  // CHECK(uri.to_string() == "file:///relative");
  // uri = URI("file:///path/.././relative/./path");
  // CHECK(!uri.is_invalid());
  // CHECK(URI::is_file(uri.to_string()));
  // CHECK(uri.to_string() == "file:///relative/path");
}

TEST_CASE("URI: Test relative paths", "[uri]") {
  URI uri = URI("path1");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string().find("file:///") == 0);
  CHECK(uri.to_path() == current_dir() + PATH_SEPARATOR + "path1");
#ifdef _WIN32
  CHECK(uri.to_string() == win::uri_from_path(win::current_dir()) + "/path1");
#else
  CHECK(uri.to_string() == "file://" + posix::current_dir() + "/path1");
#endif

  uri = URI(".");
  CHECK(!uri.is_invalid());
  CHECK(uri.to_path() == current_dir());
}

TEST_CASE("URI: Test URI to path", "[uri]") {
  URI uri = URI("file:///my/path");
#ifdef _WIN32
  // Absolute paths with no drive letter are relative to the current working
  // directory's drive.
  CHECK(uri.to_path() == "\\my\\path");
#else
  CHECK(uri.to_path() == "/my/path");
#endif

  uri = URI("file:///my/path/../relative/path");
#ifdef _WIN32
  CHECK(uri.to_path() == "\\my\\relative\\path");
#else
  CHECK(uri.to_path() == "/my/path/../relative/path");
#endif

  uri = URI("s3://path/on/s3");
  CHECK(uri.to_path() == "s3://path/on/s3");
  uri = URI("s3://relative/../path/on/s3");
  CHECK(uri.to_path() == "s3://relative/../path/on/s3");
  uri = URI("hdfs://path/on/hdfs");
  CHECK(uri.to_path() == "hdfs://path/on/hdfs");
  uri = URI("hdfs://relative/../path/on/hdfs");
  CHECK(uri.to_path() == "hdfs://relative/../path/on/hdfs");

  uri = URI("C:\\my\\path");
#ifdef _WIN32
  CHECK(uri.to_string() == "file:///C:/my/path");
  CHECK(uri.to_path() == "C:\\my\\path");
#else
  // Windows paths on non-Windows platforms are nonsensical, but have defined
  // behavior.
  CHECK(uri.to_string() == "file://" + current_dir() + "/" + "C:\\my\\path");
  CHECK(uri.to_path() == current_dir() + "/" + "C:\\my\\path");
#endif

  uri = URI("file:///C:/my/path");
#ifdef _WIN32
  CHECK(uri.to_path() == "C:\\my\\path");
#else
  CHECK(uri.to_path() == "/C:/my/path");
#endif
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
  uri = URI("path1\\path2");
  CHECK(!uri.is_invalid());
  CHECK(URI::is_file(uri.to_string()));
  CHECK(uri.to_string().find("file:///") == 0);
  CHECK(
      uri.to_string() ==
      win::uri_from_path(win::current_dir()) + "/path1/path2");
}

#endif
