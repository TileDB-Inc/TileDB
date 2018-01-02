#ifdef _WIN32

#include "catch.hpp"

#include <cassert>
#include <status.h>
#include <win32_filesystem.h>

using namespace tiledb;

static bool starts_with(const std::string& value, const std::string& prefix) {
  if (prefix.size() > value.size())
    return false;
  return std::equal(prefix.begin(), prefix.end(), value.begin());
}

static bool ends_with(const std::string& value, const std::string& suffix) {
  if (suffix.size() > value.size())
    return false;
  return std::equal(suffix.rbegin(), suffix.rend(), value.rbegin());
}

struct Win32Fx {
  const std::string TEMP_DIR = win32::current_dir() + "/";

  Win32Fx() {
    bool success = false;
    if (path_exists(TEMP_DIR + "tiledb_test_dir")) {
      success = remove_path(TEMP_DIR + "tiledb_test_dir");
      assert(success);
    }
    if (path_exists(TEMP_DIR + "tiledb_test_file")) {
      success = remove_path(TEMP_DIR + "tiledb_test_file");
      assert(success);
    }
  }

  ~Win32Fx() {
    bool success = remove_path(TEMP_DIR + "tiledb_test_dir");
    assert(success);
  }

  bool path_exists(std::string path) {
    return win32::is_file(path) || win32::is_dir(path);
  }

  bool remove_path(std::string path) {
    return win32::remove_path(path).ok();
  }
};

TEST_CASE_METHOD(Win32Fx, "Test Win32 filesystem", "[win32]") {
  const std::string test_dir = win32::current_dir() + "/tiledb_test_dir";
  const std::string test_file = win32::current_dir() + "/tiledb_test_dir/tiledb_test_file";
  Status st;

  CHECK(!win32::is_dir(URI(test_dir).to_string()));
  st = win32::create_dir(URI(test_dir).to_string());
  CHECK(st.ok());
  CHECK(!win32::is_file(URI(test_dir).to_string()));
  CHECK(win32::is_dir(URI(test_dir).to_string()));

  CHECK(!win32::is_file(URI(test_file).to_string()));
  st = win32::create_file(URI(test_file).to_string());
  CHECK(st.ok());
  CHECK(win32::is_file(URI(test_file).to_string()));
  st = win32::create_file(URI(test_file).to_string());
  CHECK(st.ok());
  CHECK(win32::is_file(URI(test_file).to_string()));

  st = win32::create_file(URI(test_file).to_string());
  CHECK(st.ok());
  st = win32::remove_path(URI(test_file).to_string());
  CHECK(st.ok());
  CHECK(!win32::is_file(URI(test_file).to_string()));

  st = win32::remove_path(URI(test_dir).to_string());
  CHECK(st.ok());
  CHECK(!win32::is_dir(URI(test_dir).to_string()));

  st = win32::create_dir(URI(test_dir).to_string());
  CHECK(st.ok());
  st = win32::create_file(URI(test_file).to_string());
  CHECK(st.ok());
  st = win32::remove_path(URI(test_dir).to_string());
  CHECK(st.ok());
  CHECK(!win32::is_dir(URI(test_dir).to_string()));

  st = win32::create_dir(URI(test_dir).to_string());
  st = win32::create_file(URI(test_file).to_string());
  CHECK(st.ok());

  const unsigned  buffer_size = 100000;
  auto write_buffer = new char[buffer_size];
  for (int i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st = win32::write_to_file(
      URI(test_file).to_string(),
      write_buffer,
      buffer_size);
  CHECK(st.ok());

  auto read_buffer = new char[26];
  st = win32::read_from_file(
      URI(test_file).to_string(), 0, read_buffer, 26);
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = win32::read_from_file(
     URI(test_file).to_string(), 11, read_buffer, 26);
  CHECK(st.ok());

  allok = true;
  for (int i = 0; i < 26; ++i) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  std::vector<std::string> paths;
  st = win32::ls(URI(test_dir).to_string(), &paths);
  CHECK(st.ok());
  CHECK(paths.size() == 1);
  CHECK(starts_with(paths[0], "file:///"));
  CHECK(ends_with(paths[0], "tiledb_test_dir/tiledb_test_file"));
  CHECK(win32::is_file(paths[0]));

  // uint64_t nbytes = 0;
  // st = hdfs::file_size(
  //     fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"), &nbytes);
  // CHECK(st.ok());
  // CHECK(nbytes == buffer_size);

  // st = hdfs::remove_path(fs, URI("hdfs:///tiledb_test_dir/i_dont_exist"));
  // CHECK(!st.ok());

  // st = hdfs::remove_file(fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"));
  // CHECK(st.ok());

  // st = hdfs::remove_path(fs, URI("hdfs:///tiledb_test_dir"));
  // CHECK(st.ok());

  // st = hdfs::disconnect(fs);
  // CHECK(st.ok());
}

#endif // _WIN32
