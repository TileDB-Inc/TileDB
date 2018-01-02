#ifdef _WIN32

#include "catch.hpp"

#include <cassert>
#include <status.h>
#include <win32_filesystem.h>

using namespace tiledb;

struct Win32Fx {
  const std::string TEMP_DIR = win32::current_dir() + "/";

  Win32Fx() {
    bool success = false;
    // if (path_exists(TEMP_DIR + "tiledb_test_dir")) {
    //   success = remove_path(TEMP_DIR + "tiledb_test_dir");
    //   assert(success);
    // }
    // if (path_exists(TEMP_DIR + "tiledb_test_file")) {
    //   success = remove_path(TEMP_DIR + "tiledb_test_file");
    //   assert(success);
    // }
  }

  ~Win32Fx() {
    // bool success = remove_path(TEMP_DIR + "tiledb_test_dir");
    // assert(success);
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

  // st = hdfs::create_file(fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"));
  // CHECK(st.ok());

  // tSize buffer_size = 100000;
  // auto write_buffer = new char[buffer_size];
  // for (int i = 0; i < buffer_size; i++) {
  //   write_buffer[i] = 'a' + (i % 26);
  // }
  // st = hdfs::write_to_file(
  //     fs,
  //     URI("hdfs:///tiledb_test_dir/tiledb_test_file"),
  //     write_buffer,
  //     buffer_size);
  // CHECK(st.ok());

  // auto read_buffer = new char[26];
  // st = hdfs::read_from_file(
  //     fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"), 0, read_buffer, 26);
  // CHECK(st.ok());

  // bool allok = true;
  // for (int i = 0; i < 26; i++) {
  //   if (read_buffer[i] != static_cast<char>('a' + i)) {
  //     allok = false;
  //     break;
  //   }
  // }
  // CHECK(allok == true);

  // st = hdfs::read_from_file(
  //     fs, URI("hdfs:///tiledb_test_dir/tiledb_test_file"), 11, read_buffer, 26);
  // CHECK(st.ok());

  // allok = true;
  // for (int i = 0; i < 26; ++i) {
  //   if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
  //     allok = false;
  //     break;
  //   }
  // }
  // CHECK(allok == true);

  // std::vector<std::string> paths;
  // st = hdfs::ls(fs, URI("hdfs:///"), &paths);
  // CHECK(st.ok());
  // CHECK(paths.size() > 0);

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
