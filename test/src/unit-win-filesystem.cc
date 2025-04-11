/**
 * @file   unit-win-filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the Windows filesystem functionality.
 */

#ifdef _WIN32

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"

#include <cassert>
#include <filesystem>
#include "tiledb/common/random/prng.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/crypto.h"
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"

#include <Windows.h>

using namespace tiledb::common;
using namespace tiledb::sm;

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

struct WinFx {
  Win win_;
  Config vfs_config_;
  TemporaryLocalDirectory temp_dir_;

  WinFx() {
    REQUIRE(win_.init(vfs_config_).ok());
  }

  ~WinFx() = default;

  bool path_exists(std::string path) {
    return win_.is_file(path) || win_.is_dir(path);
  }
};

TEST_CASE_METHOD(WinFx, "Test Windows filesystem", "[windows][filesystem]") {
  using tiledb::sm::path_win::is_win_path;
  const std::string test_dir_path = temp_dir_.path() + "/win_tests";
  const std::string test_file_path =
      temp_dir_.path() + "/win_tests/tiledb_test_file";
  URI test_dir(test_dir_path);
  URI test_file(test_file_path);
  Status st;

  CHECK(is_win_path("C:\\path"));
  CHECK(is_win_path("C:path"));
  CHECK(is_win_path("c:path1\\path2"));
  CHECK(is_win_path("..\\path"));
  CHECK(is_win_path("\\path"));
  CHECK(is_win_path("path\\"));
  CHECK(is_win_path("\\\\path1\\path2"));
  CHECK(is_win_path("path1\\path2"));
  CHECK(is_win_path("path"));
  CHECK(
      is_win_path("path1/path2"));  // Change - formerly rejected, now accepted.
  CHECK(is_win_path("../path"));
  CHECK(is_win_path("/path"));
  CHECK(is_win_path("path/"));
  CHECK(is_win_path("//path1/path2"));
  CHECK(is_win_path("path1/path2"));
  CHECK(is_win_path("c:/path"));
  CHECK(is_win_path("c:path1/path2"));
  CHECK(is_win_path("c://path1/path2"));
  CHECK(is_win_path("c://path1//path2"));
  CHECK(is_win_path("c:\\\\path1\\\\path2"));
  CHECK(is_win_path("\\"));
  CHECK(is_win_path("\\\\"));
  CHECK(is_win_path("/"));
  CHECK(is_win_path("//"));
  // (Even file:) 'URL's are not being considered as windows paths by
  // is_win_path.
  CHECK(!is_win_path("file:///c:path"));
  CHECK(!is_win_path("file:///c:path1\\path2"));
  CHECK(!is_win_path("file:\\\\\\c:path"));
  CHECK(!is_win_path("file:\\\\\\c:path1\\path2"));
  CHECK(!is_win_path("file:///path1/path2"));

  CHECK(Win::abs_path(test_dir_path) == test_dir_path);
  CHECK(Win::abs_path(test_file_path) == test_file_path);
  CHECK(Win::abs_path("") == Win::current_dir());
  CHECK(Win::abs_path("C:\\") == "C:\\");
  CHECK(Win::abs_path("C:\\path1\\path2\\") == "C:\\path1\\path2\\");
  CHECK(Win::abs_path("C:\\..") == "C:\\");
  CHECK(Win::abs_path("C:\\..\\path1") == "C:\\path1");
  CHECK(Win::abs_path("C:\\path1\\.\\..\\path2\\") == "C:\\path2\\");
  CHECK(Win::abs_path("C:\\path1\\.\\path2\\..\\path3") == "C:\\path1\\path3");
  CHECK(
      Win::abs_path("path1\\path2\\..\\path3") ==
      Win::current_dir() + "\\path1\\path3");
  CHECK(Win::abs_path("path1") == Win::current_dir() + "\\path1");
  CHECK(Win::abs_path("path1\\path2") == Win::current_dir() + "\\path1\\path2");
  CHECK(
      Win::abs_path("path1\\path2\\..\\path3") ==
      Win::current_dir() + "\\path1\\path3");

  CHECK(win_.is_dir(temp_dir_.path()));
  CHECK(!win_.is_dir(test_dir.to_path()));
  st = win_.create_dir(test_dir.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_file(test_dir.to_path()));
  CHECK(win_.is_dir(test_dir.to_path()));

  CHECK(!win_.is_file(test_file.to_path()));
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  CHECK(win_.is_file(test_file.to_path()));
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  CHECK(win_.is_file(test_file.to_path()));

  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  st = win_.remove_file(test_file.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_file(test_file.to_path()));

  st = win_.remove_dir(test_dir.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_dir(test_dir.to_path()));

  st = win_.create_dir(test_dir.to_path());
  CHECK(st.ok());
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());
  st = win_.remove_dir(test_dir.to_path());
  CHECK(st.ok());
  CHECK(!win_.is_dir(test_dir.to_path()));

  st = win_.create_dir(test_dir.to_path());
  st = win_.touch(test_file.to_path());
  CHECK(st.ok());

  const unsigned buffer_size = 100000;
  std::vector<char> write_buffer(buffer_size);
  for (unsigned i = 0; i < buffer_size; i++) {
    write_buffer[i] = 'a' + (i % 26);
  }
  st =
      win_.write(test_file.to_path(), write_buffer.data(), write_buffer.size());
  CHECK(st.ok());
  st = win_.sync(test_file.to_path());
  CHECK(st.ok());

  std::vector<char> read_buffer(26);
  st =
      win_.read(test_file.to_path(), 0, read_buffer.data(), read_buffer.size());
  CHECK(st.ok());

  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok == true);

  st = win_.read(
      test_file.to_path(), 11, read_buffer.data(), read_buffer.size());
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
  st = win_.ls(test_dir.to_path(), &paths);
  CHECK(st.ok());
  CHECK(paths.size() == 1);
  CHECK(!starts_with(paths[0], "file:///"));
  CHECK(ends_with(paths[0], "win_tests\\tiledb_test_file"));
  CHECK(win_.is_file(paths[0]));

  uint64_t nbytes = 0;
  st = win_.file_size(test_file.to_path(), &nbytes);
  CHECK(st.ok());
  CHECK(nbytes == buffer_size);

  st =
      win_.remove_file(URI("file:///tiledb_test_dir/i_dont_exist").to_string());
  CHECK(!st.ok());

  st = win_.move_path(test_file.to_path(), URI(test_file_path + "2").to_path());
  CHECK(st.ok());
  CHECK(!win_.is_file(test_file.to_path()));
  CHECK(win_.is_file(URI(test_file_path + "2").to_path()));
}

TEST_CASE_METHOD(
    WinFx, "Test writing large files", "[.nightly_only][windows][large-file]") {
  const uint64_t five_gigabytes = static_cast<uint64_t>(5) << 30;
  std::string file = temp_dir_.path() + "\\large-file";
  std::vector<uint8_t> buffer(five_gigabytes);

  // We use a prime period to catch errors where the 4GB buffer chunks are
  // written in the wrong place.
  const uint8_t sequence_period = 59;

  uint8_t i = 0;
  std::generate(buffer.begin(), buffer.end(), [&]() {
    auto val = i;
    i = (i + 1) % sequence_period;
    return val;
  });

  Buffer expected_buffer;
  REQUIRE(expected_buffer.realloc(Crypto::MD5_DIGEST_BYTES).ok());

  REQUIRE(win_.write(file, buffer.data(), buffer.size()).ok());

  REQUIRE(Crypto::md5(buffer.data(), buffer.size(), &expected_buffer).ok());

  std::fill(buffer.begin(), buffer.end(), 0);

  REQUIRE(win_.read(file, 0, buffer.data(), buffer.size()).ok());

  Buffer actual_buffer;
  REQUIRE(actual_buffer.realloc(Crypto::MD5_DIGEST_BYTES).ok());

  REQUIRE(Crypto::md5(buffer.data(), buffer.size(), &actual_buffer).ok());

  REQUIRE(
      std::memcmp(
          expected_buffer.data(),
          actual_buffer.data(),
          Crypto::MD5_DIGEST_BYTES) == 0);
}

// Uses RAII to temporarily change the Win32 thread UI language.
class ChangeThreadUILanguage {
 public:
  ChangeThreadUILanguage(LANGID langid) {
    old_langid_ = ::GetThreadUILanguage();
    ::SetThreadUILanguage(langid);
  }
  ~ChangeThreadUILanguage() {
    ::SetThreadUILanguage(old_langid_);
  }

 private:
  LANGID old_langid_;
};

// This test requires the Greek language pack to be installed.
TEST_CASE("Test UTF-8 error messages", "[.hide][windows][utf8-msgs]") {
  // Change the thread UI language to Greek, to test that an error message with
  // Unicode characters is received correctly.
  ChangeThreadUILanguage change_langid(
      MAKELANGID(LANG_GREEK, SUBLANG_GREEK_GREECE));

  Win win;
  REQUIRE(win.init(Config()).ok());
  // NUL is a special file on Windows; deleting it should always fail.
  Status st = win.remove_file("NUL");
  REQUIRE(!st.ok());
  auto message = st.message();
  auto expected = u8"Δεν επιτρέπεται η πρόσβαση.";  // Access denied.
  REQUIRE(message.find((char*)expected) != std::string::npos);
}

#endif  // _WIN32
