/**
 * @file   home_directory.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file implements standalone function home_directory().
 */

#ifdef _WIN32
#include <ShlObj.h>
#include <Windows.h>

#include <codecvt>
#endif

#include "home_directory.h"
#include "tiledb/common/scoped_executor.h"

#include <filesystem>
#include <system_error>

namespace tiledb::common::filesystem {

std::string home_directory() {
  std::string path = "";
#ifdef _WIN32
  wchar_t* home;
  auto _ = ScopedExecutor([&]() {
    if (home)
      CoTaskMemFree(home);
  });
  if (SHGetKnownFolderPath(FOLDERID_Profile, 0, NULL, &home) == S_OK) {
    path = std::wstring_convert<std::codecvt_utf8<wchar_t>>{}.to_bytes(home);
  }
  // Ensure path has trailing slash.
  if (path.back() != '\\') {
    path.push_back('\\');
  }
#else
  const char* home = std::getenv("HOME");
  if (home != nullptr) {
    path = home;
  }
  // Ensure path has trailing slash.
  if (path.back() != '/') {
    path.push_back('/');
  }
#endif
  return path;
}

}  // namespace tiledb::common::filesystem
