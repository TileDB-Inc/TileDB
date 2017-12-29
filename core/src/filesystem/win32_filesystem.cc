/**
 * @file   win32_filesystem.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
 * This file includes definitions of Win32 filesystem functions.
 */

#ifdef _WIN32

#include "win32_filesystem.h"
#include "constants.h"
#include "logger.h"
#include "utils.h"

#include <fstream>
#include <iostream>
#include <Windows.h>
#include <Shlwapi.h>
#include <wininet.h> // For INTERNET_MAX_URL_LENGTH

namespace tiledb {

namespace win32 {

std::string abs_path(const std::string& path) {
  unsigned long result_len = 0;
  char result[INTERNET_MAX_URL_LENGTH];
  std::string str_result;
  if (UrlCanonicalize(path.c_str(), result, &result_len, 0) != S_OK) {
    LOG_STATUS(Status::IOError(std::string("Cannot canonicalize path.")));
  } else {
    str_result = result;
  }
  return str_result;
}

Status create_dir(const std::string& path) {
  if (win32::is_dir(path)) {
    return LOG_STATUS(Status::IOError(
        std::string("Cannot create directory '") + path +
        "'; Directory already exists"));
  }
  CreateDirectory(path.c_str(), nullptr);
  return Status::Ok();
}

bool is_dir(const std::string& path) {
  return PathIsDirectory(path.c_str());
}

}  // namespace win32

}  // namespace tiledb

#endif // _WIN32
