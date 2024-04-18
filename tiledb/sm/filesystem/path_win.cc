/**
 * @file filesystem/path_win.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file includes definitions about paths in a Windows filesystem. It
 * excludes all functions that need to access an actual filesystem.
 */
#ifdef _WIN32

#include "path_win.h"
#include <algorithm>
#include <string>
#include "tiledb/common/logger_public.h"
#include "tiledb/common/stdx_string.h"

#if !defined(NOMINMAX)
#define NOMINMAX  // suppress definition of min/max macros in Windows headers
#endif
#include <Shlwapi.h>
#include <wininet.h>  // For INTERNET_MAX_URL_LENGTH

using namespace tiledb::common;

namespace tiledb::sm::path_win {

std::string slashes_to_backslashes(std::string pathsegments) {
  std::replace(pathsegments.begin(), pathsegments.end(), '/', '\\');
  return pathsegments;
}

std::string uri_from_path(const std::string& path) {
  if (path.length() == 0) {
    return "";
  }

  unsigned long uri_length = INTERNET_MAX_URL_LENGTH;
  char uri[INTERNET_MAX_URL_LENGTH];
  std::string str_uri;
  if (UrlCreateFromPath(path.c_str(), uri, &uri_length, 0) != S_OK) {
    LOG_STATUS_NO_RETURN_VALUE(Status_IOError(
        std::string("Failed to convert path '" + path + "' to URI.")));
  }
  str_uri = uri;
  return str_uri;
}

std::string path_from_uri(std::string_view uri_view) {
  if (uri_view.length() == 0) {
    return "";
  }

  std::string uri(uri_view);

  std::string uri_with_scheme =
      (stdx::string::starts_with(uri, "file://") ||
       // also accept 'file:/x...'
       (stdx::string::starts_with(uri, "file:/") && uri.substr(6, 1) != "/")) ?
          uri
          // else treat as file: item on 'localhost' (empty host name)
          :
          "file:///" + uri;

  unsigned long path_length = MAX_PATH;
  char path[MAX_PATH];
  std::string str_path;
  if (PathCreateFromUrl(uri_with_scheme.c_str(), path, &path_length, 0) !=
      S_OK) {
    LOG_STATUS_NO_RETURN_VALUE(Status_IOError(std::string(
        "Failed to convert URI '" + uri_with_scheme + "' to path.")));
  }
  str_path = path;
  return str_path;
}

bool is_win_path(const std::string& p_path) {
  std::string path(slashes_to_backslashes(p_path));
  if (path.empty()) {
    // Special case to match the behavior of posix_filesystem.
    return true;
  } else if (PathIsURL(path.c_str())) {
    return false;
  } else {
    bool definitely_windows = PathIsUNC(path.c_str()) ||
                              PathGetDriveNumber(path.c_str()) != -1 ||
                              path.find('\\') != std::string::npos;
    if (definitely_windows) {
      return true;
    } else {
      // Bare relative path e.g. "filename.txt"
      return path.find('/') == std::string::npos;
    }
  }
}

}  // namespace tiledb::sm::path_win

#endif  // _WIN32
