/**
 * @file filesystem/win/path_utils.h
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
 * This file defines the PathUtils functions
 */

#ifndef TILEDB_FILESYSTEM_WIN_PATH_H
#define TILEDB_FILESYSTEM_WIN_PATH_H

namespace tiledb::sm::filesystem {

#ifdef _WIN32

class PathUtils {
  static std::string current_dir();
  static std::string abs_path(const std::string& path);
  static bool is_win_path(const std::string& path);
  static std::string uri_from_path(const std::string& path);
  static std::string path_from_uri(const std::string& uri);
};

#else

class PathUtils {
  static std::string current_dir();
  static std::string abs_path(const std::string& path);
};

#endif

} // namespace tiledb::sm::filesystem::windows

#endif  // TILEDB_FILESYSTEM_WIN_PATH_H
