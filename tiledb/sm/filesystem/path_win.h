/**
 * @file filesystem/path_win.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file includes declarations of Windows filesystem functions.
 */

#ifndef TILEDB_FILESYSTEM_PATH_WIN_H
#define TILEDB_FILESYSTEM_PATH_WIN_H
#ifdef _WIN32

#include <sys/types.h>
#include <string>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "uri.h"

using namespace tiledb::common;

namespace tiledb::sm::path_win {
/**
 * Converts a Windows path to a "file:///" URI.
 *
 * @param path The Windows path to convert.
 * @status A path file URI.
 */
std::string uri_from_path(const std::string& path);

/**
 * Converts a "file:///" URI to a Windows path.
 *
 * @param path The URI to convert.
 * @status A Windows path.
 */
std::string path_from_uri(std::string_view uri);

/**
 * Converts any '/' to '\\' (single-backslash) and returns the
 * possibly modified result.
 *
 * @param path The path string possibly containing '/' separators.
 * @status A possibly modified string with any '/' changed to '\\' (.
 */
std::string slashes_to_backslashes(std::string pathsegments);

/**
 * Returns true if the given string is a Windows path.
 *
 * @param path The path to check.
 * @return True if the path is a Windows path.
 */
bool is_win_path(const std::string& path);

}  // namespace tiledb::sm::path_win

#endif  // _WIN32
#endif  // TILEDB_FILESYSTEM_PATH_WIN_H
