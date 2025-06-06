/**
 * @file   home_directory.h
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
 * This file defines standalone function home_directory().
 */

#ifndef TILEDB_HOME_DIRECTORY_H
#define TILEDB_HOME_DIRECTORY_H

#include <string>

namespace tiledb::common::filesystem {

/**
 * Ensures the given path has a trailing slash appropriate for the platform.
 */
std::string ensure_trailing_slash(const std::string& path);

/**
 * Standalone function which returns the path to user's home directory.
 *
 * @invariant `sudo` does not always preserve the path to `$HOME`. Rather than
 * throw if the path does not exist, this API will return an empty string.
 */
std::string home_directory();

}  // namespace tiledb::common::filesystem

#endif  // TILEDB_HOME_DIRECTORY_H
