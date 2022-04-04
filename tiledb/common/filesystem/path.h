/**
 * @file   path.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class path.
 */

#ifndef TILEDB_PATH_H
#define TILEDB_PATH_H

#include <string>

namespace tiledb::common::filesystem {

/**
 * This class implements a minimal mimic of std::filesystem::path,
 * it holds the path of a filesystem entry, be it a path on a local
 * filesystem or a URI in the form of s3://some_s3_path on a remote
 * filesystem.
 */
class path {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  path() = delete;

  /**
   * Constructor
   *
   * @param p The path to hold
   */
  path(const std::string& p)
      : path_(p) {
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the internal path string*/
  const std::string& native() const {
    return path_;
  }

 private:
  std::string path_;
};

}  // namespace tiledb::common::filesystem

#endif  // TILEDB_PATH_H
