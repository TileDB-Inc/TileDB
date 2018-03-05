/**
 * @file   tiledb_cpp_api_version.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * C++ API for TileDB Version.
 */

#ifndef TILEDB_CPP_API_VERSION_H
#define TILEDB_CPP_API_VERSION_H

#include "tiledb.h"

#include <iostream>

namespace tiledb {

/** TileDB version. Format: `major_minor_rev`. */
class TILEDB_EXPORT Version {
 public:
  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the major number. */
  inline int major() const {
    return major_;
  }

  /** Returns the minor number. */
  inline int minor() const {
    return minor_;
  }

  /** Returns the patch number. */
  inline int patch() const {
    return patch_;
  }

  /**
   * @return TileDB library version object
   */
  static Version version() {
    Version ret;
    tiledb_version(&ret.major_, &ret.minor_, &ret.patch_);
    return ret;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Major number. */
  int major_;

  /** Minor number. */
  int minor_;

  /** Patch number. */
  int patch_;
};

/** Prints to an output stream. */
TILEDB_EXPORT inline std::ostream& operator<<(
    std::ostream& os, const Version& v) {
  os << "TileDB v" << v.major() << '.' << v.minor() << '.' << v.patch();
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_VERSION_H
