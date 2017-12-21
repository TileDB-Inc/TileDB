/**
 * @file   tdbpp_urils.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Utils for C++ API.
 */

#ifndef TILEDB_TDBPP_UTILS_H
#define TILEDB_TDBPP_UTILS_H

#include "tiledb.h"
#include <iostream>
#include <functional>

namespace tdb {
  struct Version {
    int major, minor, rev;
  };

  inline const Version version() {
    Version ret;
    tiledb_version(&ret.major, &ret.minor, &ret.rev);
    return ret;
  }

}

inline std::ostream &operator<<(std::ostream &os, const tdb::Version &v) {
  os << "TileDB v" << v.major << '.' << v.minor << '.' << v.rev;
  return os;
}

#endif //TILEDB_TDBPP_UTILS_H
