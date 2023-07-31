/**
 * @file   as_built_experimental.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file declares the experimental C++ API for the as_built namespace.
 */

#ifndef TILEDB_CPP_API_AS_BUILT_EXPERIMENTAL_H
#define TILEDB_CPP_API_AS_BUILT_EXPERIMENTAL_H

#include "tiledb_experimental.h"

namespace tiledb {
class AsBuilt {
 public:
  /**
   * Dump the TileDB build configuration to a string.
   *
   * @return the TileDB build configuration in JSON format.
   */
  static std::string dump() {
    tiledb_string_t* out;
    tiledb_as_built_dump(&out);
    const char* out_ptr;
    size_t out_length;
    tiledb_string_view(out, &out_ptr, &out_length);
    std::string out_str(out_ptr, out_length);
    tiledb_string_free(&out);
    return out_str;
  }
};
}  // namespace tiledb

#endif  // TILEDB_CPP_API_AS_BUILT_EXPERIMENTAL_H
