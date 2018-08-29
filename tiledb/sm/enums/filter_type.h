/**
 * @file filter_type.h
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
 * This defines the TileDB FilterType enum that maps to tiledb_filter_type_t
 * C-API enum.
 */

#ifndef TILEDB_FILTER_TYPE_H
#define TILEDB_FILTER_TYPE_H

#include <cassert>
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/** Defines the filter type. */
enum class FilterType : char {
#define TILEDB_FILTER_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_FILTER_TYPE_ENUM
};

/** Returns the string representation of the input filter type. */
inline const std::string& filter_type_str(FilterType filter_type) {
  switch (filter_type) {
    case FilterType::COMPRESSION:
      return constants::filter_type_compression_str;
    default:
      assert(0);
      return constants::empty_str;
  }
}

/** Returns the filter type given a string representation. */
inline Status filter_type_enum(
    const std::string& filter_type_str, FilterType* filter_type) {
  if (filter_type_str == constants::filter_type_compression_str)
    *filter_type = FilterType::COMPRESSION;
  else {
    return Status::Error("Invalid FilterType " + filter_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_TYPE_H
