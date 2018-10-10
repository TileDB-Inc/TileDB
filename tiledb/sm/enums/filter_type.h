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

/**
 * A filter type.
 *
 * Note: not all of these are exposed in the C API.
 */
enum class FilterType : uint8_t {
#define TILEDB_FILTER_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_FILTER_TYPE_ENUM
  /** Internally used encryption with AES-256-GCM. */
  INTERNAL_FILTER_AES_256_GCM = 11,
};

/** Returns the string representation of the input filter type. */
inline const std::string& filter_type_str(FilterType filter_type) {
  switch (filter_type) {
    case FilterType::FILTER_GZIP:
      return constants::gzip_str;
    case FilterType::FILTER_ZSTD:
      return constants::zstd_str;
    case FilterType::FILTER_LZ4:
      return constants::lz4_str;
    case FilterType::FILTER_RLE:
      return constants::rle_str;
    case FilterType::FILTER_BZIP2:
      return constants::bzip2_str;
    case FilterType::FILTER_DOUBLE_DELTA:
      return constants::double_delta_str;
    default:
      assert(0);
      return constants::empty_str;
  }
}

/** Returns the filter type given a string representation. */
inline Status filter_type_enum(
    const std::string& filter_type_str, FilterType* filter_type) {
  if (filter_type_str == constants::gzip_str)
    *filter_type = FilterType::FILTER_GZIP;
  else if (filter_type_str == constants::zstd_str)
    *filter_type = FilterType::FILTER_ZSTD;
  else if (filter_type_str == constants::lz4_str)
    *filter_type = FilterType::FILTER_LZ4;
  else if (filter_type_str == constants::rle_str)
    *filter_type = FilterType::FILTER_RLE;
  else if (filter_type_str == constants::bzip2_str)
    *filter_type = FilterType::FILTER_BZIP2;
  else if (filter_type_str == constants::double_delta_str)
    *filter_type = FilterType::FILTER_DOUBLE_DELTA;
  else {
    return Status::Error("Invalid FilterType " + filter_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_TYPE_H
