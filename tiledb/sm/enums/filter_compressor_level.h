/**
 * @file filter_option.h
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
 * This defines the TileDB FilterCompressorLevel enum that maps to
 * tiledb_filter_compressor_level_t C-API enum.
 */

#ifndef TILEDB_FILTER_COMPRESSOR_LEVEL_H
#define TILEDB_FILTER_COMPRESSOR_LEVEL_H

#include <cassert>
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

/** Defines the filter compressor level. */
enum class FilterCompressorLevel : int {
#define TILEDB_FILTER_COMPRESSOR_LEVEL_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_FILTER_COMPRESSOR_LEVEL_ENUM
};

/** Returns a string representation of the input FilterCompressorLevel type. */
inline const std::string& filter_compressor_level_str(
    FilterCompressorLevel filter_compressor_level_) {
  switch (filter_compressor_level_) {
    case FilterCompressorLevel::FILTER_COMPRESSOR_LEVEL_DEFAULT:
      return constants::filter_compressor_level_default_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the FilterCompressionLevel type given a string representation. */
inline Status filter_compressor_level_enum(
    const std::string& filter_compressor_level_str,
    FilterCompressorLevel* filter_compressor_level_) {
  if (filter_compressor_level_str ==
      constants::filter_compressor_level_default_str)
    *filter_compressor_level_ =
        FilterCompressorLevel::FILTER_COMPRESSOR_LEVEL_DEFAULT;
  else
    return Status::Error(
        "Invalid FilterCompressorLevel " + filter_compressor_level_str);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_COMPRESSION_LEVEL_H
