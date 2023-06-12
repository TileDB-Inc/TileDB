/**
 * @file filter_option.h
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
 * This defines the TileDB FilterOption enum that maps to tiledb_filter_option_t
 * C-API enum.
 */

#ifndef TILEDB_FILTER_OPTION_H
#define TILEDB_FILTER_OPTION_H

#include <cassert>
#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

using namespace tiledb::common;

/** Defines the filter type. */
enum class FilterOption : uint8_t {
#define TILEDB_FILTER_OPTION_ENUM(id) id
#include "tiledb/api/c_api/filter/filter_api_enum.h"
#undef TILEDB_FILTER_OPTION_ENUM
};

enum class WebpInputFormat : uint8_t {
#define TILEDB_FILTER_WEBP_FORMAT(id) id
#include "tiledb/api/c_api/filter/filter_api_enum.h"
#undef TILEDB_FILTER_WEBP_FORMAT
};

/** Returns the string representation of the input FilterOption type. */
inline const std::string& filter_option_str(FilterOption filter_option_) {
  switch (filter_option_) {
    case FilterOption::COMPRESSION_LEVEL:
      return constants::filter_option_compression_level_str;
    case FilterOption::BIT_WIDTH_MAX_WINDOW:
      return constants::filter_option_bit_width_max_window_str;
    case FilterOption::POSITIVE_DELTA_MAX_WINDOW:
      return constants::filter_option_positive_delta_max_window_str;
    case FilterOption::SCALE_FLOAT_BYTEWIDTH:
      return constants::filter_option_scale_float_bytewidth;
    case FilterOption::SCALE_FLOAT_FACTOR:
      return constants::filter_option_scale_float_factor;
    case FilterOption::SCALE_FLOAT_OFFSET:
      return constants::filter_option_scale_float_offset;
    case FilterOption::WEBP_QUALITY:
      return constants::filter_option_webp_quality;
    case FilterOption::WEBP_INPUT_FORMAT:
      return constants::filter_option_webp_input_format;
    case FilterOption::WEBP_LOSSLESS:
      return constants::filter_option_webp_lossless;
    case FilterOption::TYPED_VIEW_FILTERED_DATATYPE:
      return constants::filter_option_typed_view_filtered_datatype;
    case FilterOption::TYPED_VIEW_UNFILTERED_DATATYPE:
      return constants::filter_option_typed_view_unfiltered_datatype;
    default:
      return constants::empty_str;
  }
}

/** Returns the FilterOption type given a string representation. */
inline Status filter_option_enum(
    const std::string& filter_option_str, FilterOption* filter_option_) {
  if (filter_option_str == constants::filter_option_compression_level_str)
    *filter_option_ = FilterOption::COMPRESSION_LEVEL;
  else if (
      filter_option_str == constants::filter_option_bit_width_max_window_str)
    *filter_option_ = FilterOption::BIT_WIDTH_MAX_WINDOW;
  else if (
      filter_option_str ==
      constants::filter_option_positive_delta_max_window_str)
    *filter_option_ = FilterOption::POSITIVE_DELTA_MAX_WINDOW;
  else if (filter_option_str == constants::filter_option_scale_float_bytewidth)
    *filter_option_ = FilterOption::SCALE_FLOAT_BYTEWIDTH;
  else if (filter_option_str == constants::filter_option_scale_float_factor)
    *filter_option_ = FilterOption::SCALE_FLOAT_FACTOR;
  else if (filter_option_str == constants::filter_option_scale_float_offset)
    *filter_option_ = FilterOption::SCALE_FLOAT_OFFSET;
  else if (filter_option_str == constants::filter_option_webp_quality)
    *filter_option_ = FilterOption::WEBP_QUALITY;
  else if (filter_option_str == constants::filter_option_webp_input_format)
    *filter_option_ = FilterOption::WEBP_INPUT_FORMAT;
  else if (filter_option_str == constants::filter_option_webp_lossless)
    *filter_option_ = FilterOption::WEBP_LOSSLESS;
  else if (
      filter_option_str ==
      constants::filter_option_typed_view_filtered_datatype)
    *filter_option_ = FilterOption::TYPED_VIEW_FILTERED_DATATYPE;
  else if (
      filter_option_str ==
      constants::filter_option_typed_view_unfiltered_datatype)
    *filter_option_ = FilterOption::TYPED_VIEW_UNFILTERED_DATATYPE;
  else
    return Status_Error("Invalid FilterOption " + filter_option_str);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_OPTION_H
