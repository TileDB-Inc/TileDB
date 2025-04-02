/**
 * @file filter_type.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
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
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/stdx/utility/to_underlying.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * A filter type.
 *
 * Note: not all of these are exposed in the C API.
 */
enum class FilterType : uint8_t {
#define TILEDB_FILTER_TYPE_ENUM(id) id
#include "tiledb/api/c_api/filter/filter_api_enum.h"
#undef TILEDB_FILTER_TYPE_ENUM
  /** Internally used encryption with AES-256-GCM. */
  INTERNAL_FILTER_AES_256_GCM = 11,
};

/** Returns the string representation of the input filter type. */
inline const std::string& filter_type_str(FilterType filter_type) {
  switch (filter_type) {
    case FilterType::FILTER_NONE:
      return constants::filter_none_str;
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
    case FilterType::FILTER_BIT_WIDTH_REDUCTION:
      return constants::filter_bit_width_reduction_str;
    case FilterType::FILTER_BITSHUFFLE:
      return constants::filter_bitshuffle_str;
    case FilterType::FILTER_BYTESHUFFLE:
      return constants::filter_byteshuffle_str;
    case FilterType::FILTER_POSITIVE_DELTA:
      return constants::filter_positive_delta_str;
    case FilterType::FILTER_CHECKSUM_MD5:
      return constants::filter_checksum_md5_str;
    case FilterType::FILTER_CHECKSUM_SHA256:
      return constants::filter_checksum_sha256_str;
    case FilterType::FILTER_DICTIONARY:
      return constants::filter_dictionary_str;
    case FilterType::FILTER_SCALE_FLOAT:
      return constants::filter_scale_float_str;
    case FilterType::FILTER_XOR:
      return constants::filter_xor_str;
    case FilterType::FILTER_WEBP:
      return constants::filter_webp_str;
    case FilterType::FILTER_DELTA:
      return constants::delta_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the filter type given a string representation. */
inline Status filter_type_enum(
    const std::string& filter_type_str, FilterType* filter_type) {
  if (filter_type_str == constants::filter_none_str)
    *filter_type = FilterType::FILTER_NONE;
  else if (filter_type_str == constants::gzip_str)
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
  else if (filter_type_str == constants::filter_bit_width_reduction_str)
    *filter_type = FilterType::FILTER_BIT_WIDTH_REDUCTION;
  else if (filter_type_str == constants::filter_bitshuffle_str)
    *filter_type = FilterType::FILTER_BITSHUFFLE;
  else if (filter_type_str == constants::filter_byteshuffle_str)
    *filter_type = FilterType::FILTER_BYTESHUFFLE;
  else if (filter_type_str == constants::filter_positive_delta_str)
    *filter_type = FilterType::FILTER_POSITIVE_DELTA;
  else if (filter_type_str == constants::filter_checksum_md5_str)
    *filter_type = FilterType::FILTER_CHECKSUM_MD5;
  else if (filter_type_str == constants::filter_checksum_sha256_str)
    *filter_type = FilterType::FILTER_CHECKSUM_SHA256;
  else if (filter_type_str == constants::filter_dictionary_str)
    *filter_type = FilterType::FILTER_DICTIONARY;
  else if (filter_type_str == constants::filter_scale_float_str)
    *filter_type = FilterType::FILTER_SCALE_FLOAT;
  else if (filter_type_str == constants::filter_xor_str)
    *filter_type = FilterType::FILTER_XOR;
  else if (filter_type_str == constants::filter_webp_str)
    *filter_type = FilterType::FILTER_WEBP;
  else if (filter_type_str == constants::delta_str)
    *filter_type = FilterType::FILTER_DELTA;
  else {
    return Status_Error("Invalid FilterType " + filter_type_str);
  }
  return Status::Ok();
}

/** Throws error if the input Filtertype enum is not between 0 and 19. */
inline void ensure_filtertype_is_valid(uint8_t type) {
  if (type > 19) {
    throw std::runtime_error(
        "Invalid FilterType (" + std::to_string(type) + ")");
  }
}

/** Throws error if the input Filtertype's enum is not between 0 and 19. */
inline void ensure_filtertype_is_valid(FilterType type) {
  ensure_filtertype_is_valid(::stdx::to_underlying(type));
}

}  // namespace tiledb::sm

#endif  // TILEDB_FILTER_TYPE_H
