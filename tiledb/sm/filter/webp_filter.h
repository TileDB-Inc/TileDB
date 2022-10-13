/**
 * @file   webp_filter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements class WebpFilter.
 */

#ifndef TILEDB_WEBP_FILTER_H
#define TILEDB_WEBP_FILTER_H

#ifdef TILEDB_WEBP

#include "tiledb/common/status.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb::sm {

enum class WebpInputFormat : uint8_t;

/**
 * The WebP filter provides two options: quality and format
 * Quality is used as quality_factor setting for WebP lossy compression
 * Format is used to define colorspace format of image data
 *
 * On write this filter takes raw colorspace values (RGB, RBGA, etc) and encodes
 * into WebP format before writing data to the array.
 *
 * On read, this filter decodes WebP data and returns raw colorspace values to
 * the caller.
 *
 * This filter expects the array to provide two dimensions for X, Y pixel
 * position. Dimensions may be defined with any name, but X, Y should be at
 * dimension index 0, 1 respectively.
 */
class WebpFilter : public Filter {
 public:
  struct FilterConfig {
    float quality;
    WebpInputFormat format;
  };

  /**
   * Default setting for webp quality factor is 100.0 for lossy compression
   * Caller must set colorspace format filter option
   */
  WebpFilter()
      : Filter(FilterType::FILTER_WEBP)
      , quality_(100.0f)
      , format_(WebpInputFormat::WEBP_NONE) {
  }

  /**
   * @param quality Quality factor to use for WebP lossy compression
   * @param inputFormat Colorspace format to use for WebP compression
   */
  WebpFilter(float quality, WebpInputFormat inputFormat)
      : Filter(FilterType::FILTER_WEBP)
      , quality_(quality)
      , format_(inputFormat) {
  }

  /**
   * Dumps filter details in ASCII format
   * @param out Location to write output
   */
  void dump(FILE* out) const override;

  /**
   * Runs the filter forward, taking raw colorspace values as input and writing
   * encoded WebP data to the TileDB Array.
   *
   * @param tile Current tile on which the filter is being run
   * @param offsets_tile Offsets tile of the current tile on which the filter is
   * being run
   * @param input_metadata Buffer with metadata for `input`
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data
   * @param output Buffer with filtered data (unused by in-place filters).
   * @return Status::Ok() on success. Throws on failure
   */
  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  /**
   * Runs the filter forward, taking raw colorspace values as input and writing
   * encoded WebP data to the TileDB Array.
   *
   * @param input_metadata Buffer with metadata for `input`
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data
   * @param output Buffer with filtered data (unused by in-place filters).
   * @return Status::Ok() on success. Throws on failure
   */
  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Runs the filter in reverse, returning raw colorspace values to the client
   *
   * @param tile Current tile on which the filter is being run
   * @param offsets_tile Offsets tile of the current tile on which the filter is
   * being run
   * @param input_metadata Buffer with metadata for `input`
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data
   * @param output Buffer with filtered data (unused by in-place filters).
   * @param config Config object for query-level parameters
   * @return Status::Ok() on success. Throws on failure
   */
  Status run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  /**
   * Runs the filter in reverse, returning raw colorspace values to the client
   *
   * @param input_metadata Buffer with metadata for `input`
   * @param input Buffer with data to be filtered.
   * @param output_metadata Buffer with metadata for filtered data
   * @param output Buffer with filtered data (unused by in-place filters).
   * @return Status::Ok() on success. Throws on failure
   */
  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  /**
   * Sets an option on the filter
   *
   * @param option The filter option to set
   * @param value Value to be set for given option
   * @return Status::Ok() on success. Throws on failure
   */
  Status set_option_impl(FilterOption option, const void* value) override;

  /**
   * Gets an option from the filter
   *
   * @param option The filter option to retrieve
   * @param value Location to store found option value
   * @return Status::Ok() on success. Throws on failure
   */
  Status get_option_impl(FilterOption option, void* value) const override;

  /**
   * Serializes filter metadata
   *
   * @param serializer Serializer with buffer to store metadata
   */
  void serialize_impl(Serializer& serializer) const override;

  /**
   * Set tile extents to be used in tile-based image compression
   * This filter references these extents only on the forward pass during writes
   *
   * @param extents Extents retrieved from array Domain object
   */
  inline void set_extent(const std::vector<ByteVecValue> & extents) {
    extents_ = extents;
  }

 private:
  float quality_;
  WebpInputFormat format_;
  std::vector<ByteVecValue> extents_;

  /**
   * @return New clone of this filter
   */
  [[nodiscard]] WebpFilter* clone_impl() const override;
};

}  // namespace tiledb::sm
#endif  // TILEDB_WEBP

#endif  // TILEDB_WEBP_FILTER_H
