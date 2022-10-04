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

using namespace tiledb::common;

namespace tiledb::sm {
enum class WebpInputFormat : uint8_t;
/**
 * TODO: Doxygen
 */
class WebpFilter : public Filter {
 public:
  struct FilterConfig {
    float quality;
    WebpInputFormat format;
  };

  /** Default setting for webp quality is 100.0 for lossless compression*/
  WebpFilter()
      : Filter(FilterType::FILTER_WEBP)
      , quality_(100.0f)
      , format_(WebpInputFormat::WEBP_NONE) {
  }

  WebpFilter(float quality, WebpInputFormat inputFormat)
      : Filter(FilterType::FILTER_WEBP)
      , quality_(quality)
      , format_(inputFormat) {
  }

  void dump(FILE* out) const override;

  Status run_forward(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override;

  Status run_forward(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  Status run_reverse(
      const Tile& tile,
      Tile* const tile_offsets,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const Config& config) const override;

  Status run_reverse(
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const;

  Status set_option_impl(FilterOption option, const void* value) override;

  Status get_option_impl(FilterOption option, void* value) const override;

  void serialize_impl(Serializer& serializer) const override;

 private:
  float quality_;
  WebpInputFormat format_;

  [[nodiscard]] WebpFilter* clone_impl() const override;
};

}  // namespace tiledb::sm
#endif  // TILEDB_WEBP

#endif  // TILEDB_WEBP_FILTER_H
