/**
 * @file   float_scaling_filter.cc
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
 * This file implements class FloatScalingFilter.
 */

#include "tiledb/sm/filter/float_scaling_filter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/filter_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

void FloatScalingFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(
      out,
      "FloatScalingFilter: BIT_WIDTH=%llu, SCALE=%lf, OFFSET=%lf",
      bit_width_,
      scale_,
      offset_);
}

Status FloatScalingFilter::run_forward(
    const Tile& tile,
    Tile* const tile_offsets,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  (void)tile;
  (void)tile_offsets;
  (void)input_metadata;
  (void)input;
  (void)output_metadata;
  (void)output;
  return Status::Ok();
}

Status FloatScalingFilter::run_reverse(
    const Tile& tile,
    Tile* const tile_offsets,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)tile;
  (void)tile_offsets;
  (void)input_metadata;
  (void)input;
  (void)output_metadata;
  (void)output;
  (void)config;
  return Status::Ok();
}

Status FloatScalingFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr)
    return LOG_STATUS(
        Status_FilterError("Float scaling filter error; invalid option value"));

  switch (option) {
    case FilterOption::SCALE_FLOAT_BITWIDTH: {
      bit_width_ = *(uint64_t*)value;
    } break;
    case FilterOption::SCALE_FLOAT_FACTOR: {
      scale_ = *(double*)value;
    } break;
    case FilterOption::SCALE_FLOAT_OFFSET: {
      offset_ = *(double*)value;
    } break;
    default:
      return LOG_STATUS(
          Status_FilterError("Float scaling filter error; unknown option"));
  }

  return Status::Ok();
}

Status FloatScalingFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::SCALE_FLOAT_BITWIDTH: {
      *(uint64_t*)value = bit_width_;
    } break;
    case FilterOption::SCALE_FLOAT_FACTOR: {
      *(double*)value = scale_;
    } break;
    case FilterOption::SCALE_FLOAT_OFFSET: {
      *(double*)value = offset_;
    } break;
    default:
      return LOG_STATUS(
          Status_FilterError("Float scaling filter error; unknown option"));
  }
  return Status::Ok();
}

/** Returns a new clone of this filter. */
FloatScalingFilter* FloatScalingFilter::clone_impl() const {
  return new FloatScalingFilter(bit_width_, scale_, offset_);
}

}  // namespace sm
}  // namespace tiledb