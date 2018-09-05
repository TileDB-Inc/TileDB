/**
 * @file   noop_filter.cc
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
 * This file defines class NoopFilter.
 */

#include "tiledb/sm/filter/noop_filter.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

NoopFilter::NoopFilter()
    : Filter(FilterType::FILTER_NONE) {
}

NoopFilter* NoopFilter::clone_impl() const {
  return new NoopFilter;
}

Status NoopFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  RETURN_NOT_OK(output->append_view(input));
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  return Status::Ok();
}

Status NoopFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  RETURN_NOT_OK(output->append_view(input));
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb