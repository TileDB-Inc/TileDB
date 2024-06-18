/**
 * @file   noop_filter.cc
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
 * This file defines class NoopFilter.
 */

#include "tiledb/sm/filter/noop_filter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

NoopFilter::NoopFilter(Datatype filter_data_type)
    : Filter(FilterType::FILTER_NONE, filter_data_type) {
}

NoopFilter* NoopFilter::clone_impl() const {
  return tdb_new(NoopFilter, filter_data_type_);
}

std::ostream& NoopFilter::output(std::ostream& os) const {
  os << "NoOp";
  return os;
}

void NoopFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  throw_if_not_ok(output->append_view(input));
  throw_if_not_ok(output_metadata->append_view(input_metadata));
}

Status NoopFilter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  RETURN_NOT_OK(output->append_view(input));
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
