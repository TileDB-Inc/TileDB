/**
 * @file categorical_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines class CategoricalFilter.
 */

#include "tiledb/sm/filter/categorical_filter.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include <sstream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

void CategoricalFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  fprintf(out, "Categorical");
}

Status CategoricalFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // No-Op filter for now
  throw_if_not_ok(output_metadata->append_view(input_metadata));
  throw_if_not_ok(output->append_view(input));

  return Status::Ok();
}

Status CategoricalFilter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  // No-op buffer for now
  throw_if_not_ok(output_metadata->append_view(input_metadata));
  throw_if_not_ok(output->append_view(input));

  return Status::Ok();
}

Status CategoricalFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr)
    throw StatusException(
        Status_FilterError("Categorical filter error; Invalid option value"));

  switch (option) {
    case FilterOption::CATEGORIES: {
      throw StatusException(Status_FilterError("Not actually implemented"));
      break;
    }
    default:
      throw StatusException(
          Status_FilterError("Categorical filter error; Unknown option"));
  }

  return Status::Ok();
}

Status CategoricalFilter::get_option_impl(
    FilterOption option, void* value) const {
  (void)value;
  switch (option) {
    case FilterOption::CATEGORIES:
      throw StatusException(
          Status_FilterError("Still nopes on the implemented bit"));
      break;
    default:
      throw StatusException(
          Status_FilterError("Categorical filter error; Unknown option"));
  }
  return Status::Ok();
}

CategoricalFilter* CategoricalFilter::clone_impl() const {
  return tdb_new(CategoricalFilter);
}

void CategoricalFilter::serialize_impl(Serializer& serializer) const {
  (void)serializer;
  // No-op for now
}

}  // namespace sm
}  // namespace tiledb
