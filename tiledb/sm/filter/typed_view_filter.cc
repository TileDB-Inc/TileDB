/**
 * @file   typed_view_filter.cc
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
 * This file defines class TypedViewFilter.
 */

#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter_buffer.h"

#include "tiledb/sm/filter/typed_view_filter.h"

using namespace tiledb::common;

namespace {
void check_output_type_is_set(
    std::optional<tiledb::sm::Datatype> output_datatype_) {
  /* TypedViewFilter: output_datatype not set */
  if (!output_datatype_.has_value()) {
    throw std::runtime_error("TypedViewFilter: output_datatype not set");
  }
}
}  // namespace

namespace tiledb {
namespace sm {

TypedViewFilter::TypedViewFilter(std::optional<Datatype> output_datatype)
    : Filter(FilterType::FILTER_TYPED_VIEW)
    , output_datatype_(output_datatype) {
}

TypedViewFilter* TypedViewFilter::clone_impl() const {
  return new TypedViewFilter(output_datatype_);
}

void TypedViewFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  fprintf(out, "TypedView, OUTPUT_DATATYPE=%s", datatype_str(output_datatype_.value()).c_str());
}

Datatype TypedViewFilter::output_datatype() const {
  check_output_type_is_set(output_datatype_);
  return output_datatype_.value_or(Datatype::ANY);
}

Status TypedViewFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr) {
    throw Status_FilterError(
        "Float scaling filter error; invalid option value");
  }

  switch (option) {
    case FilterOption::TYPED_VIEW_OUTPUT_DATATYPE: {
      Datatype datatype{*(Datatype*)value};
      ensure_datatype_is_valid(datatype);
      output_datatype_ = datatype;
    } break;
    default:
      throw Status_FilterError("Typed view filter error; unknown option");
  }

  return Status::Ok();
}

Status TypedViewFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::TYPED_VIEW_OUTPUT_DATATYPE: {
      check_output_type_is_set(output_datatype_);
      *(Datatype*)value = output_datatype_.value();
    } break;
    default:
      throw Status_FilterError("Typed view filter error; unknown option");
  }
  return Status::Ok();
}

Status TypedViewFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  RETURN_NOT_OK(output->append_view(input));
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  return Status::Ok();
}

Status TypedViewFilter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)config;

  RETURN_NOT_OK(output->append_view(input));
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
