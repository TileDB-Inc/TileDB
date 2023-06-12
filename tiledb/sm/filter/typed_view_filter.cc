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

namespace tiledb {
namespace sm {

TypedViewFilter::TypedViewFilter(
    const Datatype& filtered_datatype, const Datatype& unfiltered_datatype)
    : Filter(FilterType::FILTER_TYPED_VIEW)
    , filtered_datatype_(filtered_datatype)
    , unfiltered_datatype_(unfiltered_datatype) {
}

TypedViewFilter* TypedViewFilter::clone_impl() const {
  return tdb_new(TypedViewFilter, filtered_datatype_, unfiltered_datatype_);
}

void TypedViewFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;

  fprintf(
      out,
      "TypedView, FILTERED_DATATYPE=%s, UNFILTERED_DATATYPE=%s",
      datatype_str(filtered_datatype_).c_str(),
      datatype_str(unfiltered_datatype_).c_str());
}

Datatype TypedViewFilter::output_datatype() const {
  return filtered_datatype_;
}

Status TypedViewFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr) {
    throw StatusException(
        Status_FilterError("Typed view filter error; invalid option value"));
  }
  Datatype datatype{*(Datatype*)value};
  ensure_datatype_is_valid(datatype);

  switch (option) {
    case FilterOption::TYPED_VIEW_FILTERED_DATATYPE:
      filtered_datatype_ = datatype;
      break;
    case FilterOption::TYPED_VIEW_UNFILTERED_DATATYPE:
      unfiltered_datatype_ = datatype;
      break;
    default:
      throw StatusException(
          Status_FilterError("Typed view filter error; unknown option"));
  }

  return Status::Ok();
}

Status TypedViewFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::TYPED_VIEW_FILTERED_DATATYPE:
      *(Datatype*)value = filtered_datatype_;
      break;
    case FilterOption::TYPED_VIEW_UNFILTERED_DATATYPE:
      *(Datatype*)value = unfiltered_datatype_;
      break;
    default:
      throw StatusException(
          Status_FilterError("Typed view filter error; unknown option"));
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
  auto input_parts = input->buffers();
  uint32_t num_parts = static_cast<uint32_t>(input_parts.size());
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));
  for (auto& i : input_parts) {
    uint32_t size = i.size();
    uint32_t count = size / datatype_size(unfiltered_datatype_);
    uint32_t new_size = count * datatype_size(filtered_datatype_);
    RETURN_NOT_OK(output_metadata->write(&new_size, sizeof(uint32_t)));
    RETURN_NOT_OK(output->prepend_buffer(new_size));

    // TODO: Use actual types set by filter options.
    using Unfiltered = uint64_t;
    using Filtered = int8_t;

    const Unfiltered* part_data = static_cast<const Unfiltered*>(i.data());
    for (uint32_t j = 0; j < count; j++) {
      Unfiltered elem = part_data[j];
      Filtered converted_elem = static_cast<Filtered>(elem);
      RETURN_NOT_OK(output->write(&converted_elem, sizeof(Filtered)));
      if (j != count - 1) {
        output->advance_offset(sizeof(Filtered));
      }
    }
  }

  return Status::Ok();
}

Status TypedViewFilter::run_reverse(
    const Tile&,
    Tile*,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  // TODO: Use actual types set by filter options.
  using Unfiltered = uint64_t;
  using Filtered = int8_t;

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    uint32_t count = part.size() / sizeof(Filtered);
    const Filtered* part_data = static_cast<const Filtered*>(part.data());
    for (uint32_t j = 0; j < count; j++) {
      Unfiltered elem = static_cast<Unfiltered>(part_data[j]);
      RETURN_NOT_OK(output->write(&elem, sizeof(Unfiltered)));
    }
  }

  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));
  return Status::Ok();
}

void TypedViewFilter::serialize_impl(tiledb::sm::Serializer& serializer) const {
  serializer.write(static_cast<uint8_t>(filtered_datatype_));
  serializer.write(static_cast<uint8_t>(unfiltered_datatype_));
}

}  // namespace sm
}  // namespace tiledb
