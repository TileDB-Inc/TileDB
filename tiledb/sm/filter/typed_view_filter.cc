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
#include "tiledb/sm/tile/tile.h"

#include "tiledb/sm/filter/typed_view_filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

TypedViewFilter::TypedViewFilter(
    Datatype filtered_datatype, Datatype unfiltered_datatype)
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
    const WriterTile& tile,
    WriterTile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto filtered_t =
      filtered_datatype_ == Datatype::ANY ? tile.type() : filtered_datatype_;
  switch (filtered_t) {
    case Datatype::FLOAT32:
      return run_forward<float>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::FLOAT64:
      return run_forward<double>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::BLOB:
    case Datatype::BOOL:
    case Datatype::UINT8:
      return run_forward<uint8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT8:
      return run_forward<int8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_forward<uint16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_forward<int16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_forward<uint32_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT32:
      return run_forward<int32_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_forward<uint64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT64:
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return run_forward<int64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    default:
      throw std::logic_error("Unsupported datatype. TODO");
  }
}

template <typename T>
Status TypedViewFilter::run_forward(
    const WriterTile& tile,
    WriterTile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto unfiltered_t = unfiltered_datatype_ == Datatype::ANY ?
                          tile.type() :
                          unfiltered_datatype_;
  switch (unfiltered_t) {
    case Datatype::FLOAT32:
      return run_forward<T, float>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::FLOAT64:
      return run_forward<T, double>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::BLOB:
    case Datatype::BOOL:
    case Datatype::UINT8:
      return run_forward<T, uint8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT8:
      return run_forward<T, int8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_forward<T, uint16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_forward<T, int16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_forward<T, uint32_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT32:
      return run_forward<T, int32_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_forward<T, uint64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT64:
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return run_forward<T, int64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    default:
      throw std::logic_error("Unsupported datatype. TODO");
  }
}

template <typename T, typename W>
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

    const W* part_data = static_cast<const W*>(i.data());
    for (uint32_t j = 0; j < count; j++) {
      W elem = part_data[j];
      T converted_elem = static_cast<T>(elem);
      RETURN_NOT_OK(output->write(&converted_elem, sizeof(T)));
      if (j != count - 1) {
        output->advance_offset(sizeof(T));
      }
    }
  }

  return Status::Ok();
}

Status TypedViewFilter::run_reverse(
    const Tile& tile,
    Tile* offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  auto filtered_t =
      filtered_datatype_ == Datatype::ANY ? tile.type() : filtered_datatype_;
  switch (filtered_t) {
    case Datatype::FLOAT32:
      return run_reverse<float>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::FLOAT64:
      return run_reverse<double>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::BLOB:
    case Datatype::BOOL:
    case Datatype::UINT8:
      return run_reverse<uint8_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT8:
      return run_reverse<int8_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::UINT16:
      return run_reverse<uint16_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT16:
      return run_reverse<int16_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::UINT32:
      return run_reverse<uint32_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT32:
      return run_reverse<int32_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::UINT64:
      return run_reverse<uint64_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT64:
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return run_reverse<int64_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    default:
      throw std::logic_error("Unsupported datatype. TODO");
  }
}

template <typename T>
Status TypedViewFilter::run_reverse(
    const Tile& tile,
    Tile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  auto unfiltered_t = unfiltered_datatype_ == Datatype::ANY ?
                          tile.type() :
                          unfiltered_datatype_;
  switch (unfiltered_t) {
    case Datatype::FLOAT32:
      return run_reverse<T, float>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::FLOAT64:
      return run_reverse<T, double>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::BLOB:
    case Datatype::BOOL:
    case Datatype::UINT8:
      return run_reverse<T, uint8_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT8:
      return run_reverse<T, int8_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::UINT16:
      return run_reverse<T, uint16_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT16:
      return run_reverse<T, int16_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::UINT32:
      return run_reverse<T, uint32_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT32:
      return run_reverse<T, int32_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::UINT64:
      return run_reverse<T, uint64_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    case Datatype::INT64:
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return run_reverse<T, int64_t>(
          tile,
          offsets_tile,
          input_metadata,
          input,
          output_metadata,
          output,
          config);
    default:
      throw std::logic_error("Unsupported datatype. TODO");
  }
}

template <typename T, typename W>
Status TypedViewFilter::run_reverse(
    const Tile&,
    Tile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    uint32_t count = part.size() / sizeof(T);
    const T* part_data = static_cast<const T*>(part.data());
    for (uint32_t j = 0; j < count; j++) {
      W elem = static_cast<W>(part_data[j]);
      RETURN_NOT_OK(output->write(&elem, sizeof(W)));
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
