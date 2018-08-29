/**
 * @file   filter.cc
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
 * This file defines class Filter.
 */

#include "tiledb/sm/filter/filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

Filter::Filter(FilterType type) {
  pipeline_ = nullptr;
  type_ = type;
}

Filter* Filter::clone() const {
  // Call subclass-specific clone function
  auto clone = clone_impl();
  // Ensure the clone does not "belong" to any pipeline.
  clone->pipeline_ = nullptr;
  return clone;
}

Filter* Filter::create(FilterType type) {
  switch (type) {
    case FilterType::COMPRESSION:
      return new (std::nothrow) CompressionFilter();
    default:
      return nullptr;
  }
}

Status Filter::deserialize(ConstBuffer* buff, Filter** filter) {
  char type;
  RETURN_NOT_OK(buff->read(&type, sizeof(char)));
  uint32_t filter_metadata_len;
  RETURN_NOT_OK(buff->read(&filter_metadata_len, sizeof(uint32_t)));

  auto* f = create(static_cast<FilterType>(type));
  if (f == nullptr)
    return LOG_STATUS(Status::FilterError("Deserialization error."));

  auto offset = buff->offset();
  RETURN_NOT_OK_ELSE(f->deserialize_impl(buff), delete f);

  if (buff->offset() - offset != filter_metadata_len) {
    delete f;
    return LOG_STATUS(Status::FilterError(
        "Deserialization error; unexpected metadata length"));
  }

  *filter = f;

  return Status::Ok();
}

// ===== FORMAT =====
// filter type (char)
// filter metadata num bytes (unsigned int -- may be 0)
// filter metadata (char[])
Status Filter::serialize(Buffer* buff) const {
  char type = static_cast<char>(type_);
  RETURN_NOT_OK(buff->write(&type, sizeof(char)));

  // Save a pointer to store the actual length of the metadata.
  uint32_t metadata_len = 0;
  auto metadata_length_offset = buff->offset();
  RETURN_NOT_OK(buff->write(&metadata_len, sizeof(uint32_t)));

  // Filter-specific serialization
  uint64_t buff_size = buff->size();
  RETURN_NOT_OK(serialize_impl(buff));

  // Compute and write metadata length
  if (buff->size() < buff_size ||
      buff->size() - buff_size > std::numeric_limits<uint32_t>::max())
    return LOG_STATUS(
        Status::FilterError("Filter metadata exceeds max length"));
  metadata_len = static_cast<uint32_t>(buff->size() - buff_size);
  std::memcpy(
      buff->data(metadata_length_offset), &metadata_len, sizeof(uint32_t));

  return Status::Ok();
}

Status Filter::deserialize_impl(ConstBuffer* buff) {
  (void)buff;
  return Status::Ok();
}

Status Filter::serialize_impl(Buffer* buff) const {
  (void)buff;
  return Status::Ok();
}

void Filter::set_pipeline(const FilterPipeline* pipeline) {
  pipeline_ = pipeline;
}

FilterType Filter::type() const {
  return type_;
}

}  // namespace sm
}  // namespace tiledb