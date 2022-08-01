/**
 * @file   filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "filter.h"
#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/buffer/buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

Filter::Filter(FilterType type) {
  type_ = type;
}

Filter* Filter::clone() const {
  // Call subclass-specific clone function
  auto clone = clone_impl();
  return clone;
}

Status Filter::get_option(FilterOption option, void* value) const {
  if (value == nullptr)
    return LOG_STATUS(
        Status_FilterError("Cannot get option; null value pointer"));

  return get_option_impl(option, value);
}

Status Filter::set_option(FilterOption option, const void* value) {
  return set_option_impl(option, value);
}

// ===== FORMAT =====
// filter type (uint8_t)
// filter metadata num bytes (uint32_t -- may be 0)
// filter metadata (char[])
void Filter::serialize(Serializer& serializer) const {
  auto type = static_cast<uint8_t>(type_);
  serializer.write(type);

  // Compute and write metadata length
  SizeComputationSerializer size_computation_serializer;
  serialize_impl(size_computation_serializer);
  uint32_t md_length = size_computation_serializer.size();
  serializer.write(md_length);

  // Filter-specific serialization
  serialize_impl(serializer);
}

Status Filter::get_option_impl(FilterOption option, void* value) const {
  (void)option;
  (void)value;
  return LOG_STATUS(Status_FilterError("Filter does not support options."));
}

Status Filter::set_option_impl(FilterOption option, const void* value) {
  (void)option;
  (void)value;
  return LOG_STATUS(Status_FilterError("Filter does not support options."));
}

void Filter::serialize_impl(Serializer&) const {
}

FilterType Filter::type() const {
  return type_;
}

void Filter::init_compression_resource_pool(uint64_t size) {
  (void)size;
}

void Filter::init_decompression_resource_pool(uint64_t size) {
  (void)size;
}

}  // namespace sm
}  // namespace tiledb
