/**
 * @file   filter_serializer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file defines class FilterSerializer.
 */

#include "tiledb/sm/serialization/filter_serializer.h"

namespace tiledb {
namespace sm {

#ifdef TILEDB_SERIALIZATION

FilterSerializer::FilterSerializer()
    : capnp_builder_(nullptr)
    , capnp_reader_(nullptr) {
}

Status FilterSerializer::init(rest::capnp::Filter::Builder* builder) {
  capnp_builder_ = builder;
  return Status::Ok();
}

Status FilterSerializer::init(const rest::capnp::Filter::Reader* reader) {
  capnp_reader_ = reader;
  return Status::Ok();
}

Status FilterSerializer::get_type(FilterType* type) const {
  RETURN_NOT_OK(filter_type_enum(capnp_reader_->getType().cStr(), type));
  return Status::Ok();
}

Status FilterSerializer::get_option(FilterOption option, void* value) const {
  auto data = capnp_reader_->getData();

  switch (option) {
    case FilterOption::COMPRESSION_LEVEL:
      *static_cast<int32_t*>(value) = data.getInt32();
      break;
    case FilterOption::BIT_WIDTH_MAX_WINDOW:
      *static_cast<uint32_t*>(value) = data.getUint32();
      break;
    case FilterOption::POSITIVE_DELTA_MAX_WINDOW:
      *static_cast<uint32_t*>(value) = data.getUint32();
      break;
    default:
      return Status::Error("Cannot deserialize filter; unknown option type.");
  }

  return Status::Ok();
}

Status FilterSerializer::set_type(FilterType type) {
  capnp_builder_->setType(filter_type_str(type));
  return Status::Ok();
}

Status FilterSerializer::set_option(FilterOption option, const void* value) {
  auto data = capnp_builder_->initData();

  switch (option) {
    case FilterOption::COMPRESSION_LEVEL:
      data.setInt32(*static_cast<const int32_t*>(value));
      break;
    case FilterOption::BIT_WIDTH_MAX_WINDOW:
      data.setUint32(*static_cast<const uint32_t*>(value));
      break;
    case FilterOption::POSITIVE_DELTA_MAX_WINDOW:
      data.setUint32(*static_cast<const uint32_t*>(value));
      break;
    default:
      return Status::Error("Cannot serialize filter; unknown option type.");
  }

  return Status::Ok();
}

#else

FilterSerializer::FilterSerializer() {
}

Status FilterSerializer::init(const void* arg) {
  (void)arg;
  return Status::Error("Cannot serialize; serialization not enabled.");
}

Status FilterSerializer::get_type(FilterType* type) const {
  (void)type;
  return Status::Error("Cannot deserialize; serialization not enabled.");
}

Status FilterSerializer::get_option(FilterOption option, void* value) const {
  (void)option;
  (void)value;
  return Status::Error("Cannot deserialize; serialization not enabled.");
}

Status FilterSerializer::set_type(FilterType type) {
  (void)type;
  return Status::Error("Cannot serialize; serialization not enabled.");
}

Status FilterSerializer::set_option(FilterOption option, const void* value) {
  (void)option;
  (void)value;
  return Status::Error("Cannot serialize; serialization not enabled.");
}

#endif

}  // namespace sm
}  // namespace tiledb
