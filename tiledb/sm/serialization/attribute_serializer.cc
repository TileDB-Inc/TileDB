/**
 * @file   attribute_serializer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file defines serialization functions for Attribute.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/compat/json.h>
#include <capnp/serialize.h>
#include "tiledb/sm/serialization/capnp_utils.h"
#endif

#include "tiledb/sm/serialization/attribute_serializer.h"
#include "tiledb/sm/serialization/filter_pipeline_serializer.h"

#include <set>

using namespace tiledb::common;

namespace tiledb {
namespace sm {
namespace serialization {

#ifdef TILEDB_SERIALIZATION


Status attribute_serialize_to_capnp(
    const Attribute* attribute, capnp::Attribute::Builder* attribute_builder) {
  if (attribute == nullptr)
    return LOG_STATUS(Status::SerializationError(
        "Error serializing attribute; attribute is null."));

  attribute_builder->setName(attribute->name());
  attribute_builder->setType(datatype_str(attribute->type()));
  attribute_builder->setCellValNum(attribute->cell_val_num());
  attribute_builder->setNullable(attribute->nullable());

  // Get the fill value from `attribute`.
  const void* fill_value;
  uint64_t fill_value_size;
  uint8_t fill_validity = true;
  if (!attribute->nullable())
    RETURN_NOT_OK(attribute->get_fill_value(&fill_value, &fill_value_size));
  else
    RETURN_NOT_OK(attribute->get_fill_value(
        &fill_value, &fill_value_size, &fill_validity));

  // Copy the fill value buffer into a capnp vector of bytes.
  auto capnpFillValue = kj::Vector<uint8_t>();
  capnpFillValue.addAll(kj::ArrayPtr<uint8_t>(
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(fill_value)),
      fill_value_size));

  // Store the fill value vector of bytes.
  attribute_builder->setFillValue(capnpFillValue.asPtr());

  // Set the validity fill value.
  attribute_builder->setFillValueValidity(fill_validity ? true : false);

  const auto& filters = attribute->filters();
  auto filter_pipeline_builder = attribute_builder->initFilterPipeline();
  RETURN_NOT_OK(filter_pipeline_serialize_to_capnp(&filters, &filter_pipeline_builder));

  return Status::Ok();
}

std::tuple<Status, Attribute&&> attribute_derialize_from_capnp(
    const capnp::Attribute::Reader& attribute_reader) {
  Datatype datatype = Datatype::ANY;
  Status st;
  st = datatype_enum(attribute_reader.getType(), &datatype);
  if(!st.ok()) {
      return {st, std::move(Attribute())};
  }

  Attribute attribute(
      attribute_reader.getName(), datatype);

  st = attribute.set_cell_val_num(attribute_reader.getCellValNum());
  if(!st.ok()) {
     return {st, std::move(attribute)}; 
  }

  // Set nullable.
  const bool nullable = attribute_reader.getNullable();
  st = attribute.set_nullable(nullable);
  if(!st.ok()) {
     return {st, std::move(attribute)}; 
  }  

  // Set the fill value.
  if (attribute_reader.hasFillValue()) {
    auto fill_value = attribute_reader.getFillValue();
    if (nullable) {
      attribute.set_fill_value(
              fill_value.asBytes().begin(),
              fill_value.size(),
              attribute_reader.getFillValueValidity());
    } else {
      attribute.set_fill_value(fill_value.asBytes().begin(), fill_value.size());
    }
  }

  // Set filter pipelines.
  if (attribute_reader.hasFilterPipeline()) {
    auto filter_pipeline_reader = attribute_reader.getFilterPipeline();
    tdb_unique_ptr<FilterPipeline> filters;
    st = filter_pipeline_deserialize_from_capnp(filter_pipeline_reader, &filters);
    if (!st.ok()) {
      return {st, std::move(attribute)};
    }
    st = attribute.set_filter_pipeline(filters.get());
    if (!st.ok()) {
      return {st, std::move(attribute)};
    }
  }

  // Set nullable.
  st = attribute.set_nullable(attribute_reader.getNullable());
  if(!st.ok()) {
      return {st, std::move(attribute)};
  }

  return {Status::Ok(), std::move(attribute)};
}
 

#else 

Status attribute_serialize_to_capnp(
    const Attribute* attribute, capnp::Attribute::Builder* attribute_builder) {
  return LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled."));
}

std::tuple<Status, Attribute&&> attribute_deserialize_from_capnp(
    const capnp::Attribute::Reader& attribute_reader) {
  return {LOG_STATUS(Status::SerializationError(
      "Cannot serialize; serialization not enabled.")), std::move(Attribute())};
}



#endif  // TILEDB_SERIALIZATION


}  // namespace serialization
}  // namespace sm
}  // namespace tiledb    