/**
 * @file   query.cc
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
 * This file implements class Query.
 */

#include "tiledb/sm/query/query.h"
#include "tiledb/rest/capnp/array.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

#include <cassert>
#include <iostream>
#include <sstream>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query(StorageManager* storage_manager, Array* array, URI fragment_uri)
    : array_(array)
    , storage_manager_(storage_manager) {
  assert(array != nullptr && array->is_open());

  callback_ = nullptr;
  callback_data_ = nullptr;
  layout_ = Layout::ROW_MAJOR;
  status_ = QueryStatus::UNINITIALIZED;
  auto st = array->get_query_type(&type_);
  assert(st.ok());

  if (type_ == QueryType::WRITE)
    writer_.set_storage_manager(storage_manager);
  else
    reader_.set_storage_manager(storage_manager);

  if (type_ == QueryType::READ) {
    reader_.set_storage_manager(storage_manager);
    reader_.set_array(array);
    reader_.set_array_schema(array->array_schema());
    reader_.set_fragment_metadata(array->fragment_metadata());
  } else {
    writer_.set_storage_manager(storage_manager);
    writer_.set_array(array);
    writer_.set_array_schema(array->array_schema());
    writer_.set_fragment_uri(fragment_uri);
  }
}

Query::~Query() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

const ArraySchema* Query::array_schema() const {
  if (type_ == QueryType::WRITE)
    return writer_.array_schema();
  return reader_.array_schema();
}

std::vector<std::string> Query::attributes() const {
  if (type_ == QueryType::WRITE)
    return writer_.attributes();
  return reader_.attributes();
}

Status Query::finalize() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return Status::Ok();

  RETURN_NOT_OK(writer_.finalize());
  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
}

unsigned Query::fragment_num() const {
  if (type_ == QueryType::WRITE)
    return 0;
  return reader_.fragment_num();
}

std::vector<URI> Query::fragment_uris() const {
  std::vector<URI> uris;
  if (type_ == QueryType::WRITE)
    return uris;
  return reader_.fragment_uris();
}

Status Query::get_buffer(
    const char* attribute, void** buffer, uint64_t** buffer_size) const {
  // Normalize attribute
  std::string normalized;
  RETURN_NOT_OK(ArraySchema::attribute_name_normalized(attribute, &normalized));

  // Check attribute
  auto array_schema = this->array_schema();
  if (normalized != constants::coords) {
    if (array_schema->attribute(normalized) == nullptr)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot get buffer; Invalid attribute name '") +
          normalized + "'"));
  }
  if (array_schema->var_size(normalized))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Attribute '") + normalized +
        "' is var-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(normalized, buffer, buffer_size);
  return reader_.get_buffer(normalized, buffer, buffer_size);
}

Status Query::get_buffer(
    const char* attribute,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  // Normalize attribute
  std::string normalized;
  RETURN_NOT_OK(ArraySchema::attribute_name_normalized(attribute, &normalized));

  // Check attribute
  auto array_schema = this->array_schema();
  if (normalized == constants::coords) {
    return LOG_STATUS(
        Status::QueryError("Cannot get buffer; Coordinates are not var-sized"));
  }
  if (array_schema->attribute(normalized) == nullptr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Invalid attribute name '") +
        normalized + "'"));
  if (!array_schema->var_size(normalized))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Attribute '") + normalized +
        "' is fixed-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(
        normalized, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.get_buffer(
      normalized, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
}

bool Query::has_results() const {
  if (status_ == QueryStatus::UNINITIALIZED || type_ == QueryType::WRITE)
    return false;
  return !reader_.no_results();
}

Status Query::init() {
  // Only if the query has not been initialized before
  if (status_ == QueryStatus::UNINITIALIZED) {
    // Check if the array got closed
    if (array_ == nullptr || !array_->is_open())
      return LOG_STATUS(Status::QueryError(
          "Cannot init query; The associated array is not open"));

    // Check if the array got re-opened with a different query type
    QueryType array_query_type;
    RETURN_NOT_OK(array_->get_query_type(&array_query_type));
    if (array_query_type != type_) {
      std::stringstream errmsg;
      errmsg << "Cannot init query; "
             << "Associated array query type does not match query type: "
             << "(" << query_type_str(array_query_type)
             << " != " << query_type_str(type_) << ")";
      return LOG_STATUS(Status::QueryError(errmsg.str()));
    }

    if (type_ == QueryType::READ) {
      RETURN_NOT_OK(reader_.init());
    } else {  // Write
      RETURN_NOT_OK(writer_.init());
    }
  }

  status_ = QueryStatus::INPROGRESS;

  return Status::Ok();
}

URI Query::last_fragment_uri() const {
  if (type_ == QueryType::WRITE)
    return URI();
  return reader_.last_fragment_uri();
}

Layout Query::layout() const {
  return layout_;
}

Status Query::cancel() {
  status_ = QueryStatus::FAILED;
  return Status::Ok();
}

Status Query::check_var_attr_offsets(
    const uint64_t* buffer_off,
    const uint64_t* buffer_off_size,
    const uint64_t* buffer_val_size) {
  if (buffer_off == nullptr || buffer_off_size == nullptr ||
      buffer_val_size == nullptr)
    return LOG_STATUS(Status::QueryError("Cannot use null offset buffers."));

  auto num_offsets = *buffer_off_size / sizeof(uint64_t);
  if (num_offsets == 0)
    return Status::Ok();

  uint64_t prev_offset = buffer_off[0];
  if (prev_offset >= *buffer_val_size)
    return LOG_STATUS(Status::QueryError(
        "Invalid offsets; offset " + std::to_string(prev_offset) +
        " specified for buffer of size " + std::to_string(*buffer_val_size)));

  for (uint64_t i = 1; i < num_offsets; i++) {
    if (buffer_off[i] <= prev_offset)
      return LOG_STATUS(
          Status::QueryError("Invalid offsets; offsets must be given in "
                             "strictly ascending order."));

    if (buffer_off[i] >= *buffer_val_size)
      return LOG_STATUS(Status::QueryError(
          "Invalid offsets; offset " + std::to_string(buffer_off[i]) +
          " specified for buffer of size " + std::to_string(*buffer_val_size)));

    prev_offset = buffer_off[i];
  }

  return Status::Ok();
}

Status Query::capnp(::Query::Builder* queryBuilder) const {
  STATS_FUNC_IN(serialization_query_capnp);

  queryBuilder->setType(query_type_str(this->type()));
  queryBuilder->setLayout(layout_str(this->layout()));
  queryBuilder->setStatus(query_status_str(this->status()));

  if (this->array_ != nullptr) {
    ::Array::Builder arrayBuilder = queryBuilder->initArray();
    this->array_->capnp(&arrayBuilder);
  }

  ::DomainArray::Builder subarrayBuilder = queryBuilder->initSubarray();
  if (this->array_schema() != nullptr) {
    if (this->array_schema()->domain() == nullptr)
      return tiledb::sm::Status::Error(
          "Domain was null from array schema in query::capnp()");
    switch (this->array_schema()->domain()->type()) {
      case tiledb::sm::Datatype::INT8: {
        auto subarray = this->subarray<int8_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setInt8(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::UINT8: {
        auto subarray = this->subarray<uint8_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setUint8(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::INT16: {
        auto subarray = this->subarray<int16_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setInt16(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::UINT16: {
        auto subarray = this->subarray<uint16_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setUint16(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::INT32: {
        auto subarray = this->subarray<int32_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setInt32(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::UINT32: {
        auto subarray = this->subarray<uint32_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setUint32(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::INT64: {
        auto subarray = this->subarray<int64_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setInt64(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::UINT64: {
        auto subarray = this->subarray<uint64_t>();
        if (subarray.size() > 0)
          subarrayBuilder.setUint64(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::FLOAT32: {
        auto subarray = this->subarray<float>();
        if (subarray.size() > 0)
          subarrayBuilder.setFloat32(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::FLOAT64: {
        auto subarray = this->subarray<double>();
        if (subarray.size() > 0)
          subarrayBuilder.setFloat64(
              kj::arrayPtr(subarray.data(), subarray.size()));
        break;
      }
      case tiledb::sm::Datatype::CHAR:
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::ANY:
        // Not supported domain type
        return Status::Error("Unspported domain type");
        break;
    }
  }

  std::vector<std::string> attributes = this->attributes();

  Map<::capnp::Text, ::AttributeBuffer>::Builder attributeBuffersBuilder =
      queryBuilder->initBuffers();
  auto buffers = attributeBuffersBuilder.initEntries(attributes.size());
  // for (auto attribute_name : attributes) {
  for (uint64_t i = 0; i < attributes.size(); i++) {
    std::string attribute_name = attributes[i];
    // TODO: We have to skip special attrs, which include anonymous ones
    // because we can't call add_attribute() for a special attr name, we need
    // to figure out how to add these to a array schema nicely.
    if (attribute_name.substr(0, 2) ==
        tiledb::sm::constants::special_name_prefix) {
      continue;
    }
    const tiledb::sm::Attribute* attribute =
        this->array_schema()->attribute(attribute_name);
    if (attribute != nullptr) {
      Map<capnp::Text, ::AttributeBuffer>::Entry::Builder entryBuilder =
          buffers[i];
      ::AttributeBuffer::Builder attributeBuffer = entryBuilder.initValue();
      entryBuilder.setKey(attribute_name);
      attributeBuffer.setType(datatype_str(attribute->type()));
      switch (attribute->type()) {
        case tiledb::sm::Datatype::INT8: {
          auto buffer = this->buffer<int8_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setInt8(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::STRING_ASCII:
        case tiledb::sm::Datatype::STRING_UTF8:
        case tiledb::sm::Datatype::UINT8: {
          auto buffer = this->buffer<uint8_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setUint8(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          auto buffer = this->buffer<int16_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setInt16(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::STRING_UTF16:
        case tiledb::sm::Datatype::STRING_UCS2:
        case tiledb::sm::Datatype::UINT16: {
          auto buffer = this->buffer<uint16_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setUint16(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          auto buffer = this->buffer<int32_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setInt32(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::STRING_UTF32:
        case tiledb::sm::Datatype::STRING_UCS4:
        case tiledb::sm::Datatype::UINT32: {
          auto buffer = this->buffer<uint32_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setUint32(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          auto buffer = this->buffer<int64_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setInt64(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          auto buffer = this->buffer<uint64_t>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setUint64(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          auto buffer = this->buffer<float>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setFloat32(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
          auto buffer = this->buffer<double>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          attributeBuffer.initBuffer().setFloat64(
              kj::arrayPtr(buffer.second.first, buffer.second.second));
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::CHAR: {
          auto buffer = this->buffer<char>(attribute_name);
          // If buffer is null, skip
          if (buffer.second.first == nullptr || buffer.second.second == 0)
            break;

          const capnp::Data::Reader data(
              reinterpret_cast<kj::byte*>(buffer.second.first),
              buffer.second.second);
          attributeBuffer.initBuffer().setChar(data);
          if (buffer.first.first != nullptr)
            attributeBuffer.setBufferOffset(
                kj::arrayPtr(buffer.first.first, buffer.first.second));
          break;
        }
        case tiledb::sm::Datatype::ANY:
          // Not supported datatype for buffer
          return Status::Error("Any datatype not support for serialization");
          break;
      }
    }
  }

  if (this->layout() == tiledb::sm::Layout::GLOBAL_ORDER &&
      this->type() == tiledb::sm::QueryType::WRITE) {
    ::Writer::Builder writerBuilder = queryBuilder->initWriter();
    auto status = writer_.capnp(&writerBuilder);
    if (!status.ok())
      return status;
  } else if (this->type() == tiledb::sm::QueryType::READ) {
    ::QueryReader::Builder queryReaderBuilder = queryBuilder->initReader();
    auto status = reader_.capnp(&queryReaderBuilder);
    if (!status.ok())
      return status;
  }

  return Status::Ok();
  STATS_FUNC_OUT(serialization_query_capnp);
}

tiledb::sm::Status Query::from_capnp(::Query::Reader* query) {
  STATS_FUNC_IN(serialization_query_from_capnp);
  tiledb::sm::Status status;

  // QueryType to parse, set default to avoid compiler uninitialized warnings
  QueryType query_type = QueryType::READ;
  status = query_type_enum(query->getType().cStr(), &query_type);
  if (!status.ok())
    return status;
  if (query_type != this->type())
    return tiledb::sm::Status::QueryError(
        "Query opened for " + query_type_str(this->type()) +
        " but got serialized type for " + query->getType().cStr());
  this->type_ = query_type;

  // Layout to parse, set default to avoid compiler uninitialized warnings
  Layout layout = Layout::ROW_MAJOR;
  status = layout_enum(query->getLayout().cStr(), &layout);
  if (!status.ok())
    return status;
  this->set_layout(layout);

  // Load array details
  this->array_->from_capnp(query->getArray());

  // Override any existing writer or reader first
  // This prevents the set_subarray() from calling
  // writer.reset() which will delete the fragement metadata!!
  if (this->type() == QueryType::WRITE)
    this->writer_.reset_global_write_state();

  // If we are dense, unordered and writing, we can not set the subarray that
  // was serialized thats okay because it is just domain, if we set to nullptr
  // it will have same effect
  if (this->layout() == Layout::UNORDERED && this->type() == QueryType::WRITE &&
      this->array_schema()->dense()) {
    status = this->set_subarray(nullptr);
  } else {
    // Set subarray
    ::DomainArray::Reader subarrayReader = query->getSubarray();
    switch (array_schema()->domain()->type()) {
      case tiledb::sm::Datatype::INT8: {
        if (subarrayReader.hasInt8()) {
          auto subarrayCapnp = subarrayReader.getInt8();
          std::vector<int8_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::UINT8: {
        if (subarrayReader.hasUint8()) {
          auto subarrayCapnp = subarrayReader.getUint8();
          std::vector<uint8_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::INT16: {
        if (subarrayReader.hasInt16()) {
          auto subarrayCapnp = subarrayReader.getInt16();
          std::vector<int16_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::UINT16: {
        if (subarrayReader.hasUint16()) {
          auto subarrayCapnp = subarrayReader.getUint16();
          std::vector<uint16_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::INT32: {
        if (subarrayReader.hasInt32()) {
          auto subarrayCapnp = subarrayReader.getInt32();
          std::vector<int32_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::UINT32: {
        if (subarrayReader.hasUint32()) {
          auto subarrayCapnp = subarrayReader.getUint32();
          std::vector<uint32_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::INT64: {
        if (subarrayReader.hasInt64()) {
          auto subarrayCapnp = subarrayReader.getInt64();
          std::vector<int64_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::UINT64: {
        if (subarrayReader.hasUint64()) {
          auto subarrayCapnp = subarrayReader.getUint64();
          std::vector<uint64_t> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::FLOAT32: {
        if (subarrayReader.hasFloat32()) {
          auto subarrayCapnp = subarrayReader.getFloat32();
          std::vector<float> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::FLOAT64: {
        if (subarrayReader.hasFloat64()) {
          auto subarrayCapnp = subarrayReader.getFloat64();
          std::vector<double> subarray(subarrayCapnp.size());
          for (size_t i = 0; i < subarrayCapnp.size(); i++)
            subarray[i] = subarrayCapnp[i];
          status = this->set_subarray(subarray.data());
        }
        break;
      }
      case tiledb::sm::Datatype::CHAR:
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::ANY:
        // Not supported domain type
        return Status::Error("Unsupport domain type");
        break;
    }
  }

  if (!status.ok()) {
    LOG_STATUS(status);
    throw std::runtime_error(status.to_string());
  }

  if (query->hasBuffers()) {
    Map<capnp::Text, ::AttributeBuffer>::Reader buffers = query->getBuffers();
    for (auto bufferMap : buffers.getEntries()) {
      // empty key name means this object is empty, we should skip it.
      // Need to handle serialization better and no make empty objects
      if (std::string(bufferMap.getKey().cStr()).empty())
        continue;

      // TODO: We have to skip special attrs, which include anonymous ones
      // because we can't call add_attribute() for a special attr name, we
      // need to figure out how to add these to a array schema nicely.
      if (std::string(bufferMap.getKey().cStr()).substr(0, 2) ==
          tiledb::sm::constants::special_name_prefix) {
        continue;
      }
      const tiledb::sm::Attribute* attr =
          array_schema()->attribute(bufferMap.getKey().cStr());
      if (attr == nullptr) {
        return Status::Error(
            "Attribute " + std::string(bufferMap.getKey().cStr()) +
            " is null in query from_capnp");
      }
      // check to see if the query already has said buffer
      uint64_t* existingBufferOffset = nullptr;
      uint64_t* existingBufferOffsetSize = nullptr;
      void* existingBuffer = nullptr;
      uint64_t* existingBufferSize = nullptr;
      if (attr->var_size()) {
        this->get_buffer(
            bufferMap.getKey().cStr(),
            &existingBufferOffset,
            &existingBufferOffsetSize,
            &existingBuffer,
            &existingBufferSize);
      } else {
        this->get_buffer(
            bufferMap.getKey().cStr(), &existingBuffer, &existingBufferSize);
      }

      ::AttributeBuffer::Reader buffer = bufferMap.getValue();
      uint64_t type_size = tiledb::sm::datatype_size(attr->type());
      Datatype buffer_datatype = Datatype::ANY;
      status = datatype_enum(buffer.getType().cStr(), &buffer_datatype);
      if (!status.ok())
        return status;

      if (attr->type() != buffer_datatype)
        return Status::Error(
            "Attribute from array_schema and buffer do not have same "
            "datatype. " +
            datatype_str(attr->type()) + " != " + buffer.getType().cStr());

      ::AttributeBuffer::Buffer::Reader bufferReader = buffer.getBuffer();

      switch (attr->type()) {
        case tiledb::sm::Datatype::INT8: {
          if (bufferReader.hasInt8()) {
            auto newBuffer = bufferReader.getInt8();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              int8_t* existing_buffer_casted =
                  static_cast<int8_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<int8_t>* new_buffer =
                  new std::vector<int8_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::STRING_ASCII:
        case tiledb::sm::Datatype::STRING_UTF8:
        case tiledb::sm::Datatype::UINT8: {
          if (bufferReader.hasUint8()) {
            auto newBuffer = bufferReader.getUint8();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              uint8_t* existing_buffer_casted =
                  static_cast<uint8_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<uint8_t>* new_buffer =
                  new std::vector<uint8_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT16: {
          if (bufferReader.hasInt16()) {
            auto newBuffer = bufferReader.getInt16();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              int16_t* existing_buffer_casted =
                  static_cast<int16_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<int16_t>* new_buffer =
                  new std::vector<int16_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::STRING_UTF16:
        case tiledb::sm::Datatype::STRING_UCS2:
        case tiledb::sm::Datatype::UINT16: {
          if (bufferReader.hasUint16()) {
            auto newBuffer = bufferReader.getUint16();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              uint16_t* existing_buffer_casted =
                  static_cast<uint16_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<uint16_t>* new_buffer =
                  new std::vector<uint16_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT32: {
          if (bufferReader.hasInt32()) {
            auto newBuffer = bufferReader.getInt32();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              int32_t* existing_buffer_casted =
                  static_cast<int32_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<int32_t>* new_buffer =
                  new std::vector<int32_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::STRING_UTF32:
        case tiledb::sm::Datatype::STRING_UCS4:
        case tiledb::sm::Datatype::UINT32: {
          if (bufferReader.hasUint32()) {
            auto newBuffer = bufferReader.getUint32();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              uint32_t* existing_buffer_casted =
                  static_cast<uint32_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<uint32_t>* new_buffer =
                  new std::vector<uint32_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::INT64: {
          if (bufferReader.hasInt64()) {
            auto newBuffer = bufferReader.getInt64();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              int64_t* existing_buffer_casted =
                  static_cast<int64_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<int64_t>* new_buffer =
                  new std::vector<int64_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::UINT64: {
          if (bufferReader.hasUint64()) {
            auto newBuffer = bufferReader.getUint64();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              uint64_t* existing_buffer_casted =
                  static_cast<uint64_t*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<uint64_t>* new_buffer =
                  new std::vector<uint64_t>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::FLOAT32: {
          if (bufferReader.hasFloat32()) {
            auto newBuffer = bufferReader.getFloat32();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              float* existing_buffer_casted =
                  static_cast<float*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<float>* new_buffer =
                  new std::vector<float>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::FLOAT64: {
          if (bufferReader.hasFloat64()) {
            auto newBuffer = bufferReader.getFloat64();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if (*existingBufferSize / type_size != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              double* existing_buffer_casted =
                  static_cast<double*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<double>* new_buffer =
                  new std::vector<double>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::CHAR: {
          if (bufferReader.hasChar()) {
            auto newBuffer = bufferReader.getChar();
            uint buffer_length = newBuffer.size();
            if (existingBuffer != nullptr) {
              // If buffer sizes are different error
              if ((*existingBufferSize / type_size) != buffer_length) {
                return Status::QueryError(
                    "Existing buffer in query object is different size (" +
                    std::to_string(*existingBufferSize) +
                    ") vs new query object buffer size (" +
                    std::to_string(buffer_length) + ")");
              }
              char* existing_buffer_casted = static_cast<char*>(existingBuffer);
              for (uint i = 0; i < buffer_length; i++) {
                existing_buffer_casted[i] = newBuffer[i];
              }

              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                if (*existingBufferOffsetSize / sizeof(uint64_t) !=
                    buffer_offset.size()) {
                  return Status::QueryError(
                      "Existing buffer_var_ in query object is different size "
                      "(" +
                      std::to_string(*existingBufferOffsetSize) +
                      ") vs new query object buffer_var size (" +
                      std::to_string(buffer_offset.size()) + ")");
                }
                uint64_t* existing_buffer_var_caster =
                    static_cast<uint64_t*>(existingBufferOffset);
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  existing_buffer_var_caster[i] = buffer_offset[i];
                }
              }

            } else {
              // Get buffer
              std::vector<char>* new_buffer =
                  new std::vector<char>(buffer_length);
              for (uint i = 0; i < buffer_length; i++) {
                (*new_buffer)[i] = newBuffer[i];
              }
              uint64_t* buffer_size = new uint64_t(buffer_length * type_size);

              // Check for offset buffer
              if (buffer.hasBufferOffset()) {
                auto buffer_offset = buffer.getBufferOffset();
                std::vector<uint64_t>* new_buffer_offset =
                    new std::vector<uint64_t>(buffer_offset.size());
                for (uint i = 0; i < buffer_offset.size(); i++) {
                  (*new_buffer_offset)[i] = buffer_offset[i];
                }

                uint64_t* new_buffer_offset_size =
                    new uint64_t(new_buffer_offset->size() * sizeof(uint64_t));
                set_buffer(
                    attr->name(),
                    new_buffer_offset->data(),
                    new_buffer_offset_size,
                    new_buffer->data(),
                    buffer_size);
              } else
                set_buffer(attr->name(), new_buffer->data(), buffer_size);
            }
          }
          break;
        }
        case tiledb::sm::Datatype::ANY: {
          // Not supported datatype for buffer
          return Status::Error(
              "Any datatype not supported for deserialization");
          break;
        }
      }
    }
  }

  // This has to come after set_subarray because they remove global write state
  if (this->type() == tiledb::sm::QueryType::WRITE && query->hasWriter()) {
    ::Writer::Reader writerReader = query->getWriter();
    status = writer_.from_capnp(&writerReader);
  } else if (
      this->type() == tiledb::sm::QueryType::READ && query->hasReader()) {
    ::QueryReader::Reader queryReader = query->getReader();
    status = reader_.from_capnp(&queryReader);
  }
  if (!status.ok())
    return status;

  // This must come last because set_subarray overrides query status
  QueryStatus query_status = QueryStatus::UNINITIALIZED;
  status = query_status_enum(query->getStatus().cStr(), &query_status);
  if (!status.ok())
    return status;
  this->set_status(query_status);

  return status;
  STATS_FUNC_OUT(serialization_query_from_capnp);
}

Status Query::process() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return LOG_STATUS(
        Status::QueryError("Cannot process query; Query is not initialized"));
  status_ = QueryStatus::INPROGRESS;

  // Process query
  Status st = Status::Ok();
  if (type_ == QueryType::READ)
    st = reader_.read();
  else  // WRITE MODE
    st = writer_.write();

  // Handle error
  if (!st.ok()) {
    status_ = QueryStatus::FAILED;
    return st;
  }

  // Check if the query is complete
  bool completed = (type_ == QueryType::WRITE) ? true : !reader_.incomplete();

  // Handle callback and status
  if (completed) {
    if (callback_ != nullptr)
      callback_(callback_data_);
    status_ = QueryStatus::COMPLETED;
  } else {  // Incomplete
    status_ = QueryStatus::INCOMPLETE;
  }

  return Status::Ok();
}

Status Query::set_buffer(
    const std::string& attribute, void* buffer, uint64_t* buffer_size) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(attribute, buffer, buffer_size);
  return reader_.set_buffer(attribute, buffer, buffer_size);
}

Status Query::set_buffer(
    const std::string& attribute,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(
        attribute, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.set_buffer(
      attribute, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
}

Status Query::set_layout(Layout layout) {
  layout_ = layout;
  if (type_ == QueryType::WRITE)
    return writer_.set_layout(layout);
  return reader_.set_layout(layout);
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

Status Query::set_subarray(const void* subarray) {
  RETURN_NOT_OK(check_subarray_bounds(subarray));
  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(subarray));
  } else {  // READ
    RETURN_NOT_OK(reader_.set_subarray(subarray));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::submit() {  // Do nothing if the query is completed or failed
  RETURN_NOT_OK(init());
  return storage_manager_->query_submit(this);
}

Status Query::submit_async(
    std::function<void(void*)> callback, void* callback_data) {
  RETURN_NOT_OK(init());
  callback_ = callback;
  callback_data_ = callback_data;
  return storage_manager_->query_submit_async(this);
}

QueryStatus Query::status() const {
  return status_;
}

QueryType Query::type() const {
  return type_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Status Query::check_subarray_bounds(const void* subarray) const {
  if (subarray == nullptr)
    return Status::Ok();

  auto array_schema = this->array_schema();
  if (array_schema == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot check subarray; Array schema not set"));

  switch (array_schema->domain()->type()) {
    case Datatype::INT8:
      return check_subarray_bounds<int8_t>(
          static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return check_subarray_bounds<uint8_t>(
          static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return check_subarray_bounds<int16_t>(
          static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return check_subarray_bounds<uint16_t>(
          static_cast<const uint16_t*>(subarray));
    case Datatype::INT32:
      return check_subarray_bounds<int32_t>(
          static_cast<const int32_t*>(subarray));
    case Datatype::UINT32:
      return check_subarray_bounds<uint32_t>(
          static_cast<const uint32_t*>(subarray));
    case Datatype::INT64:
      return check_subarray_bounds<int64_t>(
          static_cast<const int64_t*>(subarray));
    case Datatype::UINT64:
      return check_subarray_bounds<uint64_t>(
          static_cast<const uint64_t*>(subarray));
    case Datatype::FLOAT32:
      return check_subarray_bounds<float>(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return check_subarray_bounds<double>(
          static_cast<const double*>(subarray));
    case Datatype::CHAR:
    case Datatype::STRING_ASCII:
    case Datatype::STRING_UTF8:
    case Datatype::STRING_UTF16:
    case Datatype::STRING_UTF32:
    case Datatype::STRING_UCS2:
    case Datatype::STRING_UCS4:
    case Datatype::ANY:
      // Not supported domain type
      assert(false);
      break;
  }

  return Status::Ok();
}

template <class T>
Status Query::check_subarray_bounds(const T* subarray) const {
  // Check subarray bounds
  auto array_schema = this->array_schema();
  auto domain = array_schema->domain();
  auto dim_num = domain->dim_num();
  for (unsigned int i = 0; i < dim_num; ++i) {
    auto dim_domain = static_cast<const T*>(domain->dimension(i)->domain());
    if (subarray[2 * i] < dim_domain[0] || subarray[2 * i + 1] > dim_domain[1])
      return LOG_STATUS(Status::QueryError("Subarray out of bounds"));
    if (subarray[2 * i] > subarray[2 * i + 1])
      return LOG_STATUS(Status::QueryError(
          "Subarray lower bound is larger than upper bound"));
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
