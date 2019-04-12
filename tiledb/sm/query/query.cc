/**
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
#include "tiledb/rest/capnp/utils.h"
#include "tiledb/rest/curl/client.h"
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

  if (array_->is_remote()) {
    array_->array_schema()->set_array_uri(array_->array_uri());
    Config config = this->storage_manager_->config();
    return tiledb::rest::finalize_query_to_rest(
        &config,
        array_->get_rest_server(),
        array_->array_uri().to_string(),
        array_->get_serialization_type(),
        this);
  }

  RETURN_NOT_OK(writer_.finalize());
  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
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

Status Query::capnp(rest::capnp::Query::Builder* queryBuilder) {
  STATS_FUNC_IN(serialization_query_capnp);

  if (layout_ == Layout::GLOBAL_ORDER)
    return LOG_STATUS(Status::QueryError(
        "Cannot serialize; global order serialization not supported."));

  queryBuilder->setType(query_type_str(this->type()));
  queryBuilder->setLayout(layout_str(this->layout()));
  queryBuilder->setStatus(query_status_str(this->status()));

  if (this->array_ != nullptr) {
    rest::capnp::Array::Builder arrayBuilder = queryBuilder->initArray();
    this->array_->capnp(&arrayBuilder);
  }

  // Serialize subarray
  rest::capnp::DomainArray::Builder subarrayBuilder =
      queryBuilder->initSubarray();
  if (this->array_schema() != nullptr) {
    const auto* schema = array_schema();
    if (schema->domain() == nullptr)
      return tiledb::sm::Status::Error(
          "Domain was null from array schema in query::capnp()");

    const auto domain_type = schema->domain()->type();
    const void* subarray =
        type_ == QueryType::READ ? reader_.subarray() : writer_.subarray();
    const uint64_t subarray_size = 2 * schema->coords_size();
    const uint64_t subarray_length =
        subarray_size / datatype_size(schema->coords_type());

    switch (domain_type) {
      case tiledb::sm::Datatype::CHAR:
      case tiledb::sm::Datatype::STRING_ASCII:
      case tiledb::sm::Datatype::STRING_UTF8:
      case tiledb::sm::Datatype::STRING_UTF16:
      case tiledb::sm::Datatype::STRING_UTF32:
      case tiledb::sm::Datatype::STRING_UCS2:
      case tiledb::sm::Datatype::STRING_UCS4:
      case tiledb::sm::Datatype::ANY:
        // String dimensions not yet supported
        return Status::RestError(
            "Cannot serialize query subarray; unsupported domain type");
      default:
        if (subarray != nullptr)
          RETURN_NOT_OK(rest::capnp::utils::set_capnp_array_ptr(
              subarrayBuilder, domain_type, subarray, subarray_length));
        break;
    }
  }

  std::vector<std::string> attributes = this->attributes();

  ::capnp::List<::tiledb::rest::capnp::AttributeBufferHeader>::Builder
      attributeBufferHeaderBuilder =
          queryBuilder->initBuffers(attributes.size());

  uint64_t totalNumOfBytesInBuffers = 0;
  uint64_t totalNumOfBytesInOffsets = 0;
  uint64_t totalNumOfBytesInVarBuffers = 0;
  // for (auto attribute_name : attributes) {
  for (uint64_t i = 0; i < attributes.size(); i++) {
    std::string attribute_name = attributes[i];
    ::tiledb::rest::capnp::AttributeBufferHeader::Builder
        attributeBufferHeader = attributeBufferHeaderBuilder[i];
    const tiledb::sm::Attribute* attribute =
        this->array_schema()->attribute(attribute_name);
    if (attribute != nullptr) {
      attributeBufferHeader.setAttributeName(attributes[i]);
      attributeBufferHeader.setType(datatype_str(attribute->type()));

      auto attr_type = attribute->type();
      auto buff = type_ == QueryType::READ ? reader_.buffer(attribute_name) :
                                             writer_.buffer(attribute_name);

      if (attribute->var_size() &&
          (buff.buffer_var_ != nullptr && buff.buffer_var_size_ != nullptr)) {
        // Variable-sized attribute.
        if (buff.buffer_ == nullptr || buff.buffer_size_ == nullptr)
          return Status::RestError(
              "Cannot serialize query; variable-length attribute has no offset "
              "buffer set.");
        totalNumOfBytesInVarBuffers += *buff.buffer_var_size_;
        attributeBufferHeader.setBufferSizeInBytes(*buff.buffer_var_size_);
        attributeBufferHeader.setDatatypeSizeInBytes(datatype_size(attr_type));
        totalNumOfBytesInOffsets += *buff.buffer_size_;
        attributeBufferHeader.setOffsetBufferSizeInBytes(*buff.buffer_size_);
        RETURN_NOT_OK(set_buffer(
            attributes[i],
            static_cast<uint64_t*>(buff.buffer_),
            buff.buffer_size_,
            buff.buffer_var_,
            buff.buffer_var_size_))
      } else if (buff.buffer_ != nullptr && buff.buffer_size_ != nullptr) {
        totalNumOfBytesInBuffers += *buff.buffer_size_;
        attributeBufferHeader.setBufferSizeInBytes(*buff.buffer_size_);
        attributeBufferHeader.setDatatypeSizeInBytes(datatype_size(attr_type));
        attributeBufferHeader.setOffsetBufferSizeInBytes(0);
        RETURN_NOT_OK(
            set_buffer(attributes[i], buff.buffer_, buff.buffer_size_))
      }
    }
  }

  queryBuilder->setTotalNumOfBytesInBuffers(totalNumOfBytesInBuffers);
  queryBuilder->setTotalNumOfBytesInOffsets(totalNumOfBytesInOffsets);
  queryBuilder->setTotalNumOfBytesInVarBuffers(totalNumOfBytesInVarBuffers);

  if (this->layout() == tiledb::sm::Layout::GLOBAL_ORDER &&
      this->type() == tiledb::sm::QueryType::WRITE) {
    rest::capnp::Writer::Builder writerBuilder = queryBuilder->initWriter();
    auto status = writer_.capnp(&writerBuilder);
    if (!status.ok())
      return status;
  } else if (this->type() == tiledb::sm::QueryType::READ) {
    rest::capnp::QueryReader::Builder queryReaderBuilder =
        queryBuilder->initReader();
    auto status = reader_.capnp(&queryReaderBuilder);
    if (!status.ok())
      return status;
  }

  return Status::Ok();
  STATS_FUNC_OUT(serialization_query_capnp);
}

tiledb::sm::Status Query::from_capnp(
    rest::capnp::Query::Reader* query, void* buffer_start) {
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
    rest::capnp::DomainArray::Reader subarrayReader = query->getSubarray();
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
    ::capnp::List<rest::capnp::AttributeBufferHeader>::Reader buffers =
        query->getBuffers();

    auto attribute_buffer_start = static_cast<char*>(buffer_start);
    for (rest::capnp::AttributeBufferHeader::Reader buffer : buffers) {
      auto attributeName = buffer.getAttributeName().cStr();
      if (std::string(attributeName).empty()) {
        continue;
      }

      auto offset_buffer_size_in_bytes = new uint64_t;
      *offset_buffer_size_in_bytes = buffer.getOffsetBufferSizeInBytes();
      auto buffer_size_in_bytes = new uint64_t;
      *buffer_size_in_bytes = buffer.getBufferSizeInBytes();

      // check to see if the query already has said buffer
      uint64_t* existingBufferOffset = nullptr;
      uint64_t* existingBufferOffsetSize = nullptr;
      void* existingBuffer = nullptr;
      uint64_t* existingBufferSize = nullptr;
      if (*offset_buffer_size_in_bytes) {
        this->get_buffer(
            attributeName,
            &existingBufferOffset,
            &existingBufferOffsetSize,
            &existingBuffer,
            &existingBufferSize);
      } else {
        this->get_buffer(attributeName, &existingBuffer, &existingBufferSize);
      }

      const tiledb::sm::Attribute* attr =
          array_schema()->attribute(attributeName);
      if (attr == nullptr) {
        return Status::Error(
            "Attribute " + std::string(attributeName) +
            " is null in query from_capnp");
      }

      Datatype buffer_datatype = Datatype::ANY;
      status = datatype_enum(buffer.getType().cStr(), &buffer_datatype);
      if (!status.ok())
        return status;

      if (attr->type() != buffer_datatype)
        return Status::Error(
            "Attribute from array_schema and buffer do not have same "
            "datatype. " +
            datatype_str(attr->type()) + " != " + buffer.getType().cStr());

      if (existingBuffer != nullptr) {
        if (*existingBufferSize < *buffer_size_in_bytes) {
          return Status::QueryError(
              "Existing buffer in query object is smaller size (" +
              std::to_string(*existingBufferSize) +
              ") vs new query object buffer size (" +
              std::to_string(*buffer_size_in_bytes) + ")");
        }
        if (*offset_buffer_size_in_bytes) {
          if (*existingBufferOffsetSize < *offset_buffer_size_in_bytes) {
            return Status::QueryError(
                "Existing buffer_var_ in query object is smaller size "
                "(" +
                std::to_string(*existingBufferOffsetSize) +
                ") vs new query object buffer_var size (" +
                std::to_string(*offset_buffer_size_in_bytes) + ")");
          }
          memcpy(
              existingBufferOffset,
              attribute_buffer_start,
              *offset_buffer_size_in_bytes);
          attribute_buffer_start += *offset_buffer_size_in_bytes;
        }
        memcpy(existingBuffer, attribute_buffer_start, *buffer_size_in_bytes);
        attribute_buffer_start += *buffer_size_in_bytes;
      } else {
        if (*offset_buffer_size_in_bytes) {
          // variable size attribute buffer
          set_buffer(
              attributeName,
              reinterpret_cast<uint64_t*>(attribute_buffer_start),
              offset_buffer_size_in_bytes,
              attribute_buffer_start + *offset_buffer_size_in_bytes,
              buffer_size_in_bytes);
          attribute_buffer_start +=
              *offset_buffer_size_in_bytes + *buffer_size_in_bytes;
        } else {
          // fixed size attribute buffer
          set_buffer(
              attributeName, attribute_buffer_start, buffer_size_in_bytes);
          attribute_buffer_start += *buffer_size_in_bytes;
        }
      }
    }
  }

  // This has to come after set_subarray because they remove global write state
  if (this->type() == tiledb::sm::QueryType::WRITE && query->hasWriter()) {
    rest::capnp::Writer::Reader writerReader = query->getWriter();
    status = writer_.from_capnp(&writerReader);
  } else if (
      this->type() == tiledb::sm::QueryType::READ && query->hasReader()) {
    rest::capnp::QueryReader::Reader queryReader = query->getReader();
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

Status Query::set_sparse_mode(bool sparse_mode) {
  if (type_ != QueryType::READ)
    return LOG_STATUS(Status::QueryError(
        "Cannot set sparse mode; Only applicable to read queries"));

  return reader_.set_sparse_mode(sparse_mode);
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

Status Query::set_subarray(const void* subarray) {
  RETURN_NOT_OK(check_subarray(subarray));
  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(subarray));
  } else {  // READ
    RETURN_NOT_OK(reader_.set_subarray(subarray));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::set_subarray(const Subarray& subarray) {
  // Check that the subarray is associated with the same array as the query
  if (subarray.array() != array_)
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray; The array of subarray is "
                           "different from that of the query"));

  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(subarray));
  } else {  // READ
    RETURN_NOT_OK(reader_.set_subarray(subarray));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::submit() {  // Do nothing if the query is completed or failed
  if (array_->is_remote()) {
    array_->array_schema()->set_array_uri(array_->array_uri());
    Config config = this->storage_manager_->config();
    return tiledb::rest::submit_query_to_rest(
        &config,
        array_->get_rest_server(),
        array_->array_uri().to_string(),
        array_->get_serialization_type(),
        this);
  }
  RETURN_NOT_OK(init());
  return storage_manager_->query_submit(this);
}

Status Query::submit_async(
    std::function<void(void*)> callback, void* callback_data) {
  RETURN_NOT_OK(init());
  if (array_->is_remote())
    return LOG_STATUS(
        Status::QueryError("Error in async query submission; async queries not "
                           "supported for remote arrays."));

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

Status Query::check_subarray(const void* subarray) const {
  if (subarray == nullptr)
    return Status::Ok();

  auto array_schema = this->array_schema();
  if (array_schema == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot check subarray; Array schema not set"));

  switch (array_schema->domain()->type()) {
    case Datatype::INT8:
      return check_subarray<int8_t>(static_cast<const int8_t*>(subarray));
    case Datatype::UINT8:
      return check_subarray<uint8_t>(static_cast<const uint8_t*>(subarray));
    case Datatype::INT16:
      return check_subarray<int16_t>(static_cast<const int16_t*>(subarray));
    case Datatype::UINT16:
      return check_subarray<uint16_t>(static_cast<const uint16_t*>(subarray));
    case Datatype::INT32:
      return check_subarray<int32_t>(static_cast<const int32_t*>(subarray));
    case Datatype::UINT32:
      return check_subarray<uint32_t>(static_cast<const uint32_t*>(subarray));
    case Datatype::INT64:
      return check_subarray<int64_t>(static_cast<const int64_t*>(subarray));
    case Datatype::UINT64:
      return check_subarray<uint64_t>(static_cast<const uint64_t*>(subarray));
    case Datatype::FLOAT32:
      return check_subarray<float>(static_cast<const float*>(subarray));
    case Datatype::FLOAT64:
      return check_subarray<double>(static_cast<const double*>(subarray));
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

}  // namespace sm
}  // namespace tiledb
