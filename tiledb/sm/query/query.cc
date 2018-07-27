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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/misc/logger.h"

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
