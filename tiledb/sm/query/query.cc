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
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/rest/rest_client.h"

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

Status Query::add_range(
    unsigned dim_idx, const void* start, const void* end, const void* stride) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Operation currently unsupported for write queries"));
  return reader_.add_range(dim_idx, start, end, stride);
}

Status Query::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(
        Status::QueryError("Cannot get number of ranges; Operation currently "
                           "unsupported for write queries"));
  return reader_.get_range_num(dim_idx, range_num);
}

Status Query::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get range; Operation currently unsupported for write queries"));
  return reader_.get_range(dim_idx, range_idx, start, end, stride);
}

Status Query::get_est_result_size(const char* attr_name, uint64_t* size) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(
        Status::QueryError("Cannot estimated result size; Operation currently "
                           "unsupported for write queries"));
  return reader_.get_est_result_size(attr_name, size);
}

Status Query::get_est_result_size(
    const char* attr_name, uint64_t* size_off, uint64_t* size_val) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(
        Status::QueryError("Cannot estimated result size; Operation currently "
                           "unsupported for write queries"));
  return reader_.get_est_result_size(attr_name, size_off, size_val);
}

Status Query::get_written_fragment_num(uint32_t* num) const {
  if (type_ != QueryType::WRITE)
    return LOG_STATUS(Status::WriterError(
        "Cannot get number of fragments; Applicable only to WRITE mode"));

  *num = (uint32_t)writer_.written_fragment_info().size();

  return Status::Ok();
}

Status Query::get_written_fragment_uri(uint32_t idx, const char** uri) const {
  if (type_ != QueryType::WRITE)
    return LOG_STATUS(Status::WriterError(
        "Cannot get fragment URI; Applicable only to WRITE mode"));

  auto& written_fragment_info = writer_.written_fragment_info();
  auto num = (uint32_t)written_fragment_info.size();
  if (idx >= num)
    return LOG_STATUS(
        Status::WriterError("Cannot get fragment URI; Invalid fragment index"));

  *uri = written_fragment_info[idx].uri_.c_str();

  return Status::Ok();
}

Status Query::get_written_fragment_timestamp_range(
    uint32_t idx, uint64_t* t1, uint64_t* t2) const {
  if (type_ != QueryType::WRITE)
    return LOG_STATUS(Status::WriterError(
        "Cannot get fragment timestamp range; Applicable only to WRITE mode"));

  auto& written_fragment_info = writer_.written_fragment_info();
  auto num = (uint32_t)written_fragment_info.size();
  if (idx >= num)
    return LOG_STATUS(Status::WriterError(
        "Cannot get fragment timestamp range; Invalid fragment index"));

  *t1 = written_fragment_info[idx].timestamp_range_.first;
  *t2 = written_fragment_info[idx].timestamp_range_.second;

  return Status::Ok();
}

const Array* Query::array() const {
  return array_;
}

Array* Query::array() {
  return array_;
}

const ArraySchema* Query::array_schema() const {
  if (type_ == QueryType::WRITE)
    return writer_.array_schema();
  return reader_.array_schema();
}

std::vector<std::string> Query::buffer_names() const {
  if (type_ == QueryType::WRITE)
    return writer_.buffer_names();
  return reader_.attributes();  // TODO: this will change in a subsequent PR
}

QueryBuffer Query::buffer(const std::string& name) const {
  if (type_ == QueryType::WRITE)
    return writer_.buffer(name);
  return reader_.buffer(name);
}

Status Query::finalize() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return Status::Ok();

  if (array_->is_remote()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::QueryError(
          "Error in query finalize; remote array with no rest client."));

    array_->array_schema()->set_array_uri(array_->array_uri());

    return rest_client->finalize_query_to_rest(array_->array_uri(), this);
  }

  RETURN_NOT_OK(writer_.finalize());
  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
}

// TODO: fix normalized for coords
Status Query::get_buffer(
    const char* name, void** buffer, uint64_t** buffer_size) const {
  // Normalize attribute
  std::string normalized;
  RETURN_NOT_OK(ArraySchema::attribute_name_normalized(name, &normalized));

  // Check attribute
  auto array_schema = this->array_schema();
  if (normalized != constants::coords) {
    if (array_schema->attribute(normalized) == nullptr &&
        array_schema->dimension(normalized) == nullptr)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot get buffer; Invalid attribute/dimension name '") +
          normalized + "'"));
  }
  if (array_schema->var_size(normalized))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + normalized + "' is var-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(normalized, buffer, buffer_size);
  return reader_.get_buffer(normalized, buffer, buffer_size);
}

// TODO: fix normalized for coords
Status Query::get_buffer(
    const char* name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  // Normalize attribute
  std::string normalized;
  RETURN_NOT_OK(ArraySchema::attribute_name_normalized(name, &normalized));

  // Check attribute
  auto array_schema = this->array_schema();
  if (normalized == constants::coords) {
    return LOG_STATUS(
        Status::QueryError("Cannot get buffer; Coordinates are not var-sized"));
  }
  if (array_schema->attribute(normalized) == nullptr &&
      array_schema->dimension(normalized) == nullptr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Invalid attribute/dimension name '") +
        normalized + "'"));
  if (!array_schema->var_size(normalized))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + normalized + "' is fixed-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(
        normalized, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.get_buffer(
      normalized, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
}

Status Query::get_attr_serialization_state(
    const std::string& attribute, SerializationState::AttrState** state) {
  *state = &serialization_state_.attribute_states[attribute];
  return Status::Ok();
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

URI Query::first_fragment_uri() const {
  if (type_ == QueryType::WRITE)
    return URI();
  return reader_.first_fragment_uri();
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
  std::cerr << "JOE Query::process 1" << std::endl;

  if (status_ == QueryStatus::UNINITIALIZED)
    return LOG_STATUS(
        Status::QueryError("Cannot process query; Query is not initialized"));
  status_ = QueryStatus::INPROGRESS;

  // Process query
  Status st = Status::Ok();
  if (type_ == QueryType::READ) {
    std::cerr << "JOE Query::process 2" << std::endl;
    st = reader_.read();
  }
  else { // WRITE MODE
    std::cerr << "JOE Query::process 3" << std::endl;
    st = writer_.write();
  }

  std::cerr << "JOE Query::process 4" << std::endl;

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

const Reader* Query::reader() const {
  return &reader_;
}

Reader* Query::reader() {
  return &reader_;
}

const Writer* Query::writer() const {
  return &writer_;
}

Writer* Query::writer() {
  return &writer_;
}

Status Query::set_buffer(
    const std::string& attribute,
    void* buffer,
    uint64_t* buffer_size,
    bool check_null_buffers) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(attribute, buffer, buffer_size);
  return reader_.set_buffer(attribute, buffer, buffer_size, check_null_buffers);
}

Status Query::set_buffer(
    const std::string& attribute,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size,
    bool check_null_buffers) {

  std::cerr << "JOE Query::set_buffer 1" << std::endl;

  if (type_ == QueryType::WRITE) {
    std::cerr << "JOE Query::set_buffer 2" << std::endl;
    return writer_.set_buffer(
        attribute, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  }
  std::cerr << "JOE Query::set_buffer 3" << std::endl;
  return reader_.set_buffer(
      attribute,
      buffer_off,
      buffer_off_size,
      buffer_val,
      buffer_val_size,
      check_null_buffers);
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

Status Query::set_subarray(const void* subarray, bool check_expanded_domain) {
  RETURN_NOT_OK(check_subarray(subarray, check_expanded_domain));
  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(subarray));
  } else {  // READ
    RETURN_NOT_OK(reader_.set_subarray(subarray, check_expanded_domain));
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

Status Query::submit() {
  std::cerr << "JOE Query::submit 1" << std::endl;

  // Do not resubmit completed reads.
  if (type_ == QueryType::READ && status_ == QueryStatus::COMPLETED) {
    return Status::Ok();
  }
  if (array_->is_remote()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::QueryError(
          "Error in query submission; remote array with no rest client."));

    array_->array_schema()->set_array_uri(array_->array_uri());

    return rest_client->submit_query_to_rest(array_->array_uri(), this);
  }
  RETURN_NOT_OK(init());
  std::cerr << "JOE Query::submit 2" << std::endl;
  return storage_manager_->query_submit(this);
}

Status Query::submit_async(
    std::function<void(void*)> callback, void* callback_data) {
  // Do not resubmit completed reads.
  if (type_ == QueryType::READ && status_ == QueryStatus::COMPLETED) {
    callback(callback_data);
    return Status::Ok();
  }
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

Status Query::check_subarray(
    const void* subarray, bool check_expanded_domain) const {
  if (subarray == nullptr)
    return Status::Ok();

  auto array_schema = this->array_schema();
  if (array_schema == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot check subarray; Array schema not set"));

  switch (array_schema->domain()->type()) {
    case Datatype::INT8:
      return check_subarray<int8_t>(
          static_cast<const int8_t*>(subarray), check_expanded_domain);
    case Datatype::UINT8:
      return check_subarray<uint8_t>(
          static_cast<const uint8_t*>(subarray), check_expanded_domain);
    case Datatype::INT16:
      return check_subarray<int16_t>(
          static_cast<const int16_t*>(subarray), check_expanded_domain);
    case Datatype::UINT16:
      return check_subarray<uint16_t>(
          static_cast<const uint16_t*>(subarray), check_expanded_domain);
    case Datatype::INT32:
      return check_subarray<int32_t>(
          static_cast<const int32_t*>(subarray), check_expanded_domain);
    case Datatype::UINT32:
      return check_subarray<uint32_t>(
          static_cast<const uint32_t*>(subarray), check_expanded_domain);
    case Datatype::INT64:
      return check_subarray<int64_t>(
          static_cast<const int64_t*>(subarray), check_expanded_domain);
    case Datatype::UINT64:
      return check_subarray<uint64_t>(
          static_cast<const uint64_t*>(subarray), check_expanded_domain);
    case Datatype::FLOAT32:
      return check_subarray<float>(
          static_cast<const float*>(subarray), check_expanded_domain);
    case Datatype::FLOAT64:
      return check_subarray<double>(
          static_cast<const double*>(subarray), check_expanded_domain);
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
      return check_subarray<int64_t>(
          static_cast<const int64_t*>(subarray), check_expanded_domain);
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
