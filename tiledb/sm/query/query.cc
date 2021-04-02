/**
 * @file   query.cc
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
 * This file implements class Query.
 */

#include "tiledb/sm/query/query.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <cassert>
#include <iostream>
#include <sstream>

using namespace tiledb::common;

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
  if (dim_idx >= array_->array_schema()->dim_num())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Invalid dimension index"));

  if (start == nullptr || end == nullptr)
    return LOG_STATUS(Status::QueryError("Cannot add range; Invalid range"));

  if (stride != nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Setting range stride is currently unsupported"));

  if (array_->array_schema()->domain()->dimension(dim_idx)->var_size())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Range must be fixed-sized"));

  // Prepare a temp range
  std::vector<uint8_t> range;
  uint8_t coord_size = array_->array_schema()->dimension(dim_idx)->coord_size();
  range.resize(2 * coord_size);
  std::memcpy(&range[0], start, coord_size);
  std::memcpy(&range[coord_size], end, coord_size);

  // Add range
  if (type_ == QueryType::WRITE)
    return writer_.add_range(dim_idx, Range(&range[0], 2 * coord_size));
  return reader_.add_range(dim_idx, Range(&range[0], 2 * coord_size));
}

Status Query::add_range_var(
    unsigned dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (dim_idx >= array_->array_schema()->dim_num())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Invalid dimension index"));

  if (start == nullptr || end == nullptr)
    return LOG_STATUS(Status::QueryError("Cannot add range; Invalid range"));

  if (start_size == 0 || end_size == 0)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Range start/end cannot have zero length"));

  if (!array_->array_schema()->domain()->dimension(dim_idx)->var_size())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Range must be variable-sized"));

  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Function applicable only to reads"));

  // Add range
  Range r;
  r.set_range_var(start, start_size, end, end_size);
  return reader_.add_range(dim_idx, r);
}

Status Query::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  if (type_ == QueryType::WRITE)
    return writer_.get_range_num(dim_idx, range_num);
  return reader_.get_range_num(dim_idx, range_num);
}

Status Query::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  if (type_ == QueryType::WRITE)
    return writer_.get_range(dim_idx, range_idx, start, end, stride);
  return reader_.get_range(dim_idx, range_idx, start, end, stride);
}

Status Query::get_range_var_size(
    unsigned dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::WriterError(
        "Getting a var range size from a write query is not applicable"));

  return reader_.get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

Status Query::get_range_var(
    unsigned dim_idx, uint64_t range_idx, void* start, void* end) const {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::WriterError(
        "Getting a var range from a write query is not applicable"));

  uint64_t start_size = 0;
  uint64_t end_size = 0;
  reader_.get_range_var_size(dim_idx, range_idx, &start_size, &end_size);

  const void* range_start;
  const void* range_end;
  const void* stride;
  RETURN_NOT_OK(
      get_range(dim_idx, range_idx, &range_start, &range_end, &stride));

  std::memcpy(start, range_start, start_size);
  std::memcpy(end, range_end, end_size);

  return Status::Ok();
}

Status Query::add_range_by_name(
    const std::string& dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema()->domain()->get_dimension_index(
      dim_name, &dim_idx));

  return add_range(dim_idx, start, end, stride);
}

Status Query::add_range_var_by_name(
    const std::string& dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema()->domain()->get_dimension_index(
      dim_name, &dim_idx));

  return add_range_var(dim_idx, start, start_size, end, end_size);
}

Status Query::get_range_num_from_name(
    const std::string& dim_name, uint64_t* range_num) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema()->domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range_num(dim_idx, range_num);
}

Status Query::get_range_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema()->domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range(dim_idx, range_idx, start, end, stride);
}

Status Query::get_range_var_size_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema()->domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

Status Query::get_range_var_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    void* start,
    void* end) const {
  unsigned dim_idx;
  RETURN_NOT_OK(array_->array_schema()->domain()->get_dimension_index(
      dim_name, &dim_idx));

  return get_range_var(dim_idx, range_idx, start, end);
}

Status Query::get_est_result_size(const char* name, uint64_t* size) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Operation currently "
        "unsupported for write queries"));

  if (name == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Name cannot be null"));

  if (name == constants::coords &&
      !array_->array_schema()->domain()->all_dims_same_type())
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with heterogeneous domain"));

  if (name == constants::coords &&
      !array_->array_schema()->domain()->all_dims_fixed())
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with domains with variable-sized dimensions"));

  if (array_->array_schema()->is_nullable(name))
    return LOG_STATUS(Status::WriterError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));

  if (array_->is_remote() && !reader_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    array_->array_schema()->set_array_uri(array_->array_uri());

    RETURN_NOT_OK(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }

  return reader_.get_est_result_size(name, size);
}

Status Query::get_est_result_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Operation currently "
        "unsupported for write queries"));

  if (array_->array_schema()->is_nullable(name))
    return LOG_STATUS(Status::WriterError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));

  if (array_->is_remote() && !reader_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    array_->array_schema()->set_array_uri(array_->array_uri());

    RETURN_NOT_OK(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }

  return reader_.get_est_result_size(name, size_off, size_val);
}

Status Query::get_est_result_size_nullable(
    const char* name, uint64_t* size_val, uint64_t* size_validity) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Operation currently "
        "unsupported for write queries"));

  if (name == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Name cannot be null"));

  if (!array_->array_schema()->attribute(name))
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Nullable API is only"
        "applicable to attributes"));

  if (!array_->array_schema()->is_nullable(name))
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot get estimated result size; Input attribute '") +
        name + "' is not nullable"));

  if (array_->is_remote() && !reader_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    return LOG_STATUS(
        Status::QueryError("Error in query estimate result size; unimplemented "
                           "for nullable attributes in remote arrays."));
  }

  return reader_.get_est_result_size_nullable(name, size_val, size_validity);
}

Status Query::get_est_result_size_nullable(
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Operation currently "
        "unsupported for write queries"));

  if (!array_->array_schema()->attribute(name))
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Nullable API is only"
        "applicable to attributes"));

  if (!array_->array_schema()->is_nullable(name))
    return LOG_STATUS(Status::WriterError(
        std::string("Cannot get estimated result size; Input attribute '") +
        name + "' is not nullable"));

  if (array_->is_remote() && !reader_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    return LOG_STATUS(
        Status::QueryError("Error in query estimate result size; unimplemented "
                           "for nullable attributes in remote arrays."));
  }

  return reader_.get_est_result_size_nullable(
      name, size_off, size_val, size_validity);
}

std::unordered_map<std::string, Subarray::ResultSize>
Query::get_est_result_size_map() {
  return reader_.get_est_result_size_map();
}

std::unordered_map<std::string, Subarray::MemorySize>
Query::get_max_mem_size_map() {
  return reader_.get_max_mem_size_map();
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
  return reader_.buffer_names();
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

Status Query::get_buffer(
    const char* name, void** buffer, uint64_t** buffer_size) const {
  // Check attribute
  auto array_schema = this->array_schema();
  if (name != constants::coords) {
    if (array_schema->attribute(name) == nullptr &&
        array_schema->dimension(name) == nullptr)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot get buffer; Invalid attribute/dimension name '") +
          name + "'"));
  }
  if (array_schema->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is var-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(name, buffer, buffer_size);
  return reader_.get_buffer(name, buffer, buffer_size);
}

Status Query::get_buffer(
    const char* name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  // Check attribute
  auto array_schema = this->array_schema();
  if (name == constants::coords) {
    return LOG_STATUS(
        Status::QueryError("Cannot get buffer; Coordinates are not var-sized"));
  }
  if (array_schema->attribute(name) == nullptr &&
      array_schema->dimension(name) == nullptr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Invalid attribute/dimension name '") +
        name + "'"));
  if (!array_schema->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is fixed-sized"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer(
        name, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.get_buffer(
      name, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
}

Status Query::get_buffer_vbytemap(
    const char* name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size,
    uint8_t** buffer_validity_bytemap,
    uint64_t** buffer_validity_bytemap_size) const {
  const ValidityVector* vv = nullptr;
  RETURN_NOT_OK(get_buffer(
      name, buffer_off, buffer_off_size, buffer_val, buffer_val_size, &vv));

  if (vv != nullptr) {
    *buffer_validity_bytemap = vv->bytemap();
    *buffer_validity_bytemap_size = vv->bytemap_size();
  }

  return Status::Ok();
}

Status Query::get_buffer_vbytemap(
    const char* name,
    void** buffer,
    uint64_t** buffer_size,
    uint8_t** buffer_validity_bytemap,
    uint64_t** buffer_validity_bytemap_size) const {
  const ValidityVector* vv = nullptr;
  RETURN_NOT_OK(get_buffer(name, buffer, buffer_size, &vv));

  if (vv != nullptr) {
    *buffer_validity_bytemap = vv->bytemap();
    *buffer_validity_bytemap_size = vv->bytemap_size();
  }

  return Status::Ok();
}

Status Query::get_buffer(
    const char* name,
    void** buffer,
    uint64_t** buffer_size,
    const ValidityVector** validity_vector) const {
  // Check nullable attribute
  auto array_schema = this->array_schema();
  if (array_schema->attribute(name) == nullptr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Invalid attribute name '") + name +
        "'"));
  if (array_schema->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is var-sized"));
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is non-nullable"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer_nullable(
        name, buffer, buffer_size, validity_vector);
  return reader_.get_buffer_nullable(
      name, buffer, buffer_size, validity_vector);
}

Status Query::get_buffer(
    const char* name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size,
    const ValidityVector** validity_vector) const {
  // Check attribute
  auto array_schema = this->array_schema();
  if (array_schema->attribute(name) == nullptr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; Invalid attribute name '") + name +
        "'"));
  if (!array_schema->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is fixed-sized"));
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is non-nullable"));

  if (type_ == QueryType::WRITE)
    return writer_.get_buffer_nullable(
        name,
        buffer_off,
        buffer_off_size,
        buffer_val,
        buffer_val_size,
        validity_vector);
  return reader_.get_buffer_nullable(
      name,
      buffer_off,
      buffer_off_size,
      buffer_val,
      buffer_val_size,
      validity_vector);
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
      RETURN_NOT_OK(reader_.init(layout_));
    } else {  // Write
      RETURN_NOT_OK(writer_.init(layout_));
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

Status Query::disable_check_global_order() {
  if (type_ == QueryType::READ)
    return LOG_STATUS(Status::QueryError(
        "Cannot disable checking global order; Applicable only to writes"));

  writer_.disable_check_global_order();
  return Status::Ok();
}

Status Query::check_set_fixed_buffer(const std::string& name) {
  if (name == constants::coords &&
      !array_->array_schema()->domain()->all_dims_same_type())
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; Setting a buffer for zipped coordinates is not "
        "applicable to heterogeneous domains"));

  if (name == constants::coords &&
      !array_->array_schema()->domain()->all_dims_fixed())
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; Setting a buffer for zipped coordinates is not "
        "applicable to domains with variable-sized dimensions"));

  return Status::Ok();
}

Status Query::set_config(const Config& config) {
  if (type_ == QueryType::READ)
    reader_.set_config(config);
  else
    writer_.set_config(config);

  return Status::Ok();
}

Status Query::set_buffer(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(name, buffer, buffer_size);
  return reader_.set_buffer(name, buffer, buffer_size, check_null_buffers);
}

Status Query::set_buffer(
    const std::string& name,
    uint64_t* const buffer_off,
    uint64_t* const buffer_off_size,
    void* const buffer_val,
    uint64_t* const buffer_val_size,
    const bool check_null_buffers) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(
        name, buffer_off, buffer_off_size, buffer_val, buffer_val_size);
  return reader_.set_buffer(
      name,
      buffer_off,
      buffer_off_size,
      buffer_val,
      buffer_val_size,
      check_null_buffers);
}

Status Query::set_buffer_vbytemap(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    uint8_t* const buffer_validity_bytemap,
    uint64_t* const buffer_validity_bytemap_size,
    const bool check_null_buffers) {
  // Convert the bytemap into a ValidityVector.
  ValidityVector vv;
  RETURN_NOT_OK(
      vv.init_bytemap(buffer_validity_bytemap, buffer_validity_bytemap_size));

  return set_buffer(
      name, buffer, buffer_size, std::move(vv), check_null_buffers);
}

Status Query::set_buffer_vbytemap(
    const std::string& name,
    uint64_t* const buffer_off,
    uint64_t* const buffer_off_size,
    void* const buffer_val,
    uint64_t* const buffer_val_size,
    uint8_t* const buffer_validity_bytemap,
    uint64_t* const buffer_validity_bytemap_size,
    const bool check_null_buffers) {
  // Convert the bytemap into a ValidityVector.
  ValidityVector vv;
  RETURN_NOT_OK(
      vv.init_bytemap(buffer_validity_bytemap, buffer_validity_bytemap_size));

  return set_buffer(
      name,
      buffer_off,
      buffer_off_size,
      buffer_val,
      buffer_val_size,
      std::move(vv),
      check_null_buffers);
}

Status Query::set_buffer(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    ValidityVector&& validity_vector,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(
        name, buffer, buffer_size, std::move(validity_vector));
  return reader_.set_buffer(
      name,
      buffer,
      buffer_size,
      std::move(validity_vector),
      check_null_buffers);
}

Status Query::set_buffer(
    const std::string& name,
    uint64_t* const buffer_off,
    uint64_t* const buffer_off_size,
    void* const buffer_val,
    uint64_t* const buffer_val_size,
    ValidityVector&& validity_vector,
    const bool check_null_buffers) {
  if (type_ == QueryType::WRITE)
    return writer_.set_buffer(
        name,
        buffer_off,
        buffer_off_size,
        buffer_val,
        buffer_val_size,
        std::move(validity_vector));
  return reader_.set_buffer(
      name,
      buffer_off,
      buffer_off_size,
      buffer_val,
      buffer_val_size,
      std::move(validity_vector),
      check_null_buffers);
}

Status Query::set_est_result_size(
    std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
    std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot set estimated result size; Operation currently "
        "unsupported for write queries"));
  return reader_.set_est_result_size(est_result_size, max_mem_size);
}

Status Query::set_layout(Layout layout) {
  if (layout == Layout::HILBERT)
    return LOG_STATUS(Status::QueryError(
        "Cannot set layout; Hilbert order is not applicable to queries"));

  layout_ = layout;
  return Status::Ok();
}

Status Query::set_condition(const QueryCondition& condition) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot set query condition; Operation only applicable "
        "to read queries"));
  return reader_.set_condition(condition);
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
  if (!array_->array_schema()->domain()->all_dims_same_type())
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray; Function not applicable to "
                           "heterogeneous domains"));

  if (!array_->array_schema()->domain()->all_dims_fixed())
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray; Function not applicable to "
                           "domains with variable-sized dimensions"));

  // Prepare a subarray object
  Subarray sub(array_, layout_);
  if (subarray != nullptr) {
    auto dim_num = array_->array_schema()->dim_num();
    auto s_ptr = (const unsigned char*)subarray;
    uint64_t offset = 0;
    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size = 2 * array_->array_schema()->dimension(d)->coord_size();
      RETURN_NOT_OK(sub.add_range(d, Range(&s_ptr[offset], r_size)));
      offset += r_size;
    }
  }

  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(sub));
  } else if (type_ == QueryType::READ) {
    RETURN_NOT_OK(reader_.set_subarray(sub));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::set_subarray_unsafe(const NDRange& subarray) {
  // Prepare a subarray object
  Subarray sub(array_, layout_);
  if (!subarray.empty()) {
    auto dim_num = array_->array_schema()->dim_num();
    for (unsigned d = 0; d < dim_num; ++d)
      RETURN_NOT_OK(sub.add_range_unsafe(d, subarray[d]));
  }

  if (type_ == QueryType::WRITE) {
    RETURN_NOT_OK(writer_.set_subarray(sub));
  } else if (type_ == QueryType::READ) {
    RETURN_NOT_OK(reader_.set_subarray(sub));
  }

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::submit() {
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

}  // namespace sm
}  // namespace tiledb
