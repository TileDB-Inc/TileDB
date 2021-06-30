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
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/reader.h"
#include "tiledb/sm/query/sparse_global_order_reader.h"
#include "tiledb/sm/query/writer.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

#include <cassert>
#include <iostream>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query(StorageManager* storage_manager, Array* array, URI fragment_uri)
    : array_(array)
    , layout_(Layout::ROW_MAJOR)
    , storage_manager_(storage_manager)
    , stats_(storage_manager_->stats()->create_child("Query"))
    , has_coords_buffer_(false)
    , has_zipped_coords_buffer_(false)
    , coord_buffer_is_set_(false)
    , coord_data_buffer_is_set_(false)
    , coord_offsets_buffer_is_set_(false)
    , data_buffer_name_("")
    , offsets_buffer_name_("")
    , disable_check_global_order_(false)
    , sparse_mode_(false)
    , fragment_uri_(fragment_uri) {
  if (array != nullptr) {
    assert(array->is_open());
    array_schema_ = array->array_schema();

    auto st = array->get_query_type(&type_);
    assert(st.ok());

    if (type_ == QueryType::WRITE) {
      subarray_ = Subarray(array, stats_);
    } else {
      subarray_ = Subarray(array, Layout::ROW_MAJOR, stats_);
    }

    fragment_metadata_ = array->fragment_metadata();
  } else {
    type_ = QueryType::READ;
  }

  coords_info_.coords_buffer_ = nullptr;
  coords_info_.coords_buffer_size_ = nullptr;
  coords_info_.coords_num_ = 0;
  coords_info_.has_coords_ = false;

  callback_ = nullptr;
  callback_data_ = nullptr;
  status_ = QueryStatus::UNINITIALIZED;

  if (storage_manager != nullptr)
    config_ = storage_manager->config();
}

Query::~Query() = default;

/* ****************************** */
/*               API              */
/* ****************************** */

Status Query::add_range(
    unsigned dim_idx, const void* start, const void* end, const void* stride) {
  if (dim_idx >= array_schema_->dim_num())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Invalid dimension index"));

  if (start == nullptr || end == nullptr)
    return LOG_STATUS(Status::QueryError("Cannot add range; Invalid range"));

  if (stride != nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Setting range stride is currently unsupported"));

  if (array_schema_->domain()->dimension(dim_idx)->var_size())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Range must be fixed-sized"));

  // Prepare a temp range
  std::vector<uint8_t> range;
  uint8_t coord_size = array_schema_->dimension(dim_idx)->coord_size();
  range.resize(2 * coord_size);
  std::memcpy(&range[0], start, coord_size);
  std::memcpy(&range[coord_size], end, coord_size);

  bool read_range_oob_error = true;
  if (type_ == QueryType::READ) {
    // Get read_range_oob config setting
    bool found = false;
    std::string read_range_oob = config_.get("sm.read_range_oob", &found);
    assert(found);

    if (read_range_oob != "error" && read_range_oob != "warn")
      return LOG_STATUS(Status::QueryError(
          "Invalid value " + read_range_oob +
          " for sm.read_range_obb. Acceptable values are 'error' or 'warn'."));

    read_range_oob_error = read_range_oob == "error";
  } else {
    if (!array_schema_->dense())
      return LOG_STATUS(
          Status::QueryError("Adding a subarray range to a write query is not "
                             "supported in sparse arrays"));

    if (subarray_.is_set(dim_idx))
      return LOG_STATUS(
          Status::QueryError("Cannot add range; Multi-range dense writes "
                             "are not supported"));
  }

  // Add range
  Range r(&range[0], 2 * coord_size);
  return subarray_.add_range(dim_idx, std::move(r), read_range_oob_error);
}

Status Query::add_range_var(
    unsigned dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (dim_idx >= array_schema_->dim_num())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Invalid dimension index"));

  if (start == nullptr || end == nullptr)
    return LOG_STATUS(Status::QueryError("Cannot add range; Invalid range"));

  if (start_size == 0 || end_size == 0)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Range start/end cannot have zero length"));

  if (!array_schema_->domain()->dimension(dim_idx)->var_size())
    return LOG_STATUS(
        Status::QueryError("Cannot add range; Range must be variable-sized"));

  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot add range; Function applicable only to reads"));

  // Get read_range_oob config setting
  bool found = false;
  std::string read_range_oob = config_.get("sm.read_range_oob", &found);
  assert(found);

  if (read_range_oob != "error" && read_range_oob != "warn")
    return LOG_STATUS(Status::QueryError(
        "Invalid value " + read_range_oob +
        " for sm.read_range_obb. Acceptable values are 'error' or 'warn'."));

  // Add range
  Range r;
  r.set_range_var(start, start_size, end, end_size);
  return subarray_.add_range(dim_idx, std::move(r), read_range_oob == "error");
}

Status Query::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  if (type_ == QueryType::WRITE && !array_schema_->dense())
    return LOG_STATUS(
        Status::QueryError("Getting the number of ranges from a write query "
                           "is not applicable to sparse arrays"));

  return subarray_.get_range_num(dim_idx, range_num);
}

Status Query::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  if (type_ == QueryType::WRITE && !array_schema_->dense())
    return LOG_STATUS(
        Status::QueryError("Getting a range from a write query is not "
                           "applicable to sparse arrays"));

  *stride = nullptr;
  return subarray_.get_range(dim_idx, range_idx, start, end);
}

Status Query::get_range_var_size(
    unsigned dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Getting a var range size from a write query is not applicable"));

  return subarray_.get_range_var_size(dim_idx, range_idx, start_size, end_size);
  ;
}

Status Query::get_range_var(
    unsigned dim_idx, uint64_t range_idx, void* start, void* end) const {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Getting a var range from a write query is not applicable"));

  uint64_t start_size = 0;
  uint64_t end_size = 0;
  subarray_.get_range_var_size(dim_idx, range_idx, &start_size, &end_size);

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
  RETURN_NOT_OK(
      array_schema_->domain()->get_dimension_index(dim_name, &dim_idx));

  return add_range(dim_idx, start, end, stride);
}

Status Query::add_range_var_by_name(
    const std::string& dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain()->get_dimension_index(dim_name, &dim_idx));

  return add_range_var(dim_idx, start, start_size, end, end_size);
}

Status Query::get_range_num_from_name(
    const std::string& dim_name, uint64_t* range_num) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain()->get_dimension_index(dim_name, &dim_idx));

  return get_range_num(dim_idx, range_num);
}

Status Query::get_range_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain()->get_dimension_index(dim_name, &dim_idx));

  return get_range(dim_idx, range_idx, start, end, stride);
}

Status Query::get_range_var_size_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain()->get_dimension_index(dim_name, &dim_idx));

  return get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

Status Query::get_range_var_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    void* start,
    void* end) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain()->get_dimension_index(dim_name, &dim_idx));

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
      !array_schema_->domain()->all_dims_same_type())
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with heterogeneous domain"));

  if (name == constants::coords && !array_schema_->domain()->all_dims_fixed())
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with domains with variable-sized dimensions"));

  if (array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    array_schema_->set_array_uri(array_->array_uri());

    RETURN_NOT_OK(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }

  return subarray_.get_est_result_size(
      name, size, &config_, storage_manager_->compute_tp());
}

Status Query::get_est_result_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Operation currently "
        "unsupported for write queries"));

  if (array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    array_schema_->set_array_uri(array_->array_uri());

    RETURN_NOT_OK(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }

  return subarray_.get_est_result_size(
      name, size_off, size_val, &config_, storage_manager_->compute_tp());
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

  if (!array_schema_->attribute(name))
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Nullable API is only"
        "applicable to attributes"));

  if (!array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get estimated result size; Input attribute '") +
        name + "' is not nullable"));

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    return LOG_STATUS(
        Status::QueryError("Error in query estimate result size; unimplemented "
                           "for nullable attributes in remote arrays."));
  }

  return subarray_.get_est_result_size_nullable(
      name, size_val, size_validity, &config_, storage_manager_->compute_tp());
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

  if (!array_schema_->attribute(name))
    return LOG_STATUS(Status::QueryError(
        "Cannot get estimated result size; Nullable API is only"
        "applicable to attributes"));

  if (!array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get estimated result size; Input attribute '") +
        name + "' is not nullable"));

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(
          Status::QueryError("Error in query estimate result size; remote "
                             "array with no rest client."));

    return LOG_STATUS(
        Status::QueryError("Error in query estimate result size; unimplemented "
                           "for nullable attributes in remote arrays."));
  }

  return subarray_.get_est_result_size_nullable(
      name,
      size_off,
      size_val,
      size_validity,
      &config_,
      storage_manager_->compute_tp());
}

std::unordered_map<std::string, Subarray::ResultSize>
Query::get_est_result_size_map() {
  return subarray_.get_est_result_size_map(
      &config_, storage_manager_->compute_tp());
}

std::unordered_map<std::string, Subarray::MemorySize>
Query::get_max_mem_size_map() {
  return subarray_.get_max_mem_size_map(
      &config_, storage_manager_->compute_tp());
}

Status Query::get_written_fragment_num(uint32_t* num) const {
  if (type_ != QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get number of fragments; Applicable only to WRITE mode"));

  *num = (uint32_t)written_fragment_info_.size();

  return Status::Ok();
}

Status Query::get_written_fragment_uri(uint32_t idx, const char** uri) const {
  if (type_ != QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get fragment URI; Applicable only to WRITE mode"));

  auto num = (uint32_t)written_fragment_info_.size();
  if (idx >= num)
    return LOG_STATUS(
        Status::QueryError("Cannot get fragment URI; Invalid fragment index"));

  *uri = written_fragment_info_[idx].uri_.c_str();

  return Status::Ok();
}

Status Query::get_written_fragment_timestamp_range(
    uint32_t idx, uint64_t* t1, uint64_t* t2) const {
  if (type_ != QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot get fragment timestamp range; Applicable only to WRITE mode"));

  auto num = (uint32_t)written_fragment_info_.size();
  if (idx >= num)
    return LOG_STATUS(Status::QueryError(
        "Cannot get fragment timestamp range; Invalid fragment index"));

  *t1 = written_fragment_info_[idx].timestamp_range_.first;
  *t2 = written_fragment_info_[idx].timestamp_range_.second;

  return Status::Ok();
}

const Array* Query::array() const {
  return array_;
}

Array* Query::array() {
  return array_;
}

const ArraySchema* Query::array_schema() const {
  return array_schema_;
}

std::vector<std::string> Query::buffer_names() const {
  std::vector<std::string> ret;

  // Add to the buffer names the attributes, as well as the dimensions only if
  // coords_buffer_ has not been set
  for (const auto& it : buffers_) {
    if (!array_schema_->is_dim(it.first) || (!coords_info_.coords_buffer_))
      ret.push_back(it.first);
  }

  // Special zipped coordinates name
  if (coords_info_.coords_buffer_)
    ret.push_back(constants::coords);

  return ret;
}

QueryBuffer Query::buffer(const std::string& name) const {
  // Special zipped coordinates
  if (type_ == QueryType::WRITE && name == constants::coords)
    return QueryBuffer(
        coords_info_.coords_buffer_,
        nullptr,
        coords_info_.coords_buffer_size_,
        nullptr);

  // Attribute or dimension
  auto buf = buffers_.find(name);
  if (buf != buffers_.end())
    return buf->second;

  // Named buffer does not exist
  return QueryBuffer{};
}

Status Query::finalize() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return Status::Ok();

  if (array_->is_remote()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::QueryError(
          "Error in query finalize; remote array with no rest client."));

    array_schema_->set_array_uri(array_->array_uri());

    return rest_client->finalize_query_to_rest(array_->array_uri(), this);
  }

  RETURN_NOT_OK(strategy_->finalize());
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

  return get_data_buffer(name, buffer, buffer_size);
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

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    *buffer_val = it->second.buffer_var_;
    *buffer_val_size = it->second.buffer_var_size_;
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer_off = nullptr;
  *buffer_off_size = nullptr;
  *buffer_val = nullptr;
  *buffer_val_size = nullptr;

  return Status::Ok();
}

Status Query::get_offsets_buffer(
    const char* name, uint64_t** buffer_off, uint64_t** buffer_off_size) const {
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

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer_off = nullptr;
  *buffer_off_size = nullptr;

  return Status::Ok();
}

Status Query::get_data_buffer(
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

  // Special zipped coordinates
  if (type_ == QueryType::WRITE && name == constants::coords) {
    *buffer = coords_info_.coords_buffer_;
    *buffer_size = coords_info_.coords_buffer_size_;
    return Status::Ok();
  }

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    if (!array_schema_->var_size(name)) {
      *buffer = it->second.buffer_;
      *buffer_size = it->second.buffer_size_;
    } else {
      *buffer = it->second.buffer_var_;
      *buffer_size = it->second.buffer_var_size_;
    }
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer = nullptr;
  *buffer_size = nullptr;

  return Status::Ok();
}

Status Query::get_validity_buffer(
    const char* name,
    uint8_t** buffer_validity_bytemap,
    uint64_t** buffer_validity_bytemap_size) const {
  // Check attribute
  auto array_schema = this->array_schema();
  if (!array_schema->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot get buffer; '") + name + "' is non-nullable"));

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    auto vv = &it->second.validity_vector_;
    *buffer_validity_bytemap = vv->bytemap();
    *buffer_validity_bytemap_size = vv->bytemap_size();
  }

  return Status::Ok();
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

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    *buffer = it->second.buffer_;
    *buffer_size = it->second.buffer_size_;
    *validity_vector = &it->second.validity_vector_;
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer = nullptr;
  *buffer_size = nullptr;
  *validity_vector = nullptr;

  return Status::Ok();
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

  // Attribute or dimension
  auto it = buffers_.find(name);
  if (it != buffers_.end()) {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    *buffer_val = it->second.buffer_var_;
    *buffer_val_size = it->second.buffer_var_size_;
    *validity_vector = &it->second.validity_vector_;
    return Status::Ok();
  }

  // Named buffer does not exist
  *buffer_off = nullptr;
  *buffer_off_size = nullptr;
  *buffer_val = nullptr;
  *buffer_val_size = nullptr;
  *validity_vector = nullptr;

  return Status::Ok();
}

Status Query::get_attr_serialization_state(
    const std::string& attribute, SerializationState::AttrState** state) {
  *state = &serialization_state_.attribute_states[attribute];
  return Status::Ok();
}

bool Query::has_results() const {
  if (status_ == QueryStatus::UNINITIALIZED || type_ == QueryType::WRITE)
    return false;

  for (const auto& it : buffers_) {
    if (*(it.second.buffer_size_) != 0)
      return true;
  }
  return false;
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

    RETURN_NOT_OK(check_buffer_names());
    RETURN_NOT_OK(create_strategy());
    RETURN_NOT_OK(strategy_->init());
  }

  status_ = QueryStatus::INPROGRESS;

  return Status::Ok();
}

URI Query::first_fragment_uri() const {
  if (type_ == QueryType::WRITE || fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.front()->fragment_uri();
}

URI Query::last_fragment_uri() const {
  if (type_ == QueryType::WRITE || fragment_metadata_.empty())
    return URI();
  return fragment_metadata_.back()->fragment_uri();
}

Layout Query::layout() const {
  return layout_;
}

const QueryCondition* Query::condition() const {
  if (type_ == QueryType::WRITE)
    return nullptr;
  return &condition_;
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
  Status st = strategy_->dowork();

  // Handle error
  if (!st.ok()) {
    status_ = QueryStatus::FAILED;
    return st;
  }

  if (type_ == QueryType::WRITE && layout_ == Layout::GLOBAL_ORDER) {
    // reset coord buffer marker at end of global write
    // this will allow for the user to properly set the next write batch
    coord_buffer_is_set_ = false;
    coord_data_buffer_is_set_ = false;
    coord_offsets_buffer_is_set_ = false;
  }

  // Check if the query is complete
  bool completed = !strategy_->incomplete();

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

Status Query::create_strategy() {
  if (type_ == QueryType::WRITE) {
    strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
        Writer,
        stats_->create_child("Writer"),
        storage_manager_,
        array_,
        config_,
        buffers_,
        subarray_,
        layout_,
        written_fragment_info_,
        disable_check_global_order_,
        coords_info_,
        fragment_uri_));
  } else {
    bool found = false;
    bool use_refactored_readers = false;
    RETURN_NOT_OK(config_.get<bool>(
        "sm.use_refactored_readers", &use_refactored_readers, &found));
    assert(found);

    bool use_default = true;
    if (use_refactored_readers) {
      if (!array_schema_->dense() && layout_ == Layout::GLOBAL_ORDER) {
        use_default = false;
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseGlobalOrderReader,
            stats_->create_child("Reader"),
            storage_manager_,
            array_,
            config_,
            buffers_,
            subarray_,
            layout_,
            condition_));
      }
    }

    if (use_default) {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          Reader,
          stats_->create_child("Reader"),
          storage_manager_,
          array_,
          config_,
          buffers_,
          subarray_,
          layout_,
          condition_,
          sparse_mode_));
    }
  }

  if (strategy_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot create strategy; allocation failed"));

  return Status::Ok();
}

IQueryStrategy* Query::strategy() {
  if (strategy_ == nullptr) {
    create_strategy();
  }
  return strategy_.get();
}

Status Query::disable_check_global_order() {
  if (status_ != QueryStatus::UNINITIALIZED)
    return LOG_STATUS(Status::QueryError(
        "Cannot disable checking global order after initialization"));

  if (type_ == QueryType::READ)
    return LOG_STATUS(Status::QueryError(
        "Cannot disable checking global order; Applicable only to writes"));

  disable_check_global_order_ = true;
  return Status::Ok();
}

Status Query::check_buffer_names() {
  if (type_ == QueryType::WRITE) {
    // If the array is sparse, the coordinates must be provided
    if (!array_schema_->dense() && !coords_info_.has_coords_)
      return LOG_STATUS(Status::WriterError(
          "Sparse array writes expect the coordinates of the "
          "cells to be written"));

    // If the layout is unordered, the coordinates must be provided
    if (layout_ == Layout::UNORDERED && !coords_info_.has_coords_)
      return LOG_STATUS(
          Status::WriterError("Unordered writes expect the coordinates of the "
                              "cells to be written"));

    // All attributes/dimensions must be provided
    auto expected_num = array_schema_->attribute_num();
    expected_num += (coord_buffer_is_set_ || coord_data_buffer_is_set_ ||
                     coord_offsets_buffer_is_set_) ?
                        array_schema_->dim_num() :
                        0;
    if (buffers_.size() != expected_num)
      return LOG_STATUS(Status::WriterError(
          "Writes expect all attributes (and coordinates in "
          "the sparse/unordered case) to be set"));
  }

  return Status::Ok();
}

Status Query::check_set_fixed_buffer(const std::string& name) {
  if (name == constants::coords &&
      !array_schema_->domain()->all_dims_same_type())
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; Setting a buffer for zipped coordinates is not "
        "applicable to heterogeneous domains"));

  if (name == constants::coords && !array_schema_->domain()->all_dims_fixed())
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; Setting a buffer for zipped coordinates is not "
        "applicable to domains with variable-sized dimensions"));

  return Status::Ok();
}

Status Query::set_config(const Config& config) {
  config_ = config;

  return Status::Ok();
}

Status Query::set_coords_buffer(void* buffer, uint64_t* buffer_size) {
  // Set zipped coordinates buffer
  coords_info_.coords_buffer_ = buffer;
  coords_info_.coords_buffer_size_ = buffer_size;
  coords_info_.has_coords_ = true;

  return Status::Ok();
}

Status Query::set_buffer(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check buffer
  if (check_null_buffers && buffer == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_size == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Must not be nullable
  if (array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is nullable"));

  // Check that attribute/dimension is fixed-sized
  const bool var_size =
      (name != constants::coords && array_schema_->var_size(name));
  if (var_size)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is var-sized"));

  // Check if zipped coordinates coexist with separate coordinate buffers
  if ((is_dim && has_zipped_coords_buffer_) ||
      (name == constants::coords && has_coords_buffer_))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set separate coordinate buffers and "
                    "a zipped coordinate buffer in the same query")));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (name == constants::coords) {
    has_zipped_coords_buffer_ = true;

    // Set special function for zipped coordinates buffer
    if (type_ == QueryType::WRITE)
      return set_coords_buffer(buffer, buffer_size);
  }

  if (is_dim && type_ == QueryType::WRITE) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_size / array_schema_->cell_size(name);
    if (coord_buffer_is_set_ && coords_num != coords_info_.coords_num_)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_info_.coords_num_ = coords_num;
    coord_buffer_is_set_ = true;
    coords_info_.has_coords_ = true;
  }

  has_coords_buffer_ |= is_dim;

  // Set attribute buffer
  buffers_[name] = QueryBuffer(buffer, nullptr, buffer_size, nullptr);

  return Status::Ok();
}

Status Query::set_data_buffer(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check buffer
  if (check_null_buffers && buffer == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Check if zipped coordinates coexist with separate coordinate buffers
  if ((is_dim && has_zipped_coords_buffer_) ||
      (name == constants::coords && has_coords_buffer_))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set separate coordinate buffers and "
                    "a zipped coordinate buffer in the same query")));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (name == constants::coords) {
    has_zipped_coords_buffer_ = true;

    // Set special function for zipped coordinates buffer
    if (type_ == QueryType::WRITE)
      return set_coords_buffer(buffer, buffer_size);
  }

  if (is_dim && type_ == QueryType::WRITE) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_size / array_schema_->cell_size(name);
    if (coord_data_buffer_is_set_ && coords_num != coords_info_.coords_num_ &&
        name == data_buffer_name_)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_info_.coords_num_ = coords_num;
    coord_data_buffer_is_set_ = true;
    data_buffer_name_ = name;
    coords_info_.has_coords_ = true;
  }

  has_coords_buffer_ |= is_dim;

  // Set attribute/dimension buffer on the appropriate buffer
  if (!array_schema_->var_size(name))
    // Fixed size data buffer
    buffers_[name].set_data_buffer(buffer, buffer_size);
  else
    // Var sized data buffer
    buffers_[name].set_data_var_buffer(buffer, buffer_size);

  return Status::Ok();
}

Status Query::set_offsets_buffer(
    const std::string& name,
    uint64_t* const buffer_offsets,
    uint64_t* const buffer_offsets_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check buffer
  if (check_null_buffers && buffer_offsets == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_offsets_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Neither a dimension nor an attribute
  if (!is_dim && !is_attr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Invalid buffer name '") + name +
        "' (it should be an attribute or dimension)"));

  // Error if it is fixed-sized
  if (!array_schema_->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute/dimension after initialization
  bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (is_dim && type_ == QueryType::WRITE) {
    // Check number of coordinates
    uint64_t coords_num =
        *buffer_offsets_size / constants::cell_var_offset_size;
    if (coord_offsets_buffer_is_set_ &&
        coords_num != coords_info_.coords_num_ && name != offsets_buffer_name_)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_info_.coords_num_ = coords_num;
    coord_offsets_buffer_is_set_ = true;
    coords_info_.has_coords_ = true;
    offsets_buffer_name_ = name;
  }

  has_coords_buffer_ |= is_dim;

  // Set attribute/dimension buffer
  buffers_[name].set_offsets_buffer(buffer_offsets, buffer_offsets_size);

  return Status::Ok();
}

Status Query::set_validity_buffer(
    const std::string& name,
    uint8_t* const buffer_validity_bytemap,
    uint64_t* const buffer_validity_bytemap_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  ValidityVector validity_vector;
  RETURN_NOT_OK(validity_vector.init_bytemap(
      buffer_validity_bytemap, buffer_validity_bytemap_size));
  // Check validity buffer
  if (check_null_buffers && validity_vector.buffer() == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));

  // Check validity buffer size
  if (check_null_buffers && validity_vector.buffer_size() == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // Must be an attribute
  if (!array_schema_->is_attr(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));

  // Must be nullable
  if (!array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));

  // Error if setting a new attribute after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute/dimension buffer
  buffers_[name].set_validity_buffer(std::move(validity_vector));

  return Status::Ok();
}

Status Query::set_buffer(
    const std::string& name,
    uint64_t* const buffer_off,
    uint64_t* const buffer_off_size,
    void* const buffer_val,
    uint64_t* const buffer_val_size,
    const bool check_null_buffers) {
  // Check buffer
  if (check_null_buffers && buffer_val == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_val_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Check offset buffer
  if (check_null_buffers && buffer_off == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " offset buffer is null"));

  // Check offset buffer size
  if (check_null_buffers && buffer_off_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " offset buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (!is_dim && !is_attr)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Must not be nullable
  if (array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is nullable"));

  // Check that attribute/dimension is var-sized
  if (!array_schema_->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (is_dim && type_ == QueryType::WRITE) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_off_size / constants::cell_var_offset_size;
    if (coord_buffer_is_set_ && coords_num != coords_info_.coords_num_)
      return LOG_STATUS(Status::QueryError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_info_.coords_num_ = coords_num;
    coord_buffer_is_set_ = true;
    coords_info_.has_coords_ = true;
  }

  // Set attribute/dimension buffer
  buffers_[name] =
      QueryBuffer(buffer_off, buffer_val, buffer_off_size, buffer_val_size);

  return Status::Ok();
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

  // Check buffer
  if (check_null_buffers && buffer == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Check validity buffer offset
  if (check_null_buffers && validity_vector.buffer() == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));

  // Check validity buffer size
  if (check_null_buffers && validity_vector.buffer_size() == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // Must be an attribute
  if (!array_schema_->is_attr(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));

  // Must be fixed-size
  if (array_schema_->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is var-sized"));

  // Must be nullable
  if (!array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute buffer
  buffers_[name] = QueryBuffer(
      buffer, nullptr, buffer_size, nullptr, std::move(validity_vector));

  return Status::Ok();
}

Status Query::set_buffer(
    const std::string& name,
    uint64_t* const buffer_off,
    uint64_t* const buffer_off_size,
    void* const buffer_val,
    uint64_t* const buffer_val_size,
    ValidityVector&& validity_vector,
    const bool check_null_buffers) {
  // Check buffer
  if (check_null_buffers && buffer_val == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_val_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Check buffer offset
  if (check_null_buffers && buffer_off == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " offset buffer is null"));

  // Check buffer offset size
  if (check_null_buffers && buffer_off_size == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " offset buffer size is null"));
  ;

  // Check validity buffer offset
  if (check_null_buffers && validity_vector.buffer() == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));

  // Check validity buffer size
  if (check_null_buffers && validity_vector.buffer_size() == nullptr)
    return LOG_STATUS(Status::QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));

  // Array schema must exist
  if (array_schema_ == nullptr)
    return LOG_STATUS(
        Status::QueryError("Cannot set buffer; Array schema not set"));

  // Must be an attribute
  if (!array_schema_->is_attr(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));

  // Must be var-size
  if (!array_schema_->var_size(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is fixed-sized"));

  // Must be nullable
  if (!array_schema_->is_nullable(name))
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));

  // Error if setting a new attribute after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return LOG_STATUS(Status::QueryError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute/dimension buffer
  buffers_[name] = QueryBuffer(
      buffer_off,
      buffer_val,
      buffer_off_size,
      buffer_val_size,
      std::move(validity_vector));

  return Status::Ok();
}

Status Query::set_est_result_size(
    std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
    std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot set estimated result size; Operation currently "
        "unsupported for write queries"));
  return subarray_.set_est_result_size(est_result_size, max_mem_size);
}

Status Query::set_layout_unsafe(Layout layout) {
  layout_ = layout;
  subarray_.set_layout(layout);
  return Status::Ok();
}

Status Query::set_layout(Layout layout) {
  if (type_ == QueryType::READ && status_ != QueryStatus::UNINITIALIZED)
    return LOG_STATUS(
        Status::QueryError("Cannot set layout after initialization"));

  if (layout == Layout::HILBERT)
    return LOG_STATUS(Status::QueryError(
        "Cannot set layout; Hilbert order is not applicable to queries"));

  layout_ = layout;
  subarray_.set_layout(layout);
  return Status::Ok();
}

Status Query::set_condition(const QueryCondition& condition) {
  if (type_ == QueryType::WRITE)
    return LOG_STATUS(Status::QueryError(
        "Cannot set query condition; Operation only applicable "
        "to read queries"));

  condition_ = condition;
  return Status::Ok();
}

Status Query::set_sparse_mode(bool sparse_mode) {
  if (status_ != QueryStatus::UNINITIALIZED)
    return LOG_STATUS(
        Status::QueryError("Cannot set sparse mode after initialization"));

  if (type_ != QueryType::READ)
    return LOG_STATUS(Status::QueryError(
        "Cannot set sparse mode; Only applicable to read queries"));

  if (!array_schema_->dense())
    return LOG_STATUS(Status::QueryError(
        "Cannot set sparse mode; Only applicable to dense arrays"));

  bool all_sparse = true;
  for (const auto& f : fragment_metadata_) {
    if (f->dense()) {
      all_sparse = false;
      break;
    }
  }

  if (!all_sparse)
    return LOG_STATUS(
        Status::QueryError("Cannot set sparse mode; Only applicable to opened "
                           "dense arrays having only sparse fragments"));

  sparse_mode_ = sparse_mode;
  return Status::Ok();
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

Status Query::set_subarray(const void* subarray) {
  if (!array_schema_->domain()->all_dims_same_type())
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray; Function not applicable to "
                           "heterogeneous domains"));

  if (!array_schema_->domain()->all_dims_fixed())
    return LOG_STATUS(
        Status::QueryError("Cannot set subarray; Function not applicable to "
                           "domains with variable-sized dimensions"));

  // Prepare a subarray object
  Subarray sub(array_, layout_, stats_);
  if (subarray != nullptr) {
    auto dim_num = array_schema_->dim_num();
    auto s_ptr = (const unsigned char*)subarray;
    uint64_t offset = 0;

    bool err_on_range_oob = true;
    if (type_ == QueryType::READ) {
      // Get read_range_oob config setting
      bool found = false;
      std::string read_range_oob_str =
          config()->get("sm.read_range_oob", &found);
      assert(found);
      if (read_range_oob_str != "error" && read_range_oob_str != "warn")
        return LOG_STATUS(Status::QueryError(
            "Invalid value " + read_range_oob_str +
            " for sm.read_range_obb. Acceptable values are 'error' or "
            "'warn'."));
      err_on_range_oob = read_range_oob_str == "error";
    }

    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size = 2 * array_schema_->dimension(d)->coord_size();
      Range range(&s_ptr[offset], r_size);
      RETURN_NOT_OK(sub.add_range(d, std::move(range), err_on_range_oob));
      offset += r_size;
    }
  }

  if (type_ == QueryType::WRITE) {
    // Not applicable to sparse arrays
    if (!array_schema_->dense())
      return LOG_STATUS(Status::WriterError(
          "Setting a subarray is not supported in sparse writes"));

    // Subarray must be unary for dense writes
    if (sub.range_num() != 1)
      return LOG_STATUS(
          Status::WriterError("Cannot set subarray; Multi-range dense writes "
                              "are not supported"));
    if (strategy_ != nullptr)
      strategy_->reset();
  }

  subarray_ = sub;

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

const Subarray* Query::subarray() const {
  return &subarray_;
}

Status Query::set_subarray_unsafe(const Subarray& subarray) {
  subarray_ = subarray;
  return Status::Ok();
}

Status Query::set_subarray_unsafe(const NDRange& subarray) {
  // Prepare a subarray object
  Subarray sub(array_, layout_, stats_);
  if (!subarray.empty()) {
    auto dim_num = array_schema_->dim_num();
    for (unsigned d = 0; d < dim_num; ++d)
      RETURN_NOT_OK(sub.add_range_unsafe(d, subarray[d]));
  }

  subarray_ = sub;

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::check_buffers_correctness() {
  // Iterate through each attribute
  for (auto& attr : buffer_names()) {
    if (buffer(attr).buffer_ == nullptr)
      return LOG_STATUS(
          Status::QueryError(std::string("Data buffer is not set")));
    if (array_schema_->var_size(attr)) {
      // Var-sized
      // Check for data buffer under buffer_var
      bool exists_data = buffer(attr).buffer_var_ != nullptr;
      bool exists_offset = buffer(attr).buffer_ != nullptr;
      if (!exists_data || !exists_offset) {
        return LOG_STATUS(Status::QueryError(
            std::string("Var-Sized input attribute/dimension '") + attr +
            "' is not set correctly \nOffsets buffer is not set"));
      }
    } else {
      // Fixed sized
      bool exists_data = buffer(attr).buffer_ != nullptr;
      bool exists_offset = buffer(attr).buffer_var_ != nullptr;
      if (!exists_data || exists_offset) {
        return LOG_STATUS(Status::QueryError(
            std::string("Fix-Sized input attribute/dimension '") + attr +
            "' is not set correctly \nOffsets buffer is not set"));
      }
    }
    if (array_schema_->is_nullable(attr)) {
      bool exists_validity = buffer(attr).validity_vector_.buffer() != nullptr;
      if (!exists_validity) {
        return LOG_STATUS(Status::QueryError(
            std::string("Nullable input attribute/dimension '") + attr +
            "' is not set correctly \nValidity buffer is not set"));
      }
    }
  }
  return Status::Ok();
}

Status Query::submit() {
  // Do not resubmit completed reads.
  if (type_ == QueryType::READ && status_ == QueryStatus::COMPLETED) {
    return Status::Ok();
  }
  // Check attribute/dimensions buffers completeness before query submits
  RETURN_NOT_OK(check_buffers_correctness());

  if (array_->is_remote()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return LOG_STATUS(Status::QueryError(
          "Error in query submission; remote array with no rest client."));

    array_schema_->set_array_uri(array_->array_uri());
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

const Config* Query::config() const {
  return &config_;
}

stats::Stats* Query::stats() const {
  return stats_;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
