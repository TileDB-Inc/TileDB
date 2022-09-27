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
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/sm/query/deletes_and_updates/deletes.h"
#include "tiledb/sm/query/dimension_label/array_dimension_label_queries.h"
#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/dense_reader.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/sm/query/readers/sparse_unordered_with_dups_reader.h"
#include "tiledb/sm/query/writers/global_order_writer.h"
#include "tiledb/sm/query/writers/ordered_writer.h"
#include "tiledb/sm/query/writers/unordered_writer.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/writer_tile.h"

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

Query::Query(
    StorageManager* storage_manager,
    shared_ptr<Array> array,
    optional<std::string> fragment_name)
    : array_shared_(array)
    , array_(array_shared_.get())
    , array_schema_(array->array_schema_latest_ptr())
    , type_(array_->get_query_type())
    , layout_(
          (type_ == QueryType::READ || array_schema_->dense()) ?
              Layout::ROW_MAJOR :
              Layout::UNORDERED)
    , storage_manager_(storage_manager)
    , stats_(storage_manager_->stats()->create_child("Query"))
    , logger_(storage_manager->logger()->clone("Query", ++logger_id_))
    , dim_label_queries_(nullptr)
    , has_coords_buffer_(false)
    , has_zipped_coords_buffer_(false)
    , coord_buffer_is_set_(false)
    , coord_data_buffer_is_set_(false)
    , coord_offsets_buffer_is_set_(false)
    , data_buffer_name_("")
    , offsets_buffer_name_("")
    , disable_checks_consolidation_(false)
    , consolidation_with_timestamps_(false)
    , force_legacy_reader_(false)
    , fragment_name_(fragment_name)
    , remote_query_(false) {
  assert(array->is_open());

  subarray_ = Subarray(array_, layout_, stats_, logger_);

  fragment_metadata_ = array->fragment_metadata();

  coords_info_.coords_buffer_ = nullptr;
  coords_info_.coords_buffer_size_ = nullptr;
  coords_info_.coords_num_ = 0;
  coords_info_.has_coords_ = false;

  callback_ = nullptr;
  callback_data_ = nullptr;
  status_ = QueryStatus::UNINITIALIZED;

  if (storage_manager != nullptr)
    config_ = storage_manager->config();

  // Set initial subarray configuration
  subarray_.set_config(config_);

  rest_scratch_ = make_shared<Buffer>(HERE());
}

Query::~Query() {
  bool found = false;
  bool use_malloc_trim = false;
  const Status& st =
      config_.get<bool>("sm.mem.malloc_trim", &use_malloc_trim, &found);
  if (st.ok() && found && use_malloc_trim) {
    tdb_malloc_trim();
  }
};

/* ****************************** */
/*               API              */
/* ****************************** */

Status Query::add_range(
    unsigned dim_idx, const void* start, const void* end, const void* stride) {
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(
        Status_QueryError("Adding a range for an unsupported query type"));
  }

  if (dim_idx >= array_schema_->dim_num()) {
    return logger_->status(
        Status_QueryError("Cannot add range; Invalid dimension index"));
  }

  if (start == nullptr || end == nullptr) {
    return logger_->status(
        Status_QueryError("Cannot add range; Invalid range"));
  }

  if (stride != nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot add range; Setting range stride is currently unsupported"));
  }

  if (array_schema_->domain().dimension_ptr(dim_idx)->var_size()) {
    return logger_->status(
        Status_QueryError("Cannot add range; Range must be fixed-sized"));
  }

  // Prepare a temp range
  std::vector<uint8_t> range;
  auto coord_size{array_schema_->dimension_ptr(dim_idx)->coord_size()};
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
      return logger_->status(Status_QueryError(
          "Invalid value " + read_range_oob +
          " for sm.read_range_obb. Acceptable values are 'error' or 'warn'."));

    read_range_oob_error = read_range_oob == "error";
  } else {
    if (!array_schema_->dense())
      return logger_->status(
          Status_QueryError("Adding a subarray range to a write query is not "
                            "supported in sparse arrays"));

    if (subarray_.is_set(dim_idx))
      return logger_->status(
          Status_QueryError("Cannot add range; Multi-range dense writes "
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
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(
        Status_QueryError("Adding a range for an unsupported query type"));
  }

  if (dim_idx >= array_schema_->dim_num())
    return logger_->status(
        Status_QueryError("Cannot add range; Invalid dimension index"));

  if ((start == nullptr && start_size != 0) ||
      (end == nullptr && end_size != 0))
    return logger_->status(
        Status_QueryError("Cannot add range; Invalid range"));

  if (!array_schema_->domain().dimension_ptr(dim_idx)->var_size())
    return logger_->status(
        Status_QueryError("Cannot add range; Range must be variable-sized"));

  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)
    return logger_->status(Status_QueryError(
        "Cannot add range; Function applicable only to reads"));

  // Get read_range_oob config setting
  bool found = false;
  std::string read_range_oob = config_.get("sm.read_range_oob", &found);
  assert(found);

  if (read_range_oob != "error" && read_range_oob != "warn")
    return logger_->status(Status_QueryError(
        "Invalid value " + read_range_oob +
        " for sm.read_range_obb. Acceptable values are 'error' or 'warn'."));

  // Add range
  Range r;
  r.set_range_var(start, start_size, end, end_size);
  return subarray_.add_range(dim_idx, std::move(r), read_range_oob == "error");
}

Status Query::get_range_num(unsigned dim_idx, uint64_t* range_num) const {
  if ((type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      !array_schema_->dense())
    return logger_->status(
        Status_QueryError("Getting the number of ranges from a write query "
                          "is not applicable to sparse arrays"));

  return subarray_.get_range_num(dim_idx, range_num);
}

Status Query::get_range(
    unsigned dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) const {
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Getting a var range size only applicable to read queries"));
  }

  if ((type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      !array_schema_->dense()) {
    return logger_->status(
        Status_QueryError("Getting a range from a write query is not "
                          "applicable to sparse arrays"));
  }

  *stride = nullptr;
  return subarray_.get_range(dim_idx, range_idx, start, end);
}

Status Query::get_range_var_size(
    unsigned dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Getting a var range size only applicable to read queries"));
  }

  return subarray_.get_range_var_size(dim_idx, range_idx, start_size, end_size);
  ;
}

Status Query::get_range_var(
    unsigned dim_idx, uint64_t range_idx, void* start, void* end) const {
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Getting a var range only applicable to read queries"));
  }

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
      array_schema_->domain().get_dimension_index(dim_name, &dim_idx));

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
      array_schema_->domain().get_dimension_index(dim_name, &dim_idx));

  return add_range_var(dim_idx, start, start_size, end, end_size);
}

Status Query::get_range_num_from_name(
    const std::string& dim_name, uint64_t* range_num) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain().get_dimension_index(dim_name, &dim_idx));

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
      array_schema_->domain().get_dimension_index(dim_name, &dim_idx));

  return get_range(dim_idx, range_idx, start, end, stride);
}

Status Query::get_range_var_size_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain().get_dimension_index(dim_name, &dim_idx));

  return get_range_var_size(dim_idx, range_idx, start_size, end_size);
}

Status Query::get_range_var_from_name(
    const std::string& dim_name,
    uint64_t range_idx,
    void* start,
    void* end) const {
  unsigned dim_idx;
  RETURN_NOT_OK(
      array_schema_->domain().get_dimension_index(dim_name, &dim_idx));

  return get_range_var(dim_idx, range_idx, start, end);
}

Status Query::get_est_result_size(const char* name, uint64_t* size) {
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Operation currently "
        "only unsupported for read queries"));
  }

  if (name == nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Name cannot be null"));
  }

  if (name == constants::coords &&
      !array_schema_->domain().all_dims_same_type()) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with heterogeneous domain"));
  }

  if (name == constants::coords && !array_schema_->domain().all_dims_fixed()) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Not applicable to zipped "
        "coordinates in arrays with domains with variable-sized dimensions"));
  }

  if (array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));
  }

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return logger_->status(
          Status_QueryError("Error in query estimate result size; remote "
                            "array with no rest client."));
    }

    RETURN_NOT_OK(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }

  return subarray_.get_est_result_size_internal(
      name, size, &config_, storage_manager_->compute_tp());
}

Status Query::get_est_result_size(
    const char* name, uint64_t* size_off, uint64_t* size_val) {
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Operation currently "
        "only supported for read queries"));
  }

  if (array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string(
            "Cannot get estimated result size; Input attribute/dimension '") +
        name + "' is nullable"));
  }

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return logger_->status(
          Status_QueryError("Error in query estimate result size; remote "
                            "array with no rest client."));
    }

    RETURN_NOT_OK(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }

  return subarray_.get_est_result_size(
      name, size_off, size_val, &config_, storage_manager_->compute_tp());
}

Status Query::get_est_result_size_nullable(
    const char* name, uint64_t* size_val, uint64_t* size_validity) {
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Operation currently "
        "only supported for read queries"));
  }

  if (name == nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Name cannot be null"));
  }

  if (!array_schema_->attribute(name)) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Nullable API is only"
        "applicable to attributes"));
  }

  if (!array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get estimated result size; Input attribute '") +
        name + "' is not nullable"));
  }

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return logger_->status(
          Status_QueryError("Error in query estimate result size; remote "
                            "array with no rest client."));
    }

    return logger_->status(
        Status_QueryError("Error in query estimate result size; unimplemented "
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
  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Operation currently "
        "only supported for read queries"));
  }

  if (!array_schema_->attribute(name)) {
    return logger_->status(Status_QueryError(
        "Cannot get estimated result size; Nullable API is only"
        "applicable to attributes"));
  }

  if (!array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get estimated result size; Input attribute '") +
        name + "' is not nullable"));
  }

  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr) {
      return logger_->status(
          Status_QueryError("Error in query estimate result size; remote "
                            "array with no rest client."));
    }

    return logger_->status(
        Status_QueryError("Error in query estimate result size; unimplemented "
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
  if (type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(
        Status_QueryError("Cannot get number of fragments; Applicable only to "
                          "WRITE and MODIFY_EXCLUSIVE mode"));
  }

  *num = (uint32_t)written_fragment_info_.size();

  return Status::Ok();
}

Status Query::get_written_fragment_uri(uint32_t idx, const char** uri) const {
  if (type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(
        Status_QueryError("Cannot get fragment URI; Applicable only to WRITE "
                          "and MODIFY_EXCLUSIVE mode"));
  }

  auto num = (uint32_t)written_fragment_info_.size();
  if (idx >= num)
    return logger_->status(
        Status_QueryError("Cannot get fragment URI; Invalid fragment index"));

  *uri = written_fragment_info_[idx].uri_.c_str();

  return Status::Ok();
}

Status Query::get_written_fragment_timestamp_range(
    uint32_t idx, uint64_t* t1, uint64_t* t2) const {
  if (type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(
        Status_QueryError("Cannot get fragment timestamp range; Applicable "
                          "only to WRITE and MODIFY_EXCLSUIVE mode"));
  }

  auto num = (uint32_t)written_fragment_info_.size();
  if (idx >= num)
    return logger_->status(Status_QueryError(
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

const ArraySchema& Query::array_schema() const {
  return *(array_schema_.get());
}

const std::shared_ptr<const ArraySchema> Query::array_schema_shared() const {
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
  if (coords_info_.coords_buffer_) {
    ret.push_back(constants::coords);
  }

  return ret;
}

QueryBuffer Query::buffer(const std::string& name) const {
  // Special zipped coordinates
  if ((type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      name == constants::coords) {
    return QueryBuffer(
        coords_info_.coords_buffer_,
        nullptr,
        coords_info_.coords_buffer_size_,
        nullptr);
  }

  // Attribute or dimension
  auto buf = buffers_.find(name);
  if (buf != buffers_.end()) {
    return buf->second;
  }

  // Named buffer does not exist
  return QueryBuffer{};
}

Status Query::finalize() {
  if (status_ == QueryStatus::UNINITIALIZED) {
    return Status::Ok();
  }

  if (array_->is_remote()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return logger_->status(Status_QueryError(
          "Error in query finalize; remote array with no rest client."));

    if (type_ == QueryType::WRITE && layout_ == Layout::GLOBAL_ORDER) {
      return logger_->status(Status_QueryError(
          "Error in query finalize; remote global order writes are only "
          "allowed to call submit_and_finalize to submit the last tile"));
    }
    return rest_client->finalize_query_to_rest(array_->array_uri(), this);
  }

  RETURN_NOT_OK(strategy_->finalize());
  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
}

Status Query::submit_and_finalize() {
  if (type_ != QueryType::WRITE || layout_ != Layout::GLOBAL_ORDER) {
    return logger_->status(
        Status_QueryError("Error in query submit_and_finalize; Call valid only "
                          "in global_order writes."));
  }

  // Check attribute/dimensions buffers completeness before query submits
  RETURN_NOT_OK(check_buffers_correctness());

  if (array_->is_remote()) {
    auto rest_client = storage_manager_->rest_client();
    if (rest_client == nullptr)
      return logger_->status(
          Status_QueryError("Error in query submit_and_finalize; remote array "
                            "with no rest client."));

    if (status_ == QueryStatus::UNINITIALIZED) {
      RETURN_NOT_OK(create_strategy());
    }
    return rest_client->submit_and_finalize_query_to_rest(
        array_->array_uri(), this);
  }

  RETURN_NOT_OK(init());
  RETURN_NOT_OK(storage_manager_->query_submit(this));

  RETURN_NOT_OK(strategy_->finalize());
  status_ = QueryStatus::COMPLETED;

  return Status::Ok();
}

Status Query::check_tile_alignment() const {
  // Only applicable for remote global order writes
  if (!array_->is_remote() || type_ != QueryType::WRITE ||
      layout_ != Layout::GLOBAL_ORDER) {
    return Status::Ok();
  }

  // It is enough to check for the first attr/dim only as we have
  // previously checked in check_buffer_sizes that all the buffers
  // have the same size
  auto& first_buffer_name = buffers_.begin()->first;
  auto& first_buffer = buffers_.begin()->second;
  const bool is_var_size = array_schema_->var_size(first_buffer_name);

  uint64_t cell_num_per_tile = array_schema_->dense() ?
                                   array_schema_->domain().cell_num_per_tile() :
                                   array_schema_->capacity();
  bool buffers_tile_aligned = true;
  if (is_var_size) {
    auto offsets_buf_size = *first_buffer.buffer_size_;
    if ((offsets_buf_size / constants::cell_var_offset_size) %
        cell_num_per_tile) {
      buffers_tile_aligned = false;
    }
  } else {
    uint64_t cell_size = array_schema_->cell_size(first_buffer_name);
    if ((*first_buffer.buffer_size_ / cell_size) % cell_num_per_tile) {
      buffers_tile_aligned = false;
    }
  }

  if (!buffers_tile_aligned) {
    return Status_WriterError(
        "Tile alignment check failed; Input buffers need to be tile-aligned "
        "for remote global order writes.");
  }

  return Status::Ok();
}

Status Query::check_buffer_multipart_size() const {
  // Only applicable for remote global order writes
  if (!array_->is_remote() || type_ != QueryType::WRITE ||
      layout_ != Layout::GLOBAL_ORDER) {
    return Status::Ok();
  }

  const uint64_t s3_multipart_min_limit = 5242880;  // 5MB
  for (const auto& it : buffers_) {
    const auto& buf_name = it.first;
    const bool is_var = array_schema_->var_size(buf_name);
    const uint64_t buffer_size =
        is_var ? *it.second.buffer_var_size_ : *it.second.buffer_size_;

    uint64_t buffer_validity_size = s3_multipart_min_limit;
    if (array_schema_->is_nullable(buf_name)) {
      buffer_validity_size = *it.second.validity_vector_.buffer_size();
    }

    if (buffer_size < s3_multipart_min_limit ||
        buffer_validity_size < s3_multipart_min_limit) {
      return logger_->status(Status_WriterError(
          "Remote global order writes require buffers to be of"
          "minimum 5MB in size."));
    }
  }

  return Status::Ok();
}

Status Query::get_buffer(
    const char* name, void** buffer, uint64_t** buffer_size) const {
  // Check attribute
  if (name != constants::coords) {
    if (array_schema_->attribute(name) == nullptr &&
        array_schema_->dimension_ptr(name) == nullptr)
      return logger_->status(Status_QueryError(
          std::string("Cannot get buffer; Invalid attribute/dimension name '") +
          name + "'"));
  }
  if (array_schema_->var_size(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is var-sized"));

  return get_data_buffer(name, buffer, buffer_size);
}

Status Query::get_buffer(
    const char* name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) const {
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check attribute
  if (name == constants::coords) {
    return logger_->status(
        Status_QueryError("Cannot get buffer; Coordinates are not var-sized"));
  }
  if (array_schema_->attribute(name) == nullptr &&
      array_schema_->dimension_ptr(name) == nullptr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; Invalid attribute/dimension name '") +
        name + "'"));
  }
  if (!array_schema_->var_size(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is fixed-sized"));
  }

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
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check attribute
  if (name == constants::coords) {
    return logger_->status(
        Status_QueryError("Cannot get buffer; Coordinates are not var-sized"));
  }
  if (array_schema_->attribute(name) == nullptr &&
      array_schema_->dimension_ptr(name) == nullptr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; Invalid attribute/dimension name '") +
        name + "'"));
  }
  if (!array_schema_->var_size(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is fixed-sized"));
  }

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
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check attribute
  if (name != constants::coords) {
    if (array_schema_->attribute(name) == nullptr &&
        array_schema_->dimension_ptr(name) == nullptr)
      return logger_->status(Status_QueryError(
          std::string("Cannot get buffer; Invalid attribute/dimension name '") +
          name + "'"));
  }

  // Special zipped coordinates
  if ((type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      name == constants::coords) {
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

void Query::get_label_data_buffer(
    const std::string& name, void** buffer, uint64_t** buffer_size) const {
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE) {
    throw StatusException(
        Status_QueryError("Cannot get buffer; Unsupported query type."));
  }

  // Check that dimension label exists
  if (!array_schema_->is_dim_label(name)) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer; Invalid dimension label '") + name +
        "'"));
  }

  // Return the buffer
  auto it = label_buffers_.find(name);
  if (it != label_buffers_.end()) {
    if (array_schema_->dimension_label_reference(name).is_var()) {
      *buffer = it->second.buffer_var_;
      *buffer_size = it->second.buffer_var_size_;
    } else {
      *buffer = it->second.buffer_;
      *buffer_size = it->second.buffer_size_;
    }
    return;
  }

  // Named buffer does not exist
  *buffer = nullptr;
  *buffer_size = nullptr;
}

void Query::get_label_offsets_buffer(
    const std::string& name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size) const {
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE) {
    throw StatusException(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check that dimension label exists
  if (!array_schema_->is_dim_label(name)) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer; Invalid dimension label '") + name +
        "'"));
  }

  // Error if it is fixed-sized
  if (!array_schema_->dimension_label_reference(name).is_var()) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));
  }

  // Attribute or dimension
  auto it = label_buffers_.find(name);
  if (it != label_buffers_.end()) {
    *buffer_off = (uint64_t*)it->second.buffer_;
    *buffer_off_size = it->second.buffer_size_;
    return;
  }

  // Named buffer does not exist
  *buffer_off = nullptr;
  *buffer_off_size = nullptr;
}

Status Query::get_validity_buffer(
    const char* name,
    uint8_t** buffer_validity_bytemap,
    uint64_t** buffer_validity_bytemap_size) const {
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check attribute
  if (!array_schema_->is_nullable(name))
    return logger_->status(Status_QueryError(
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
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check nullable attribute
  if (array_schema_->attribute(name) == nullptr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; Invalid attribute name '") + name +
        "'"));
  }
  if (array_schema_->var_size(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is var-sized"));
  }
  if (!array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is non-nullable"));
  }

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
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot get buffer; Unsupported query type."));
  }

  // Check attribute
  if (array_schema_->attribute(name) == nullptr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; Invalid attribute name '") + name +
        "'"));
  }
  if (!array_schema_->var_size(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is fixed-sized"));
  }
  if (!array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot get buffer; '") + name + "' is non-nullable"));
  }

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
  if (status_ == QueryStatus::UNINITIALIZED || type_ != QueryType::READ) {
    return false;
  }

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
      return logger_->status(Status_QueryError(
          "Cannot init query; The associated array is not open"));

    // Check if the array got re-opened with a different query type
    QueryType array_query_type{array_->get_query_type()};
    if (array_query_type != type_) {
      std::stringstream errmsg;
      errmsg << "Cannot init query; "
             << "Associated array query type does not match query type: "
             << "(" << query_type_str(array_query_type)
             << " != " << query_type_str(type_) << ")";
      return logger_->status(Status_QueryError(errmsg.str()));
    }

    RETURN_NOT_OK(check_buffer_names());

    // Cache if dimension label queries are needed.
    bool uses_labels = uses_dimension_labels();
    bool only_labels = uses_labels && only_dim_label_query();

    // Create dimension label queries and remove labels from subarray.
    if (uses_labels) {
      // Initialize the dimension label queries.
      dim_label_queries_ = tdb_unique_ptr<ArrayDimensionLabelQueries>(tdb_new(
          ArrayDimensionLabelQueries,
          storage_manager_,
          array_,
          subarray_,
          label_buffers_,
          buffers_,
          fragment_name_));

      // Clear the label ranges from the subarray. They are no longer needed.
      subarray_.remove_label_ranges();

      // Do not allow reading data from dimension labels for sparse arrays
      // unless we are only returning the dimension label data. If returning
      // data for the entire sparse array, the output for the dimension label
      // data needs to be re-arranged to match the coordinate form we use for
      // sparse reads. This has not yet been implemented.
      if (!only_labels && type_ == QueryType::READ && !array_schema_->dense() &&
          dim_label_queries_->has_data_query()) {
        return logger_->status(Status_QueryError(
            "Cannot initialize query; Reading dimension label data is not yet "
            "supported on sparse arrays."));
      }
    }

    // Create the query strategy if possible. May need to wait for range queries
    // to be completed and for the subarray to be updated.
    if (!uses_labels ||
        (!only_labels && dim_label_queries_->completed_range_queries())) {
      RETURN_NOT_OK(create_strategy());
    }
  }

  status_ = QueryStatus::INPROGRESS;

  return Status::Ok();
}

URI Query::first_fragment_uri() const {
  if (type_ != QueryType::READ || fragment_metadata_.empty()) {
    return URI();
  }

  return fragment_metadata_.front()->fragment_uri();
}

URI Query::last_fragment_uri() const {
  if (type_ != QueryType::READ || fragment_metadata_.empty()) {
    return URI();
  }

  return fragment_metadata_.back()->fragment_uri();
}

Layout Query::layout() const {
  return layout_;
}

const QueryCondition& Query::condition() const {
  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) {
    throw std::runtime_error(
        "Query condition is not available for write or modify exclusive "
        "queries");
  }

  return condition_;
}

const std::vector<UpdateValue>& Query::update_values() const {
  return update_values_;
}

Status Query::cancel() {
  status_ = QueryStatus::FAILED;
  return Status::Ok();
}

Status Query::process() {
  if (status_ == QueryStatus::UNINITIALIZED)
    return logger_->status(
        Status_QueryError("Cannot process query; Query is not initialized"));
  status_ = QueryStatus::INPROGRESS;

  // Check if we need to process label ranges and update subarray before
  // continuing to the main query.
  if (dim_label_queries_ && dim_label_queries_->completed_range_queries()) {
    // Process the dimension label queries.
    dim_label_queries_->process_range_queries(subarray_);

    // The dimension label query did not complete. For now, we are failing on
    // this step. In the future, this may be updated to allow incomplete
    // dimension label queries.
    if (!dim_label_queries_->completed_range_queries()) {
      status_ = QueryStatus::FAILED;
      return logger_->status(
          Status_QueryError("Cannot process query; Failed to read data from "
                            "dimension label ranges."));
    }

    // Check if the subarray has a dimension with no data.
    // Note: For now we are returning an error if no dimension label data was
    // found on a range. In the future, we may want to update this to instead
    // finalize the query as returning no results.
    for (uint32_t dim_idx{0}; dim_idx < subarray_.dim_num(); ++dim_idx) {
      if (subarray_.empty(dim_idx)) {
        return logger_->status(Status_QueryError(
            "Cannot process query; No range set on dimension " +
            std::to_string(dim_idx) + " after update dimension label ranges."));
      }
    }

    // Label queries are completed and the subarray is fully updated. We can
    // create the query strategy now.
    if (!only_dim_label_query()) {
      RETURN_NOT_OK(create_strategy());
    }
  }

  // Update query status.
  status_ = QueryStatus::INPROGRESS;

  // Process query
  Status st{Status::Ok()};
  if (!only_dim_label_query()) {
    st = strategy_->dowork();
  }

  // Process dimension label queries
  if (dim_label_queries_) {
    dim_label_queries_->process_data_queries();
  }

  // Handle error
  if (!st.ok()) {
    status_ = QueryStatus::FAILED;
    return st;
  }

  // Check if the query is completed or not.
  if ((only_dim_label_query() || !strategy_->incomplete()) &&
      (!dim_label_queries_ || dim_label_queries_->completed())) {
    // Main query and dimension label query are both completed. Handle the
    // callback, then set status to complete.
    if (callback_ != nullptr) {
      callback_(callback_data_);
    }
    status_ = QueryStatus::COMPLETED;
  } else {
    // Either the main query or the dimension lable query are incomplete.
    status_ = QueryStatus::INCOMPLETE;
  }

  return Status::Ok();
}  // namespace sm

IQueryStrategy* Query::strategy(bool skip_checks_serialization) {
  if (strategy_ == nullptr) {
    throw_if_not_ok(create_strategy(skip_checks_serialization));
  }
  return strategy_.get();
}

Status Query::reset_strategy_with_layout(
    Layout layout, bool force_legacy_reader) {
  force_legacy_reader_ = force_legacy_reader;
  if (strategy_ != nullptr) {
    dynamic_cast<StrategyBase*>(strategy_.get())->stats()->reset();
    strategy_ = nullptr;
  }
  layout_ = layout;
  subarray_.set_layout(layout);
  RETURN_NOT_OK(create_strategy(true));

  return Status::Ok();
}

bool Query::uses_dimension_labels() const {
  return !label_buffers_.empty() || subarray_.has_label_ranges() ||
         dim_label_queries_;
}

Status Query::disable_checks_consolidation() {
  if (status_ != QueryStatus::UNINITIALIZED) {
    return logger_->status(Status_QueryError(
        "Cannot disable checks for consolidation after initialization"));
  }

  if (type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(
        Status_QueryError("Cannot disable checks for consolidation; Applicable "
                          "only to write and modify_exclusive"));
  }

  disable_checks_consolidation_ = true;
  return Status::Ok();
}

Status Query::set_consolidation_with_timestamps() {
  if (status_ != QueryStatus::UNINITIALIZED) {
    return logger_->status(Status_QueryError(
        "Cannot enable consolidation with timestamps after initialization"));
  }

  if (type_ != QueryType::READ) {
    return logger_->status(Status_QueryError(
        "Cannot enable consolidation with timestamps; Applicable only to "
        "reads"));
  }

  consolidation_with_timestamps_ = true;
  return Status::Ok();
}

void Query::set_processed_conditions(
    std::vector<std::string>& processed_conditions) {
  processed_conditions_ = processed_conditions;
}

Status Query::check_buffer_names() {
  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) {
    // If the array is sparse, the coordinates must be provided
    if (!array_schema_->dense() && !coords_info_.has_coords_) {
      return logger_->status(
          Status_QueryError("Sparse array writes expect the coordinates of the "
                            "cells to be written"));
    }

    // If the layout is unordered, the coordinates must be provided
    if (layout_ == Layout::UNORDERED && !coords_info_.has_coords_) {
      return logger_->status(
          Status_QueryError("Unordered writes expect the coordinates of the "
                            "cells to be written"));
    }

    // All attributes/dimensions must be provided unless this query is only for
    // dimension labels.
    if (!only_dim_label_query()) {
      auto expected_num = array_schema_->attribute_num();
      expected_num += static_cast<decltype(expected_num)>(
          buffers_.count(constants::timestamps));
      expected_num += static_cast<decltype(expected_num)>(
          buffers_.count(constants::delete_timestamps));
      expected_num += static_cast<decltype(expected_num)>(
          buffers_.count(constants::delete_condition_index));
      expected_num += (coord_buffer_is_set_ || coord_data_buffer_is_set_ ||
                       coord_offsets_buffer_is_set_) ?
                          array_schema_->dim_num() :
                          0;
      if (buffers_.size() != expected_num) {
        return logger_->status(Status_QueryError(
            "Writes expect all attributes (and coordinates in "
            "the sparse/unordered case) to be set"));
      }
    }
  }

  return Status::Ok();
}

Status Query::create_strategy(bool skip_checks_serialization) {
  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) {
    if (layout_ == Layout::COL_MAJOR || layout_ == Layout::ROW_MAJOR) {
      if (!array_schema_->dense()) {
        return Status_QueryError(
            "Cannot create strategy; sparse writes do not support layout " +
            layout_str(layout_));
      }
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          OrderedWriter,
          stats_->create_child("Writer"),
          logger_,
          storage_manager_,
          array_,
          config_,
          buffers_,
          subarray_,
          layout_,
          written_fragment_info_,
          coords_info_,
          remote_query_,
          fragment_name_,
          skip_checks_serialization));
    } else if (layout_ == Layout::UNORDERED) {
      if (array_schema_->dense()) {
        return Status_QueryError(
            "Cannot create strategy; dense writes do not support layout " +
            layout_str(layout_));
      }
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          UnorderedWriter,
          stats_->create_child("Writer"),
          logger_,
          storage_manager_,
          array_,
          config_,
          buffers_,
          subarray_,
          layout_,
          written_fragment_info_,
          coords_info_,
          remote_query_,
          fragment_name_,
          skip_checks_serialization));
    } else if (layout_ == Layout::GLOBAL_ORDER) {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          GlobalOrderWriter,
          stats_->create_child("Writer"),
          logger_,
          storage_manager_,
          array_,
          config_,
          buffers_,
          subarray_,
          layout_,
          written_fragment_info_,
          disable_checks_consolidation_,
          processed_conditions_,
          coords_info_,
          remote_query_,
          fragment_name_,
          skip_checks_serialization));
    } else {
      return Status_QueryError(
          "Cannot create strategy; unsupported layout " + layout_str(layout_));
    }
  } else if (type_ == QueryType::READ) {
    bool use_default = true;
    bool all_dense = true;
    for (auto& frag_md : fragment_metadata_) {
      all_dense &= frag_md->dense();
    }

    if (use_refactored_sparse_unordered_with_dups_reader(
            layout_, *array_schema_)) {
      use_default = false;

      auto&& [st, non_overlapping_ranges]{Query::non_overlapping_ranges()};
      RETURN_NOT_OK(st);

      if (*non_overlapping_ranges || !subarray_.is_set() ||
          subarray_.range_num() == 1) {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseUnorderedWithDupsReader<uint8_t>,
            stats_->create_child("Reader"),
            logger_,
            storage_manager_,
            array_,
            config_,
            buffers_,
            subarray_,
            layout_,
            condition_,
            skip_checks_serialization));
      } else {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseUnorderedWithDupsReader<uint64_t>,
            stats_->create_child("Reader"),
            logger_,
            storage_manager_,
            array_,
            config_,
            buffers_,
            subarray_,
            layout_,
            condition_,
            skip_checks_serialization));
      }
    } else if (
        use_refactored_sparse_global_order_reader(layout_, *array_schema_) &&
        !array_schema_->dense() &&
        (layout_ == Layout::GLOBAL_ORDER || layout_ == Layout::UNORDERED)) {
      // Using the reader for unordered queries to do deduplication.
      use_default = false;

      auto&& [st, non_overlapping_ranges]{Query::non_overlapping_ranges()};
      RETURN_NOT_OK(st);

      if (*non_overlapping_ranges || !subarray_.is_set() ||
          subarray_.range_num() == 1) {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseGlobalOrderReader<uint8_t>,
            stats_->create_child("Reader"),
            logger_,
            storage_manager_,
            array_,
            config_,
            buffers_,
            subarray_,
            layout_,
            condition_,
            consolidation_with_timestamps_,
            skip_checks_serialization));
      } else {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseGlobalOrderReader<uint64_t>,
            stats_->create_child("Reader"),
            logger_,
            storage_manager_,
            array_,
            config_,
            buffers_,
            subarray_,
            layout_,
            condition_,
            consolidation_with_timestamps_,
            skip_checks_serialization));
      }
    } else if (use_refactored_dense_reader(*array_schema_, all_dense)) {
      use_default = false;
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          DenseReader,
          stats_->create_child("Reader"),
          logger_,
          storage_manager_,
          array_,
          config_,
          buffers_,
          subarray_,
          layout_,
          condition_,
          skip_checks_serialization));
    }

    if (use_default) {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          Reader,
          stats_->create_child("Reader"),
          logger_,
          storage_manager_,
          array_,
          config_,
          buffers_,
          subarray_,
          layout_,
          condition_,
          skip_checks_serialization));
    }
  } else if (type_ == QueryType::DELETE) {
    strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
        Deletes,
        stats_->create_child("Deletes"),
        logger_,
        storage_manager_,
        array_,
        config_,
        buffers_,
        subarray_,
        layout_,
        condition_,
        skip_checks_serialization));
  } else {
    return logger_->status(
        Status_QueryError("Cannot create strategy; unsupported query type"));
  }

  if (strategy_ == nullptr)
    return logger_->status(
        Status_QueryError("Cannot create strategy; allocation failed"));

  return Status::Ok();
}

Status Query::check_set_fixed_buffer(const std::string& name) {
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot set buffer; Unsupported query type."));
  }

  if (name == constants::coords &&
      !array_schema_->domain().all_dims_same_type())
    return logger_->status(Status_QueryError(
        "Cannot set buffer; Setting a buffer for zipped coordinates is not "
        "applicable to heterogeneous domains"));

  if (name == constants::coords && !array_schema_->domain().all_dims_fixed())
    return logger_->status(Status_QueryError(
        "Cannot set buffer; Setting a buffer for zipped coordinates is not "
        "applicable to domains with variable-sized dimensions"));

  return Status::Ok();
}

Status Query::set_config(const Config& config) {
  config_ = config;

  // Refresh memory budget configuration.
  if (strategy_ != nullptr)
    RETURN_NOT_OK(strategy_->initialize_memory_budget());

  // Set subarray's config for backwards compatibility
  // Users expect the query config to effect the subarray based on existing
  // behavior before subarray was exposed directly
  subarray_.set_config(config_);

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
    return logger_->status(
        Status_QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_size == nullptr)
    return logger_->status(
        Status_QueryError("Cannot set buffer; " + name + " buffer is null"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (name != constants::coords && !is_dim && !is_attr)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Must not be nullable
  if (array_schema_->is_nullable(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is nullable"));

  // Check that attribute/dimension is fixed-sized
  const bool var_size =
      (name != constants::coords && array_schema_->var_size(name));
  if (var_size)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is var-sized"));

  // Check if zipped coordinates coexist with separate coordinate buffers
  if ((is_dim && has_zipped_coords_buffer_) ||
      (name == constants::coords && has_coords_buffer_))
    return logger_->status(Status_QueryError(
        std::string("Cannot set separate coordinate buffers and "
                    "a zipped coordinate buffer in the same query")));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (name == constants::coords) {
    has_zipped_coords_buffer_ = true;

    // Set special function for zipped coordinates buffer
    if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)
      return set_coords_buffer(buffer, buffer_size);
  }

  if (is_dim &&
      (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_size / array_schema_->cell_size(name);
    if (coord_buffer_is_set_ && coords_num != coords_info_.coords_num_)
      return logger_->status(Status_QueryError(
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
  buffers_[name].set_data_buffer(buffer, buffer_size);

  return Status::Ok();
}

void Query::set_dimension_label_buffer(
    const std::string& name, const QueryBuffer& buffer) {
  if (buffer.buffer_var_ || buffer.buffer_var_size_) {
    // Variable-length buffer. Set data buffer and offsets buffer.
    throw_if_not_ok(
        set_data_buffer(name, buffer.buffer_var_, buffer.buffer_var_size_));
    throw_if_not_ok(set_offsets_buffer(
        name, static_cast<uint64_t*>(buffer.buffer_), buffer.buffer_size_));
  } else {
    // Fixed-length buffer. Set data buffer only.
    throw_if_not_ok(set_data_buffer(name, buffer.buffer_, buffer.buffer_size_));
  }
}

Status Query::set_data_buffer(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check buffer
  if (check_null_buffers && buffer == nullptr) {
    if ((type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) ||
        *buffer_size != 0) {
      return logger_->status(
          Status_QueryError("Cannot set buffer; " + name + " buffer is null"));
    }
  }

  // Check buffer size
  if (check_null_buffers && buffer_size == nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));
  }

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (!ArraySchema::is_special_attribute(name) && !is_dim && !is_attr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));
  }

  if (array_schema_->dense() &&
      (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      !is_attr) {
    return logger_->status(Status_QueryError(
        std::string("Dense write queries cannot set dimension buffers")));
  }

  // Check if zipped coordinates coexist with separate coordinate buffers
  if ((is_dim && has_zipped_coords_buffer_) ||
      (name == constants::coords && has_coords_buffer_)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set separate coordinate buffers and "
                    "a zipped coordinate buffer in the same query")));
  }

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));
  }

  if (name == constants::coords) {
    has_zipped_coords_buffer_ = true;

    // Set special function for zipped coordinates buffer
    if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)
      return set_coords_buffer(buffer, buffer_size);
  }

  if (is_dim &&
      (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_size / array_schema_->cell_size(name);
    if (coord_data_buffer_is_set_ && coords_num != coords_info_.coords_num_ &&
        name == data_buffer_name_)
      return logger_->status(Status_QueryError(
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

void Query::set_label_data_buffer(
    const std::string& name,
    void* const buffer,
    uint64_t* const buffer_size,
    const bool check_null_buffers) {
  // Check the query type is valid.
  if (type_ != QueryType::READ && type_ != QueryType::WRITE) {
    throw StatusException(Status_SerializationError(
        "Cannot set buffer; Unsupported query type."));
  }

  // Check buffer and buffer size.
  if (check_null_buffers) {
    if (buffer == nullptr && (type_ != QueryType::WRITE || *buffer_size != 0)) {
      throw StatusException(
          Status_QueryError("Cannot set buffer; " + name + " buffer is null"));
    }
    if (buffer_size == nullptr) {
      throw StatusException(Status_QueryError(
          "Cannot set buffer; " + name + " buffer size is null"));
    }
  }

  // Check that dimension label exists.
  if (!array_schema_->is_dim_label(name)) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer; Invalid dimension label '") + name +
        "'"));
  }

  // Error if setting a new dimension label after initialization.
  if (status_ != QueryStatus::UNINITIALIZED) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer for new dimension label '") + name +
        "' after initialization"));
  }

  // Set dimension label buffer on the appropriate buffer depending if the label
  // is fixed or variable length.
  array_schema_->dimension_label_reference(name).is_var() ?
      label_buffers_[name].set_data_var_buffer(buffer, buffer_size) :
      label_buffers_[name].set_data_buffer(buffer, buffer_size);
}

void Query::set_label_offsets_buffer(
    const std::string& name,
    uint64_t* const buffer_offsets,
    uint64_t* const buffer_offsets_size,
    const bool check_null_buffers) {
  // Check the query type is valid.
  if (type_ != QueryType::READ && type_ != QueryType::WRITE) {
    throw StatusException(Status_SerializationError(
        "Cannot set buffer; Unsupported query type."));
  }

  // Check for nullptrs.
  if (check_null_buffers) {
    if (buffer_offsets == nullptr) {
      throw StatusException(
          Status_QueryError("Cannot set buffer; " + name + " buffer is null"));
    }
    // Check buffer size
    if (buffer_offsets_size == nullptr) {
      throw StatusException(Status_QueryError(
          "Cannot set buffer; " + name + " buffer size is null"));
    }
  }

  // Check that dimension label exists.
  if (!array_schema_->is_dim_label(name)) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer; Invalid dimension label '") + name +
        "'"));
  }

  // Check the dimension labe is in fact variable length.
  if (!array_schema_->dimension_label_reference(name).is_var()) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));
  }

  // Check the query was not already initialized.
  if (status_ != QueryStatus::UNINITIALIZED) {
    throw StatusException(Status_QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));
  }

  // Set dimension label offsets buffers.
  label_buffers_[name].set_offsets_buffer(buffer_offsets, buffer_offsets_size);
}

Status Query::set_offsets_buffer(
    const std::string& name,
    uint64_t* const buffer_offsets,
    uint64_t* const buffer_offsets_size,
    const bool check_null_buffers) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check buffer
  if (check_null_buffers && buffer_offsets == nullptr)
    return logger_->status(
        Status_QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_offsets_size == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Neither a dimension nor an attribute
  if (!is_dim && !is_attr)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Invalid buffer name '") + name +
        "' (it should be an attribute or dimension)"));

  // Error if it is fixed-sized
  if (!array_schema_->var_size(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute/dimension after initialization
  bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (is_dim &&
      (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)) {
    // Check number of coordinates
    uint64_t coords_num =
        *buffer_offsets_size / constants::cell_var_offset_size;
    if (coord_offsets_buffer_is_set_ &&
        coords_num != coords_info_.coords_num_ && name == offsets_buffer_name_)
      return logger_->status(Status_QueryError(
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
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));

  // Check validity buffer size
  if (check_null_buffers && validity_vector.buffer_size() == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));

  // Must be an attribute
  if (!array_schema_->is_attr(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));

  // Must be nullable
  if (!array_schema_->is_nullable(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));

  // Error if setting a new attribute after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return logger_->status(Status_QueryError(
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
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot set buffer; Unsupported query type."));
  }

  // Check buffer
  if (check_null_buffers && buffer_val == nullptr)
    if ((type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) ||
        *buffer_val_size != 0)
      return logger_->status(
          Status_QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_val_size == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Check offset buffer
  if (check_null_buffers && buffer_off == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " offset buffer is null"));

  // Check offset buffer size
  if (check_null_buffers && buffer_off_size == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " offset buffer size is null"));

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (!is_dim && !is_attr)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension '") + name +
        "'"));

  // Must not be nullable
  if (array_schema_->is_nullable(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is nullable"));

  // Check that attribute/dimension is var-sized
  if (!array_schema_->var_size(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));

  if (is_dim &&
      (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)) {
    // Check number of coordinates
    uint64_t coords_num = *buffer_off_size / constants::cell_var_offset_size;
    if (coord_buffer_is_set_ && coords_num != coords_info_.coords_num_)
      return logger_->status(Status_QueryError(
          std::string("Cannot set buffer; Input buffer for dimension '") +
          name +
          "' has a different number of coordinates than previously "
          "set coordinate buffers"));

    coords_info_.coords_num_ = coords_num;
    coord_buffer_is_set_ = true;
    coords_info_.has_coords_ = true;
  }

  // Set attribute/dimension buffer
  buffers_[name].set_data_var_buffer(buffer_val, buffer_val_size);
  buffers_[name].set_offsets_buffer(buffer_off, buffer_off_size);

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
    return logger_->status(
        Status_QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_size == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Check validity buffer offset
  if (check_null_buffers && validity_vector.buffer() == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));

  // Check validity buffer size
  if (check_null_buffers && validity_vector.buffer_size() == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));

  // Must be an attribute
  if (!array_schema_->is_attr(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));

  // Must be fixed-size
  if (array_schema_->var_size(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is var-sized"));

  // Must be nullable
  if (!array_schema_->is_nullable(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute buffer
  buffers_[name].set_data_buffer(buffer, buffer_size);
  buffers_[name].set_validity_buffer(std::move(validity_vector));

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
  // Check query type
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot set layout; Unsupported query type."));
  }

  // Check buffer
  if (check_null_buffers && buffer_val == nullptr)
    if ((type_ != QueryType::WRITE && type_ != QueryType::MODIFY_EXCLUSIVE) ||
        *buffer_val_size != 0)
      return logger_->status(
          Status_QueryError("Cannot set buffer; " + name + " buffer is null"));

  // Check buffer size
  if (check_null_buffers && buffer_val_size == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));

  // Check buffer offset
  if (check_null_buffers && buffer_off == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " offset buffer is null"));

  // Check buffer offset size
  if (check_null_buffers && buffer_off_size == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " offset buffer size is null"));
  ;

  // Check validity buffer offset
  if (check_null_buffers && validity_vector.buffer() == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));

  // Check validity buffer size
  if (check_null_buffers && validity_vector.buffer_size() == nullptr)
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));

  // Must be an attribute
  if (!array_schema_->is_attr(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));

  // Must be var-size
  if (!array_schema_->var_size(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is fixed-sized"));

  // Must be nullable
  if (!array_schema_->is_nullable(name))
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));

  // Error if setting a new attribute after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists)
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));

  // Set attribute/dimension buffer
  buffers_[name].set_data_var_buffer(buffer_val, buffer_val_size);
  buffers_[name].set_offsets_buffer(buffer_off, buffer_off_size);
  buffers_[name].set_validity_buffer(std::move(validity_vector));

  return Status::Ok();
}

Status Query::set_est_result_size(
    std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
    std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size) {
  if (type_ != QueryType::READ) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot set estimated result size; Unsupported query type."));
  }

  return subarray_.set_est_result_size(est_result_size, max_mem_size);
}

Status Query::set_layout(Layout layout) {
  switch (type_) {
    case (QueryType::READ):
      if (status_ != QueryStatus::UNINITIALIZED) {
        return logger_->status(
            Status_QueryError("Cannot set layout after initialization"));
      }

      break;

    case (QueryType::WRITE):
    case (QueryType::MODIFY_EXCLUSIVE):
      if (array_schema_->dense()) {
        // Check layout for dense writes is valid.
        if (layout == Layout::UNORDERED) {
          return logger_->status(Status_QueryError(
              "Unordered writes are only possible for sparse arrays"));
        }
      } else {
        // Check layout for sparse writes is valid.
        if (layout == Layout::ROW_MAJOR || layout == Layout::COL_MAJOR) {
          return logger_->status(
              Status_QueryError("Row-major and column-major writes are only "
                                "possible for dense arrays"));
        }
      }

      break;

    default:
      return LOG_STATUS(Status_SerializationError(
          "Cannot set layout; Unsupported query type."));
  }

  if (layout == Layout::HILBERT) {
    return logger_->status(Status_QueryError(
        "Cannot set layout; Hilbert order is not applicable to queries"));
  }

  layout_ = layout;
  subarray_.set_layout(layout);
  return Status::Ok();
}

Status Query::set_condition(const QueryCondition& condition) {
  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) {
    return logger_->status(Status_QueryError(
        "Cannot set query condition; Operation not applicable "
        "to write queries"));
  }

  condition_ = condition;
  return Status::Ok();
}

Status Query::add_update_value(
    const char* field_name,
    const void* update_value,
    uint64_t update_value_size) {
  if (type_ != QueryType::UPDATE) {
    return logger_->status(Status_QueryError(
        "Cannot add query update value; Operation only applicable "
        "to update queries"));
  }

  // Make sure the array is sparse.
  if (array_schema_->dense()) {
    return logger_->status(Status_QueryError(
        "Setting update values is only valid for sparse arrays"));
  }

  if (attributes_with_update_value_.count(field_name) != 0) {
    return logger_->status(
        Status_QueryError("Update value already set for attribute"));
  }

  attributes_with_update_value_.emplace(field_name);
  update_values_.emplace_back(field_name, update_value, update_value_size);
  return Status::Ok();
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

Status Query::set_subarray(const void* subarray) {
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot set subarray; Unsupported query type."));
  }

  if (!array_schema_->domain().all_dims_same_type())
    return logger_->status(
        Status_QueryError("Cannot set subarray; Function not applicable to "
                          "heterogeneous domains"));

  if (!array_schema_->domain().all_dims_fixed())
    return logger_->status(
        Status_QueryError("Cannot set subarray; Function not applicable to "
                          "domains with variable-sized dimensions"));

  // Prepare a subarray object
  Subarray sub(array_, layout_, stats_, logger_);
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
        return logger_->status(Status_QueryError(
            "Invalid value " + read_range_oob_str +
            " for sm.read_range_obb. Acceptable values are 'error' or "
            "'warn'."));
      err_on_range_oob = read_range_oob_str == "error";
    }

    for (unsigned d = 0; d < dim_num; ++d) {
      auto r_size{2 * array_schema_->dimension_ptr(d)->coord_size()};
      Range range(&s_ptr[offset], r_size);
      RETURN_NOT_OK(sub.add_range(d, std::move(range), err_on_range_oob));
      offset += r_size;
    }
  }

  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) {
    // Not applicable to sparse arrays
    if (!array_schema_->dense())
      return logger_->status(Status_WriterError(
          "Setting a subarray is not supported in sparse writes"));

    // Subarray must be unary for dense writes
    if (sub.range_num() != 1)
      return logger_->status(
          Status_WriterError("Cannot set subarray; Multi-range dense writes "
                             "are not supported"));

    // When executed from serialization, resetting the strategy will delete
    // the fragment for global order writes. We need to prevent this, thus
    // persist the fragment between remote global order write submits.
    if (strategy_ != nullptr && layout_ != Layout::GLOBAL_ORDER) {
      strategy_->reset();
    }
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

Status Query::set_subarray(const tiledb::sm::Subarray& subarray) {
  auto query_status = status();
  if (query_status != tiledb::sm::QueryStatus::UNINITIALIZED &&
      query_status != tiledb::sm::QueryStatus::COMPLETED) {
    // Can be in this initialized state when query has been de-serialized
    // server-side and are trying to perform local submit.
    // Don't change anything and return indication of success.
    return Status::Ok();
  }

  // Set subarray
  if (!subarray.is_set())
    // Nothing useful to set here, will leave query with its current
    // settings and consider successful.
    return Status::Ok();

  auto prev_layout = subarray_.layout();
  subarray_ = subarray;
  subarray_.set_layout(prev_layout);

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::set_subarray_unsafe(const NDRange& subarray) {
  // Prepare a subarray object
  Subarray sub(array_, layout_, stats_, logger_);
  if (!subarray.empty()) {
    auto dim_num = array_schema_->dim_num();
    for (unsigned d = 0; d < dim_num; ++d)
      RETURN_NOT_OK(sub.add_range_unsafe(d, subarray[d]));
  }

  assert(layout_ == sub.layout());
  subarray_ = sub;

  status_ = QueryStatus::UNINITIALIZED;

  return Status::Ok();
}

Status Query::check_buffers_correctness() {
  if (type_ != QueryType::READ && type_ != QueryType::WRITE &&
      type_ != QueryType::MODIFY_EXCLUSIVE && type_ != QueryType::DELETE) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot check buffers; Unsupported query type."));
  }

  // Iterate through each attribute
  for (auto& attr : buffer_names()) {
    if (array_schema_->var_size(attr)) {
      // Check for data buffer under buffer_var and offsets buffer under
      // buffer
      if (type_ == QueryType::READ) {
        if (buffer(attr).buffer_var_ == nullptr) {
          return logger_->status(Status_QueryError(
              std::string("Var-Sized input attribute/dimension '") + attr +
              "' is not set correctly. \nVar size buffer is not set."));
        }
      } else {
        if (buffer(attr).buffer_var_ == nullptr &&
            *buffer(attr).buffer_var_size_ != 0) {
          return logger_->status(Status_QueryError(
              std::string("Var-Sized input attribute/dimension '") + attr +
              "' is not set correctly. \nVar size buffer is not set and "
              "buffer "
              "size if not 0."));
        }
      }
      if (buffer(attr).buffer_ == nullptr) {
        return logger_->status(Status_QueryError(
            std::string("Var-Sized input attribute/dimension '") + attr +
            "' is not set correctly. \nOffsets buffer is not set."));
      }
    } else {
      // Fixed sized
      if (buffer(attr).buffer_ == nullptr) {
        return logger_->status(Status_QueryError(
            std::string("Fix-Sized input attribute/dimension '") + attr +
            "' is not set correctly. \nData buffer is not set."));
      }
    }
    if (array_schema_->is_nullable(attr)) {
      bool exists_validity = buffer(attr).validity_vector_.buffer() != nullptr;
      if (!exists_validity) {
        return logger_->status(Status_QueryError(
            std::string("Nullable input attribute/dimension '") + attr +
            "' is not set correctly \nValidity buffer is not set"));
      }
    }
  }
  return Status::Ok();
}

bool Query::only_dim_label_query() const {
  // Returns true if all the following are true:
  // 1. At most one dimension buffer is set.
  // 2. No attribute buffers are set.
  // 3. At least one label buffer is set.
  return (
      !label_buffers_.empty() &&
      (buffers_.size() == 0 ||
       (buffers_.size() == 1 &&
        (coord_buffer_is_set_ || coord_data_buffer_is_set_ ||
         coord_offsets_buffer_is_set_))));
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
      return logger_->status(Status_QueryError(
          "Error in query submission; remote array with no rest client."));

    if (status_ == QueryStatus::UNINITIALIZED) {
      RETURN_NOT_OK(create_strategy());
    }

    // Check that input buffers are tile-aligned for remote global order writes
    RETURN_NOT_OK(check_tile_alignment());

    // Check that input buffers >5mb for remote global order writes
    RETURN_NOT_OK(check_buffer_multipart_size());

    RETURN_NOT_OK(rest_client->submit_query_to_rest(array_->array_uri(), this));

    reset_coords_markers();
    return Status::Ok();
  }
  RETURN_NOT_OK(init());
  RETURN_NOT_OK(storage_manager_->query_submit(this));

  reset_coords_markers();
  return Status::Ok();
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
    return logger_->status(
        Status_QueryError("Error in async query submission; async queries not "
                          "supported for remote arrays."));

  callback_ = callback;
  callback_data_ = callback_data;
  return storage_manager_->query_submit_async(this);
}

QueryStatus Query::status() const {
  return status_;
}

QueryStatusDetailsReason Query::status_incomplete_reason() const {
  if (strategy_ != nullptr)
    return strategy_->status_incomplete_reason();

  return QueryStatusDetailsReason::REASON_NONE;
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

shared_ptr<Buffer> Query::rest_scratch() const {
  return rest_scratch_;
}

bool Query::use_refactored_dense_reader(
    const ArraySchema& array_schema, bool all_dense) {
  bool use_refactored_reader = false;
  bool found = false;

  // If the query comes from a client using the legacy reader.
  if (force_legacy_reader_) {
    return false;
  }

  // First check for legacy option
  config_.get<bool>(
      "sm.use_refactored_readers", &use_refactored_reader, &found);
  // If the legacy/deprecated option is set use it over the new parameters
  // This facilitates backwards compatibility
  if (found) {
    logger_->warn(
        "sm.use_refactored_readers config option is deprecated.\nPlease use "
        "'sm.query.dense.reader' with value of 'refactored' or 'legacy'");
  } else {
    const std::string& val = config_.get("sm.query.dense.reader", &found);
    assert(found);
    use_refactored_reader = val == "refactored";
  }

  return use_refactored_reader && array_schema.dense() && all_dense;
}

bool Query::use_refactored_sparse_global_order_reader(
    Layout layout, const ArraySchema& array_schema) {
  bool use_refactored_reader = false;
  bool found = false;

  // If the query comes from a client using the legacy reader.
  if (force_legacy_reader_) {
    return false;
  }

  // First check for legacy option
  config_.get<bool>(
      "sm.use_refactored_readers", &use_refactored_reader, &found);
  // If the legacy/deprecated option is set use it over the new parameters
  // This facilitates backwards compatibility
  if (found) {
    logger_->warn(
        "sm.use_refactored_readers config option is deprecated.\nPlease use "
        "'sm.query.sparse_global_order.reader' with value of 'refactored' or "
        "'legacy'");
  } else {
    const std::string& val =
        config_.get("sm.query.sparse_global_order.reader", &found);
    assert(found);
    use_refactored_reader = val == "refactored";
  }
  return use_refactored_reader && !array_schema.dense() &&
         (layout == Layout::GLOBAL_ORDER || layout == Layout::UNORDERED);
}

bool Query::use_refactored_sparse_unordered_with_dups_reader(
    Layout layout, const ArraySchema& array_schema) {
  bool use_refactored_reader = false;
  bool found = false;

  // If the query comes from a client using the legacy reader.
  if (force_legacy_reader_) {
    return false;
  }

  // First check for legacy option
  config_.get<bool>(
      "sm.use_refactored_readers", &use_refactored_reader, &found);
  // If the legacy/deprecated option is set use it over the new parameters
  // This facilitates backwards compatibility
  if (found) {
    logger_->warn(
        "sm.use_refactored_readers config option is deprecated.\nPlease use "
        "'sm.query.sparse_unordered_with_dups.reader' with value of "
        "'refactored' or 'legacy'");
  } else {
    const std::string& val =
        config_.get("sm.query.sparse_unordered_with_dups.reader", &found);
    assert(found);
    use_refactored_reader = val == "refactored";
  }

  return use_refactored_reader && !array_schema.dense() &&
         layout == Layout::UNORDERED && array_schema.allows_dups();
}

tuple<Status, optional<bool>> Query::non_overlapping_ranges() {
  return subarray_.non_overlapping_ranges(storage_manager_->compute_tp());
}

bool Query::is_dense() const {
  return !coords_info_.has_coords_;
}

std::vector<WrittenFragmentInfo>& Query::get_written_fragment_info() {
  return written_fragment_info_;
}

void Query::reset_coords_markers() {
  if ((type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      layout_ == Layout::GLOBAL_ORDER) {
    coord_buffer_is_set_ = false;
    coord_data_buffer_is_set_ = false;
    coord_offsets_buffer_is_set_ = false;
  }
}

void Query::set_remote_query() {
  remote_query_ = true;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

}  // namespace sm
}  // namespace tiledb
