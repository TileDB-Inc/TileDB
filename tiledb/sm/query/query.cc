/*
 * @file   query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/current_domain.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/sm/query/deletes_and_updates/deletes_and_updates.h"
#include "tiledb/sm/query/dimension_label/array_dimension_label_queries.h"
#include "tiledb/sm/query/legacy/reader.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/dense_reader.h"
#include "tiledb/sm/query/readers/ordered_dim_label_reader.h"
#include "tiledb/sm/query/readers/sparse_global_order_reader.h"
#include "tiledb/sm/query/readers/sparse_unordered_with_dups_reader.h"
#include "tiledb/sm/query/writers/global_order_writer.h"
#include "tiledb/sm/query/writers/ordered_writer.h"
#include "tiledb/sm/query/writers/unordered_writer.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/tile/writer_tile_tuple.h"

#include <cassert>
#include <iostream>
#include <sstream>

using namespace tiledb::common;
using namespace tiledb::sm::stats;

namespace tiledb::sm {

/**
 * Gets the effective memory budget for a query. This will be memory_budget if
 * set, otherwise the value "sm.mem.total_budget" from config.
 */
static uint64_t get_effective_memory_budget(
    const Config& config, optional<uint64_t> memory_budget) {
  return memory_budget ?
             *memory_budget :
             config.get<uint64_t>("sm.mem.total_budget", Config::must_find);
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Query::Query(
    ContextResources& resources,
    CancellationSource cancellation_source,
    StorageManager* storage_manager,
    shared_ptr<Array> array,
    optional<std::string> fragment_name,
    optional<uint64_t> memory_budget)
    : resources_(resources)
    , stats_(resources_.stats().create_child("Query"))
    , logger_(resources_.logger()->clone("Query", ++logger_id_))
    , query_memory_tracker_(resources_.memory_tracker_manager().create_tracker(
          get_effective_memory_budget(resources_.config(), memory_budget),
          [stats = stats_]() {
            stats->add_counter("memory_budget_exceeded", 1);
          }))
    , array_shared_(array)
    , array_(array_shared_.get())
    , opened_array_(array->opened_array())
    , array_schema_(array->array_schema_latest_ptr())
    , config_(resources_.config())
    , type_(array_->get_query_type())
    , layout_(
          (type_ == QueryType::READ || array_schema_->dense()) ?
              Layout::ROW_MAJOR :
              Layout::UNORDERED)
    , cancellation_source_(cancellation_source)
    , storage_manager_(storage_manager)
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
    , remote_query_(false)
    , is_dimension_label_ordered_read_(false)
    , dimension_label_increasing_(true)
    , fragment_size_(std::numeric_limits<uint64_t>::max())
    , memory_budget_(memory_budget)
    , query_remote_buffer_storage_(std::nullopt)
    , default_channel_{make_shared<QueryChannel>(HERE(), *this, 0)} {
  assert(array->is_open());

  if (array->get_query_type() == QueryType::READ) {
    query_memory_tracker_->set_type(MemoryTrackerType::QUERY_READ);
  } else {
    query_memory_tracker_->set_type(MemoryTrackerType::QUERY_WRITE);
  }

  subarray_ = Subarray(array_, layout_, stats_, logger_);

  fragment_metadata_ = array->fragment_metadata();

  coords_info_.coords_buffer_ = nullptr;
  coords_info_.coords_buffer_size_ = nullptr;
  coords_info_.coords_num_ = 0;
  coords_info_.has_coords_ = false;

  callback_ = nullptr;
  callback_data_ = nullptr;
  status_ = QueryStatus::UNINITIALIZED;

  // Set initial subarray configuration
  subarray_.set_config(type_, config_);

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

void Query::field_require_array_fixed(
    const std::string_view origin, std::string_view field_name) {
  if (!array_schema_->is_field(field_name.data())) {
    throw QueryException(
        std::string{origin} + ": '" + std::string{field_name} +
        "' is not an array field");
  }
  if (array_schema_->var_size(field_name.data())) {
    throw QueryException(
        std::string{origin} + ": '" + std::string{field_name} +
        "' is not fixed-sized");
  }
}

void Query::field_require_array_variable(
    const std::string_view origin, std::string_view field_name) {
  if (!array_schema_->is_field(field_name.data())) {
    throw QueryException(
        std::string{origin} + ": '" + std::string{field_name} +
        "' is not an array field");
  }
  if (!array_schema_->var_size(field_name.data())) {
    throw QueryException(
        std::string{origin} + ": '" + std::string{field_name} +
        "' is not variable-sized");
  }
}

void Query::field_require_array_nullable(
    const std::string_view origin, std::string_view field_name) {
  if (!array_schema_->attribute(field_name.data())) {
    throw QueryException(
        std::string{origin} + ": '" + std::string{field_name} +
        "' is not the name of an attribute");
  }
  if (!array_schema_->is_nullable(field_name.data())) {
    throw QueryException(
        std::string{origin} + ": attribute '" + std::string{field_name} +
        "' is not nullable");
  }
}

void Query::field_require_array_nonnull(
    const std::string_view origin, std::string_view field_name) {
  if (array_schema_->is_nullable(field_name.data())) {
    throw QueryException(
        std::string(origin) + ": field '" + std::string(field_name) +
        "' is not a nonnull array field");
  }
}

constexpr std::string_view origin_est_result_size{
    "query estimated result size"};

FieldDataSize Query::internal_est_result_size(std::string_view field_name) {
  if (type_ != QueryType::READ) {
    throw QueryException(
        std::string{origin_est_result_size} +
        ": operation currently supported only for read queries");
  }
  if (array_->is_remote() && !subarray_.est_result_size_computed()) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw QueryException(
          "Error in query estimate result size; "
          "remote array with no rest client.");
    }
    throw_if_not_ok(
        rest_client->get_query_est_result_sizes(array_->array_uri(), this));
  }
  return subarray_.get_est_result_size(
      field_name, &config_, &resources_.compute_tp());
}

FieldDataSize Query::get_est_result_size_fixed_nonnull(
    std::string_view field_name) {
  field_require_array_fixed(origin_est_result_size, field_name);
  if (field_name == constants::coords) {
    if (!array_schema_->domain().all_dims_same_type()) {
      throw QueryException(
          std::string{origin_est_result_size} +
          ": not applicable to zipped coordinates "
          "in arrays with heterogeneous domain");
    }
    if (!array_schema_->domain().all_dims_fixed()) {
      throw QueryException(
          std::string{origin_est_result_size} +
          ": not applicable to zipped coordinates "
          "in arrays with domains with variable-sized dimensions");
    }
  }
  field_require_array_nonnull(origin_est_result_size, field_name);
  return internal_est_result_size(field_name);
}

FieldDataSize Query::get_est_result_size_variable_nonnull(
    std::string_view field_name) {
  field_require_array_variable(origin_est_result_size, field_name);
  field_require_array_nonnull(origin_est_result_size, field_name);
  return internal_est_result_size(field_name);
}

FieldDataSize Query::get_est_result_size_fixed_nullable(
    std::string_view field_name) {
  field_require_array_fixed(origin_est_result_size, field_name);
  field_require_array_nullable(origin_est_result_size, field_name);
  return internal_est_result_size(field_name);
}

FieldDataSize Query::get_est_result_size_variable_nullable(
    std::string_view field_name) {
  field_require_array_variable(origin_est_result_size, field_name);
  field_require_array_nullable(origin_est_result_size, field_name);
  return internal_est_result_size(field_name);
}

std::unordered_map<std::string, Subarray::ResultSize>
Query::get_est_result_size_map() {
  return subarray_.get_est_result_size_map(&config_, &resources_.compute_tp());
}

std::unordered_map<std::string, Subarray::MemorySize>
Query::get_max_mem_size_map() {
  return subarray_.get_max_mem_size_map(&config_, &resources_.compute_tp());
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

std::vector<std::string> Query::dimension_label_buffer_names() const {
  std::vector<std::string> ret(label_buffers_.size());

  size_t i = 0;
  for (const auto& buffer : label_buffers_) {
    ret[i++] = buffer.first;
  }

  return ret;
}

std::vector<std::string> Query::aggregate_buffer_names() const {
  std::vector<std::string> buffer_names;
  buffer_names.reserve(aggregate_buffers_.size());

  for (const auto& buffer : aggregate_buffers_) {
    buffer_names.push_back(buffer.first);
  }
  return buffer_names;
}

std::vector<std::string> Query::unwritten_buffer_names() const {
  std::vector<std::string> ret;
  for (auto& name : buffer_names()) {
    if (written_buffers_.count(name) == 0) {
      ret.push_back(name);
    }
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

  if (array_schema_->is_dim_label(name)) {
    // Dimension label buffer
    auto buf = label_buffers_.find(name);
    if (buf != label_buffers_.end()) {
      return buf->second;
    }
  } else if (is_aggregate(name)) {
    // Aggregate buffer
    auto buf = aggregate_buffers_.find(name);
    if (buf != aggregate_buffers_.end()) {
      return buf->second;
    }
  } else {
    // Attribute or dimension
    auto buf = buffers_.find(name);
    if (buf != buffers_.end()) {
      return buf->second;
    }
  }

  // Named buffer does not exist
  return QueryBuffer{};
}

Status Query::finalize() {
  if (status_ == QueryStatus::UNINITIALIZED ||
      (status_ == QueryStatus::INITIALIZED && !array_->is_remote())) {
    return Status::Ok();
  }

  if (array_->is_remote() && type_ == QueryType::WRITE) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw QueryException(
          "Failed to finalize query; remote array with no rest client.");
    }

    if (layout_ == Layout::GLOBAL_ORDER) {
      throw QueryException(
          "Failed to finalize query; remote global order writes are only "
          "allowed to call submit_and_finalize to submit the last tile");
    }
    return rest_client->finalize_query_to_rest(array_->array_uri(), this);
  }

  throw_if_not_ok(strategy_->finalize());

  status_ = QueryStatus::COMPLETED;
  return Status::Ok();
}

Status Query::submit_and_finalize() {
  if (type_ != QueryType::WRITE || layout_ != Layout::GLOBAL_ORDER) {
    throw QueryException(
        "Failed to submit and finalize query; Call valid only in global_order "
        "writes.");
  }

  // Check attribute/dimensions buffers completeness before query submits
  throw_if_not_ok(check_buffers_correctness());

  if (array_->is_remote()) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw QueryException(
          "Failed to submit and finalize query; remote array with no rest "
          "client.");
    }

    if (status_ == QueryStatus::UNINITIALIZED) {
      throw_if_not_ok(create_strategy());
    }
    return rest_client->submit_and_finalize_query_to_rest(
        array_->array_uri(), this);
  }

  init();
  throw_if_not_ok(storage_manager_->query_submit(this));

  throw_if_not_ok(strategy_->finalize());
  status_ = QueryStatus::COMPLETED;

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
      array_schema_->dimension_ptr(name) == nullptr &&
      !array_schema_->is_dim_label(name)) {
    return logger_->status(Status_QueryError(
        std::string(
            "Cannot get buffer; Invalid attribute/dimension/label name '") +
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

  // Dimension label
  it = label_buffers_.find(name);
  if (it != label_buffers_.end()) {
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
  if (!ArraySchema::is_special_attribute(name)) {
    if (array_schema_->attribute(name) == nullptr &&
        array_schema_->dimension_ptr(name) == nullptr &&
        !array_schema_->is_dim_label(name))
      return logger_->status(Status_QueryError(
          std::string(
              "Cannot get buffer; Invalid attribute/dimension/label name '") +
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

  // Return the buffer
  it = label_buffers_.find(name);
  if (it != label_buffers_.end()) {
    if (array_schema_->dimension_label(name).is_var()) {
      *buffer = it->second.buffer_var_;
      *buffer_size = it->second.buffer_var_size_;
    } else {
      *buffer = it->second.buffer_;
      *buffer_size = it->second.buffer_size_;
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

Status Query::get_attr_serialization_state(
    const std::string& attribute, SerializationState::AttrState** state) {
  *state = &serialization_state_.attribute_states[attribute];
  return Status::Ok();
}

bool Query::has_results() const {
  if (status_ == QueryStatus::UNINITIALIZED ||
      status_ == QueryStatus::INITIALIZED || type_ != QueryType::READ) {
    return false;
  }

  for (const auto& it : buffers_) {
    if (*(it.second.buffer_size_) != 0)
      return true;
  }
  return false;
}

void Query::init() {
  // Only if the query has not been initialized before
  if (status_ == QueryStatus::UNINITIALIZED ||
      status_ == QueryStatus::INITIALIZED) {
    // Check if the array got closed
    if (array_ == nullptr || !array_->is_open()) {
      throw QueryException(
          "Cannot init query; The associated array is not open");
    }

    // Check if the array got re-opened with a different query type
    QueryType array_query_type{array_->get_query_type()};
    if (array_query_type != type_) {
      throw QueryException(
          "Cannot init query; Associated array query type does not match "
          "query type: (" +
          query_type_str(array_query_type) + " != " + query_type_str(type_) +
          ")");
    }

    throw_if_not_ok(check_buffer_names());

    // Create dimension label queries and remove labels from subarray.
    if (uses_dimension_labels()) {
      if (condition_.has_value()) {
        throw QueryException(
            "Cannot init query; Using query conditions and dimension labels "
            "together is not supported.");
      }

      // Check the layout is valid.
      if (layout_ == Layout::GLOBAL_ORDER) {
        throw QueryException(
            "Cannot init query; The global order layout is not supported "
            "when querying dimension labels");
      }

      // Support for reading dimension label data from sparse arrays with
      // multiple dimensions is not yet implemented. The data needs to be
      // reformatted after reading to match the form of other attribute and
      // dimension output.
      if (!only_dim_label_query() && type_ == QueryType::READ &&
          !array_schema_->dense() && array_schema_->dim_num() > 1 &&
          !label_buffers_.empty()) {
        throw QueryException(
            "Cannot initialize query; Reading dimension label data is not "
            "yet supported on sparse arrays with multiple dimensions.");
      }

      // Initialize the dimension label queries.
      dim_label_queries_ = tdb_unique_ptr<ArrayDimensionLabelQueries>(tdb_new(
          ArrayDimensionLabelQueries,
          resources_,
          storage_manager_,
          array_,
          subarray_,
          label_buffers_,
          buffers_,
          fragment_name_));
    }

    // Create the query strategy if querying main array and the Subarray does
    // not need to be updated.
    if (!only_dim_label_query() && !subarray_.has_label_ranges()) {
      throw_if_not_ok(create_strategy());
    }
  }

  status_ = QueryStatus::INPROGRESS;
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

const std::optional<QueryCondition>& Query::condition() const {
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

void Query::cancel() {
  local_state_machine_.event(LocalQueryEvent::cancel);
  status_ = QueryStatus::FAILED;
}

bool Query::cancelled() {
  return local_state_machine_.is_cancelled();
}

Status Query::process() {
  if (status_ == QueryStatus::UNINITIALIZED ||
      status_ == QueryStatus::INITIALIZED) {
    return logger_->status(
        Status_QueryError("Cannot process query; Query is not initialized"));
  }

  status_ = QueryStatus::INPROGRESS;

  // Check if we need to process label ranges and update subarray before
  // continuing to the main query.
  if (dim_label_queries_ && !dim_label_queries_->completed_range_queries()) {
    // Process the dimension label queries. Updates the subarray of this query
    // to have the index ranges computed from the label ranges.
    dim_label_queries_->process_range_queries(this);

    // The dimension label query did not complete. For now, we are failing on
    // this step. In the future, this may be updated to allow incomplete
    // dimension label queries.
    if (!dim_label_queries_->completed_range_queries()) {
      status_ = QueryStatus::FAILED;
      return logger_->status(
          Status_QueryError("Cannot process query; Failed to read data from "
                            "dimension label ranges."));
    }

    if (!only_dim_label_query()) {
      if (strategy_ != nullptr) {
        // The strategy destructor should reset its own Stats object here
        dynamic_cast<StrategyBase*>(strategy_.get())->stats()->reset();
        strategy_ = nullptr;
      }
      // This changes the query into INITIALIZED, but it's ok as the status
      // is updated correctly below
      throw_if_not_ok(create_strategy());
    }
  }

  if (condition_.has_value()) {
    auto& names = condition_->enumeration_field_names();
    std::unordered_set<std::string> deduped_enmr_names;
    for (auto name : names) {
      auto attr = array_schema_->attribute(name);
      if (attr == nullptr) {
        continue;
      }
      auto enmr_name = attr->get_enumeration_name();
      if (enmr_name.has_value()) {
        deduped_enmr_names.insert(enmr_name.value());
      }
    }
    std::vector<std::string> enmr_names;
    enmr_names.reserve(deduped_enmr_names.size());
    for (auto& enmr_name : deduped_enmr_names) {
      enmr_names.emplace_back(enmr_name);
    }

    parallel_for(
        &resources_.compute_tp(), 0, enmr_names.size(), [&](const uint64_t i) {
          array_->get_enumeration(enmr_names[i]);
        });

    condition_->rewrite_enumeration_conditions(array_schema());
  }

  if (type_ == QueryType::READ) {
    auto cd = array_schema_->get_current_domain();
    if (!cd->empty()) {
      // See if any data was written outside of the current domain.
      bool all_ned_contained_in_current_domain = true;
      for (auto& meta : fragment_metadata_) {
        if (!cd->includes(meta->non_empty_domain())) {
          all_ned_contained_in_current_domain = false;
        }
      }

      for (Domain::dimension_size_type d = 0; d < array_schema_->dim_num();
           d++) {
        if (subarray_.is_set(d)) {
          // Make sure all ranges are contained in the current domain.
          for (auto& range : subarray_.ranges_for_dim(d)) {
            if (!cd->includes(d, range)) {
              throw QueryException(
                  "A range was set outside of the current domain.");
            }
          }
        } else if (!all_ned_contained_in_current_domain) {
          // Add ranges to make sure all data read is contained in the current
          // domain.
          auto range_copy = cd->ndrectangle()->get_range(d);
          subarray_.add_range(d, std::move(range_copy));
        }
      }
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

    copy_aggregates_data_to_user_buffer();
    status_ = QueryStatus::COMPLETED;
  } else {
    // Either the main query or the dimension lable query are incomplete.
    status_ = QueryStatus::INCOMPLETE;
  }

  return Status::Ok();
}

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
    // The strategy destructor should reset its own Stats object here
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

void Query::set_config(const Config& config) {
  if (!remote_query_ && status_ != QueryStatus::UNINITIALIZED) {
    throw QueryException(
        "[set_config] Cannot set config after initialization.");
  }
  config_.inherit(config);

  query_memory_tracker_->refresh_memory_budget(
      get_effective_memory_budget(config_, memory_budget_));

  // Refresh memory budget configuration.
  if (strategy_ != nullptr) {
    strategy_->refresh_config();
  }

  // Set subarray's config for backwards compatibility
  // Users expect the query config to effect the subarray based on existing
  // behavior before subarray was exposed directly
  subarray_.set_config(type_, config_);
}

Status Query::set_coords_buffer(void* buffer, uint64_t* buffer_size) {
  // Set zipped coordinates buffer
  coords_info_.coords_buffer_ = buffer;
  coords_info_.coords_buffer_size_ = buffer_size;
  coords_info_.has_coords_ = true;

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
    const bool check_null_buffers,
    const bool serialization_allow_new_attr) {
  // General checks for fixed buffers
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

  // If this is for a dimension label, set the dimension label buffer and
  // return.
  if (array_schema_->is_dim_label(name)) {
    // Check the query type is valid.
    if (type_ != QueryType::READ && type_ != QueryType::WRITE) {
      throw QueryException("[set_data_buffer] Unsupported query type.");
    }

    const bool exists = label_buffers_.find(name) != label_buffers_.end();
    if (status_ != QueryStatus::UNINITIALIZED && !exists &&
        !serialization_allow_new_attr) {
      throw QueryException(
          "[set_data_buffer] Cannot set buffer for new dimension label '" +
          name + "' after initialization");
    }

    // Set dimension label buffer on the appropriate buffer depending if the
    // label is fixed or variable length.
    array_schema_->dimension_label(name).is_var() ?
        label_buffers_[name].set_data_var_buffer(buffer, buffer_size) :
        label_buffers_[name].set_data_buffer(buffer, buffer_size);
    return Status::Ok();
  }

  // If this is an aggregate buffer, set it and return.
  if (is_aggregate(name)) {
    const bool is_var =
        default_channel_aggregates_[name]->aggregation_var_sized();
    if (!is_var) {
      // Fixed size data buffer
      aggregate_buffers_[name].set_data_buffer(buffer, buffer_size);
    } else {
      // Var sized data buffer
      aggregate_buffers_[name].set_data_var_buffer(buffer, buffer_size);
    }

    return Status::Ok();
  }

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Check that attribute/dimension exists
  if (!ArraySchema::is_special_attribute(name) && !is_dim && !is_attr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Invalid attribute/dimension/label '") +
        name + "'"));
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
  if (status_ != QueryStatus::UNINITIALIZED && !exists &&
      !allow_separate_attribute_writes() && !serialization_allow_new_attr) {
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

  const bool is_var = array_schema_->var_size(name);
  if (is_dim && !is_var &&
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

  // Make sure the buffer was not already written.
  if (written_buffers_.count(name) != 0) {
    return logger_->status(Status_QueryError(
        std::string("Buffer ") + name + std::string(" was already written")));
  }

  has_coords_buffer_ |= is_dim;

  // Set attribute/dimension buffer on the appropriate buffer
  if (!is_var) {
    // Fixed size data buffer
    buffers_[name].set_data_buffer(buffer, buffer_size);
  } else {
    // Var sized data buffer
    buffers_[name].set_data_var_buffer(buffer, buffer_size);
  }

  return Status::Ok();
}

std::optional<shared_ptr<IAggregator>> Query::get_aggregate(
    std::string output_field_name) const {
  auto it = default_channel_aggregates_.find(output_field_name);
  if (it == default_channel_aggregates_.end()) {
    return nullopt;
  }
  return it->second;
}

Status Query::set_offsets_buffer(
    const std::string& name,
    uint64_t* const buffer_offsets,
    uint64_t* const buffer_offsets_size,
    const bool check_null_buffers,
    const bool serialization_allow_new_attr) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check buffer
  if (check_null_buffers && buffer_offsets == nullptr) {
    return logger_->status(
        Status_QueryError("Cannot set buffer; " + name + " buffer is null"));
  }

  // Check buffer size
  if (check_null_buffers && buffer_offsets_size == nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " buffer size is null"));
  }

  // If this is for a dimension label, set the dimension label offsets buffer
  // and return.
  if (array_schema_->is_dim_label(name)) {
    // Check the query type is valid.
    if (type_ != QueryType::READ && type_ != QueryType::WRITE) {
      throw QueryException("[set_offsets_buffer] Unsupported query type.");
    }

    // Check the dimension label is in fact variable length.
    if (!array_schema_->dimension_label(name).is_var()) {
      throw QueryException(
          "[set_offsets_buffer] Input dimension label '" + name +
          "' is fixed-sized");
    }

    // Check the query was not already initialized.
    const bool exists = label_buffers_.find(name) != label_buffers_.end();
    if (status_ != QueryStatus::UNINITIALIZED && !exists &&
        !serialization_allow_new_attr) {
      throw QueryException(
          "[set_offsets_buffer] Cannot set buffer for new dimension label '" +
          name + "' after initialization");
    }

    // Set dimension label offsets buffers.
    label_buffers_[name].set_offsets_buffer(
        buffer_offsets, buffer_offsets_size);
    return Status::Ok();
  }

  // If this is an aggregate buffer, set it and return.
  if (is_aggregate(name)) {
    if (!array_schema_->var_size(
            default_channel_aggregates_[name]->field_name())) {
      return logger_->status(Status_QueryError(
          std::string("Cannot set buffer; Input attribute '") + name +
          "' is not var sized"));
    }

    aggregate_buffers_[name].set_offsets_buffer(
        buffer_offsets, buffer_offsets_size);

    return Status::Ok();
  }

  // For easy reference
  const bool is_dim = array_schema_->is_dim(name);
  const bool is_attr = array_schema_->is_attr(name);

  // Neither a dimension nor an attribute
  if (!is_dim && !is_attr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Invalid buffer name '") + name +
        "' (it should be an attribute, dimension, or dimension label)"));
  }

  // Error if it is fixed-sized
  if (!array_schema_->var_size(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute/dimension '") + name +
        "' is fixed-sized"));
  }

  // Error if setting a new attribute/dimension after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists &&
      !allow_separate_attribute_writes() && !serialization_allow_new_attr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute/dimension '") + name +
        "' after initialization"));
  }

  if (is_dim &&
      (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE)) {
    // Check number of coordinates
    uint64_t coords_num =
        *buffer_offsets_size / constants::cell_var_offset_size;

    const bool offsets_extra_element_ =
        config_.get<bool>("sm.var_offsets.extra_element", Config::must_find);

    if (offsets_extra_element_) {
      // the offsets buffer has `ncoords + 1` elements so that each coordinate
      // is given by `[offset[i], offset[i + 1])` instead of using the length
      // to determine the last
      --coords_num;
    }

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

  // Make sure the buffer was not already written.
  if (written_buffers_.count(name) != 0) {
    return logger_->status(Status_QueryError(
        std::string("Buffer ") + name + std::string(" was already written")));
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
    const bool check_null_buffers,
    const bool serialization_allow_new_attr) {
  RETURN_NOT_OK(check_set_fixed_buffer(name));

  // Check validity buffer
  if (check_null_buffers && buffer_validity_bytemap == nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer is null"));
  }

  // Check validity buffer size
  if (check_null_buffers && buffer_validity_bytemap_size == nullptr) {
    return logger_->status(Status_QueryError(
        "Cannot set buffer; " + name + " validity buffer size is null"));
  }

  // If this is an aggregate buffer, set it and return.
  if (is_aggregate(name)) {
    if (!array_schema_->is_nullable(
            default_channel_aggregates_[name]->field_name())) {
      return logger_->status(Status_QueryError(
          std::string("Cannot set buffer; Input attribute '") + name +
          "' is not nullable"));
    }

    aggregate_buffers_[name].set_validity_buffer(
        {buffer_validity_bytemap, buffer_validity_bytemap_size});

    return Status::Ok();
  }

  // Must be an attribute
  if (!array_schema_->is_attr(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Buffer name '") + name +
        "' is not an attribute"));
  }

  // Must be nullable
  if (!array_schema_->is_nullable(name)) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer; Input attribute '") + name +
        "' is not nullable"));
  }

  // Error if setting a new attribute after initialization
  const bool exists = buffers_.find(name) != buffers_.end();
  if (status_ != QueryStatus::UNINITIALIZED && !exists &&
      !allow_separate_attribute_writes() && !serialization_allow_new_attr) {
    return logger_->status(Status_QueryError(
        std::string("Cannot set buffer for new attribute '") + name +
        "' after initialization"));
  }

  // Make sure the buffer was not already written.
  if (written_buffers_.count(name) != 0) {
    return logger_->status(Status_QueryError(
        std::string("Buffer ") + name + std::string(" was already written")));
  }

  // Set attribute/dimension buffer
  buffers_[name].set_validity_buffer(
      {buffer_validity_bytemap, buffer_validity_bytemap_size});

  return Status::Ok();
}

Status Query::set_est_result_size(
    std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
    std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size) {
  if (type_ != QueryType::READ) {
    return LOG_STATUS(Status_SerializationError(
        "Cannot set estimated result size; Unsupported query type."));
  }
  subarray_.set_est_result_size(est_result_size, max_mem_size);
  return Status::Ok();
}

Status Query::set_layout(Layout layout) {
  if (layout == layout_) {  // Noop
    return Status::Ok();
  }

  if (status_ != QueryStatus::UNINITIALIZED) {
    return logger_->status(
        Status_QueryError("Cannot set layout after initialization"));
  }

  switch (type_) {
    case (QueryType::READ):
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
  if (status_ != tiledb::sm::QueryStatus::UNINITIALIZED) {
    return logger_->status(Status_QueryError(
        "Cannot set query condition; Setting a query condition on an already"
        "initialized query is not supported."));
  }

  if (condition.empty()) {
    throw std::invalid_argument("Query conditions must not be empty");
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

void Query::add_index_ranges_from_label(
    uint32_t dim_idx,
    const bool is_point_ranges,
    const void* start,
    const uint64_t count) {
  subarray_.add_index_ranges_from_label(dim_idx, is_point_ranges, start, count);
}

void Query::set_status(QueryStatus status) {
  status_ = status;
}

void Query::set_subarray(const void* subarray) {
  // Perform checks related to the query type.
  switch (type_) {
    case QueryType::READ:
      break;

    case QueryType::WRITE:
    case QueryType::MODIFY_EXCLUSIVE:
      if (!array_schema_->dense()) {
        throw QueryException(
            "[set_subarray] Setting a subarray is not supported on "
            "sparse writes.");
      }
      break;

    default:

      throw QueryException(
          "[set_subarray] Setting a subarray is not supported for query type "
          "'" +
          query_type_str(type_) + "'.");
  }

  // Check this isn't an already initialized query using dimension labels.
  if (status_ != QueryStatus::UNINITIALIZED) {
    throw QueryException(
        "[set_subarray] Setting a subarray on an already initialized  "
        "query is not supported.");
  }

  // Set the subarray.
  subarray_.set_subarray(subarray);
}

const Subarray* Query::subarray() const {
  return &subarray_;
}

Status Query::set_subarray_unsafe(const Subarray& subarray) {
  subarray_ = subarray;
  return Status::Ok();
}

void Query::set_subarray(const tiledb::sm::Subarray& subarray) {
  // Perform checks related to the query type.
  switch (type_) {
    case QueryType::READ:
      break;

    case QueryType::WRITE:
    case QueryType::MODIFY_EXCLUSIVE:
      if (!array_schema_->dense()) {
        throw QueryException(
            "[set_subarray] Setting a subarray is not supported on "
            "sparse writes.");
      }
      break;

    default:
      throw QueryException(
          "[set_subarray] Setting a subarray is not supported for query type "
          "'" +
          query_type_str(type_) + "'.");
  }

  // Check the query has not been initialized.
  if (status_ != tiledb::sm::QueryStatus::UNINITIALIZED) {
    throw QueryException(
        "[set_subarray] Setting a subarray on an already initialized "
        "query is not supported.");
  }

  // Set the subarray.
  auto prev_layout = subarray_.layout();
  subarray_ = subarray;
  subarray_.set_layout(prev_layout);
}

Status Query::set_subarray_unsafe(const NDRange& subarray) {
  // Prepare a subarray object
  Subarray sub(array_, layout_, stats_, logger_);
  if (!subarray.empty()) {
    auto dim_num = array_schema_->dim_num();
    for (unsigned d = 0; d < dim_num; ++d)
      sub.add_range_unsafe(d, subarray[d]);
  }

  assert(layout_ == sub.layout());
  subarray_ = sub;

  return Status::Ok();
}

void Query::set_subarray_unsafe(const void* subarray) {
  subarray_.set_subarray_unsafe(subarray);
}

Status Query::submit() {
  // Do not resubmit completed reads.
  if (type_ == QueryType::READ && status_ == QueryStatus::COMPLETED) {
    return Status::Ok();
  }

  // Make sure fragment size is only set for global order.
  if (fragment_size_ != std::numeric_limits<uint64_t>::max() &&
      (layout_ != Layout::GLOBAL_ORDER || type_ != QueryType::WRITE)) {
    throw QueryException(
        "[submit] Fragment size is only supported for global order writes.");
  }

  // Check attribute/dimensions buffers completeness before query submits
  throw_if_not_ok(check_buffers_correctness());

  if (array_->is_remote()) {
    auto rest_client = resources_.rest_client();
    if (rest_client == nullptr) {
      throw QueryException(
          "Failed to submit query; remote array with no rest client.");
    }

    if (status_ == QueryStatus::UNINITIALIZED && !only_dim_label_query() &&
        !subarray_.has_label_ranges()) {
      throw_if_not_ok(create_strategy());

      // Allocate remote buffer storage for global order writes if necessary.
      // If we cache an entire write a query may be uninitialized for N submits.
      if (!query_remote_buffer_storage_.has_value() &&
          type_ == QueryType::WRITE && layout_ == Layout::GLOBAL_ORDER) {
        query_remote_buffer_storage_.emplace(*this, buffers_);
      }
    }

    throw_if_not_ok(
        rest_client->submit_query_to_rest(array_->array_uri(), this));

    reset_coords_markers();
    return Status::Ok();
  }
  init();
  throw_if_not_ok(storage_manager_->query_submit(this));

  reset_coords_markers();
  return Status::Ok();
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

const Config& Query::config() const {
  return config_;
}

stats::Stats* Query::stats() const {
  return stats_;
}

void Query::set_stats(const stats::StatsData& data) {
  stats_->populate_with_data(data);
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

  const std::string& val = config_.get("sm.query.dense.reader", &found);
  assert(found);
  use_refactored_reader = val == "refactored";

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

  const std::string& val =
      config_.get("sm.query.sparse_global_order.reader", &found);
  assert(found);
  use_refactored_reader = val == "refactored";
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

  const std::string& val =
      config_.get("sm.query.sparse_unordered_with_dups.reader", &found);
  assert(found);
  use_refactored_reader = val == "refactored";

  return use_refactored_reader && !array_schema.dense() &&
         layout == Layout::UNORDERED && array_schema.allows_dups();
}

bool Query::non_overlapping_ranges() {
  return subarray_.non_overlapping_ranges(&resources_.compute_tp());
}

bool Query::is_dense() const {
  return array_schema_->dense();
}

std::vector<WrittenFragmentInfo>& Query::get_written_fragment_info() {
  return written_fragment_info_;
}

std::unordered_set<std::string>& Query::get_written_buffers() {
  return written_buffers_;
}

void Query::set_remote_query() {
  remote_query_ = true;
}

void Query::set_dimension_label_ordered_read(bool increasing_order) {
  is_dimension_label_ordered_read_ = true;
  dimension_label_increasing_ = increasing_order;
}

bool Query::is_aggregate(std::string output_field_name) const {
  return default_channel_aggregates_.count(output_field_name) != 0;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

Layout Query::effective_layout() const {
  // If the user has not set a layout, it will default to row-major, which will
  // use the legacy reader on sparse arrays, and fail if aggregates were
  // specified. However, if only aggregates are specified and no regular data
  // buffers, the layout doesn't matter and we can transparently switch to the
  // much more efficient unordered layout.
  if (type_ == QueryType::READ && !array_schema_->dense() && has_aggregates() &&
      buffers_.empty()) {
    return Layout::UNORDERED;
  }
  return layout_;
}

Status Query::create_strategy(bool skip_checks_serialization) {
  auto layout = effective_layout();
  auto params = StrategyParams(
      resources_,
      array_->memory_tracker(),
      query_memory_tracker_,
      local_state_machine_,
      cancellation_source_,
      opened_array_,
      config_,
      memory_budget_,
      buffers_,
      aggregate_buffers_,
      subarray_,
      layout,
      condition_,
      default_channel_aggregates_,
      skip_checks_serialization);
  if (type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) {
    if (layout == Layout::COL_MAJOR || layout == Layout::ROW_MAJOR) {
      if (!array_schema_->dense()) {
        return Status_QueryError(
            "Cannot create strategy; sparse writes do not support layout " +
            layout_str(layout));
      }
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          OrderedWriter,
          stats_->create_child("Writer"),
          logger_,
          params,
          written_fragment_info_,
          coords_info_,
          remote_query_,
          fragment_name_));
    } else if (layout == Layout::UNORDERED) {
      if (array_schema_->dense()) {
        return Status_QueryError(
            "Cannot create strategy; dense writes do not support layout " +
            layout_str(layout));
      }
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          UnorderedWriter,
          stats_->create_child("Writer"),
          logger_,
          params,
          written_fragment_info_,
          coords_info_,
          written_buffers_,
          remote_query_,
          fragment_name_));
    } else if (layout == Layout::GLOBAL_ORDER) {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          GlobalOrderWriter,
          stats_->create_child("Writer"),
          logger_,
          params,
          fragment_size_,
          written_fragment_info_,
          disable_checks_consolidation_,
          processed_conditions_,
          coords_info_,
          remote_query_,
          fragment_name_));
    } else {
      return Status_QueryError(
          "Cannot create strategy; unsupported layout " + layout_str(layout));
    }
  } else if (type_ == QueryType::READ) {
    bool all_dense = true;
    for (auto& frag_md : fragment_metadata_) {
      all_dense &= frag_md->dense();
    }

    // We are going to deprecate dense arrays with sparse fragments in 2.27 but
    // log a warning for now.
    if (array_schema_->dense() && !all_dense) {
      LOG_WARN(
          "This dense array contains sparse fragments. Support for reading "
          "sparse fragments in dense arrays will be removed in TileDB version "
          "2.27 to be released in September 2024. To make sure this array "
          "continues to work after an upgrade to version 2.27 or later, please "
          "consolidate the sparse fragments using a TileDB version 2.26 or "
          "earlier.");
    }

    if (is_dimension_label_ordered_read_) {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          OrderedDimLabelReader,
          stats_->create_child("Reader"),
          logger_,
          params,
          dimension_label_increasing_));
    } else if (use_refactored_sparse_unordered_with_dups_reader(
                   layout, *array_schema_)) {
      if (Query::non_overlapping_ranges() || !subarray_.is_set() ||
          subarray_.range_num() == 1) {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseUnorderedWithDupsReader<uint8_t>,
            stats_->create_child("Reader"),
            logger_,
            params));
      } else {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseUnorderedWithDupsReader<uint64_t>,
            stats_->create_child("Reader"),
            logger_,
            params));
      }
    } else if (
        use_refactored_sparse_global_order_reader(layout, *array_schema_) &&
        !array_schema_->dense() &&
        (layout == Layout::GLOBAL_ORDER || layout == Layout::UNORDERED)) {
      // Using the reader for unordered queries to do deduplication.
      if (Query::non_overlapping_ranges() || !subarray_.is_set() ||
          subarray_.range_num() == 1) {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseGlobalOrderReader<uint8_t>,
            stats_->create_child("Reader"),
            logger_,
            params,
            consolidation_with_timestamps_));
      } else {
        strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
            SparseGlobalOrderReader<uint64_t>,
            stats_->create_child("Reader"),
            logger_,
            params,
            consolidation_with_timestamps_));
      }
    } else if (use_refactored_dense_reader(*array_schema_, all_dense)) {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          DenseReader,
          stats_->create_child("Reader"),
          logger_,
          params,
          remote_query_));
    } else {
      strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
          Reader,
          stats_->create_child("Reader"),
          logger_,
          params,
          remote_query_));
    }
  } else if (type_ == QueryType::DELETE || type_ == QueryType::UPDATE) {
    strategy_ = tdb_unique_ptr<IQueryStrategy>(tdb_new(
        DeletesAndUpdates,
        stats_->create_child("Deletes"),
        logger_,
        params,
        update_values_));
  } else {
    return logger_->status(
        Status_QueryError("Cannot create strategy; unsupported query type"));
  }

  if (strategy_ == nullptr)
    return logger_->status(
        Status_QueryError("Cannot create strategy; allocation failed"));

  // Transition the query into INITIALIZED state
  if (!skip_checks_serialization) {
    set_status(QueryStatus::INITIALIZED);
  }

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
    if (!only_dim_label_query() && !allow_separate_attribute_writes()) {
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

    // All dimension buffers should be set for separate attribute writes.
    if (allow_separate_attribute_writes()) {
      for (unsigned d = 0; d < array_schema_->dim_num(); d++) {
        auto dim = array_schema_->dimension_ptr(d);
        if (buffers_.count(dim->name()) == 0) {
          throw QueryException(
              "[check_buffer_names] Dimension buffer " + dim->name() +
              " is not set");
        }
      }
    }
  }

  return Status::Ok();
}

Status Query::check_buffers_correctness() {
  ensure_query_type_is_valid(type_);

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
  // 3. At least one label buffer or subarray label range is set.
  return (
      (!label_buffers_.empty() || subarray_.has_label_ranges()) &&
      (buffers_.size() == 0 ||
       (buffers_.size() == 1 &&
        (coord_buffer_is_set_ || coord_data_buffer_is_set_ ||
         coord_offsets_buffer_is_set_))));
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

void Query::reset_coords_markers() {
  if ((type_ == QueryType::WRITE || type_ == QueryType::MODIFY_EXCLUSIVE) &&
      layout_ == Layout::GLOBAL_ORDER) {
    coord_buffer_is_set_ = false;
    coord_data_buffer_is_set_ = false;
    coord_offsets_buffer_is_set_ = false;
  }
}

void Query::copy_aggregates_data_to_user_buffer() {
  for (auto& default_channel_aggregate : default_channel_aggregates_) {
    default_channel_aggregate.second->copy_to_user_buffer(
        default_channel_aggregate.first, aggregate_buffers_);
  }
}

RestClient* Query::rest_client() const {
  return resources_.rest_client().get();
}

}  // namespace tiledb::sm
