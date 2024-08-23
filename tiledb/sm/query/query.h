/**
 * @file   query.h
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
 * This file defines class Query.
 */

#ifndef TILEDB_QUERY_H
#define TILEDB_QUERY_H

#include <atomic>
#include <functional>
#include <sstream>
#include <utility>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/query_status_details.h"
#include "tiledb/sm/fragment/written_fragment_info.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/query_remote_buffer_storage.h"
#include "tiledb/sm/query/query_state.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"
#include "tiledb/sm/query/readers/aggregators/query_channel.h"
#include "tiledb/sm/query/update_value.h"
#include "tiledb/sm/query/validity_vector.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/storage_manager/cancellation_source.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class ArrayDimensionLabelQueries;

enum class QueryStatus : uint8_t;
enum class QueryType : uint8_t;

/** Processes a (read/write) query. */
class Query {
 public:
  /* ********************************* */
  /*          PUBLIC DATATYPES         */
  /* ********************************* */

  /**
   * Contains any current state related to (de)serialization of this query.
   * Mostly this supports setting buffers on this query that were allocated
   * internally as a part of deserialization (as opposed to user-set buffers).
   */
  struct SerializationState {
    /** Serialization state for a single attribute. */
    struct AttrState {
      /**
       * Buffer holding (or wrapping) fixed-length data, either attribute or
       * offset data.
       */
      Buffer fixed_len_data;

      /** Buffer holding (or wrapping) variable-length data. */
      Buffer var_len_data;

      /** Buffer holding (or wrapping) validity vector data. */
      Buffer validity_len_data;

      /** Value holding the length of the fixed-length data. */
      uint64_t fixed_len_size = 0;

      /** Value holding the length of the variable-length data. */
      uint64_t var_len_size = 0;

      /** Value holding the length of the validity vector data. */
      uint64_t validity_len_size = 0;
    };

    /** Serialization state per attribute. */
    std::unordered_map<std::string, AttrState> attribute_states;
  };

  /**
   * Contains current state related to coords of this query.
   */
  struct CoordsInfo {
    /**
     * True if either zipped coordinates buffer or separate coordinate
     * buffers are set.
     */
    bool has_coords_;

    /** The zipped coordinates buffer potentially set by the user. */
    void* coords_buffer_;

    /** The zipped coordinates buffer size potentially set by the user. */
    uint64_t* coords_buffer_size_;

    /** Keeps track of the number of coordinates across coordinate buffers. */
    uint64_t coords_num_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor. The query type is inherited from the query type of the
   * input array. An optional fragment URI is given as input in
   * case the query will be used as writes and the given URI should be used
   * for the name of the new fragment to be created.
   *
   * @section Maturity
   *
   * This is a transitional constructor. There is still a `StorageManager`
   * argument, and there is also a vestige of it with the `CancellationSource`
   * argument. These argument now only pertain to job control of query with
   * respect to its context. Once this facility is rewritten, these constructor
   * argument may be dropped.
   *
   * @pre Array must be a properly opened array.
   *
   * @param resources The context resources.
   * @param cancellation_source A source of external cancellation events
   * @param storage_manager Storage manager object.
   * @param array The array that is being queried.
   * @param fragment_uri The full URI for the new fragment. Only used for
   * writes.
   * @param fragment_base_uri Optional base name for new fragment. Only used for
   *     writes and only if fragment_uri is empty.
   * @param memory_budget Total memory budget. If set to nullopt, the value
   *     will be obtained from the sm.mem.total_budget config option.
   */
  Query(
      ContextResources& resources,
      CancellationSource cancellation_source,
      StorageManager* storage_manager,
      shared_ptr<Array> array,
      optional<std::string> fragment_name = nullopt,
      optional<uint64_t> memory_budget = nullopt);

  /** Destructor. */
  virtual ~Query();

  DISABLE_COPY_AND_COPY_ASSIGN(Query);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Query);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Require that a name be that of a fixed-size field from the source array.
   *
   * Validate these conditions and throw if any are not met.
   * - The name is that of a field in the array
   * - The field is fixed-sized
   *
   * @param origin The name of the operation that this validation is a part of
   * @param field_name The name of a field
   */
  void field_require_array_fixed(
      const std::string_view origin, std::string_view field_name);

  /**
   * Require that a name be that of a variable-size field from the source array.
   *
   * Validate these conditions and throw if any are not met.
   * - The name is that of a field in the array
   * - The field is variable-sized
   *
   * @param origin The name of the operation that this validation is a part of
   * @param field_name The name of a field
   */
  void field_require_array_variable(
      const std::string_view origin, std::string_view field_name);

  /**
   * Require that a field be a nullable field from the source array.
   *
   * Validate these conditions and throw if any are not met.
   * - The name is that of an attribute of the source array
   * - The attribute is nullable
   *
   * @param origin The name of the operation that this validation is a part of
   * @param field_name The name of a field
   */
  void field_require_array_nullable(
      const std::string_view origin, std::string_view field_name);

  /**
   * Require that a field be a nonnull field from the source array.
   *
   * Validate these conditions and throw if any are not met.
   * - The name is that of a field in the array
   * - The field is not nullable
   *
   * @param origin The name of the operation that this validation is a part of.
   * @param field_name The name of a field
   */
  void field_require_array_nonnull(
      const std::string_view origin, std::string_view field_name);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  FieldDataSize get_est_result_size_fixed_nonnull(std::string_view field_name);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute/dimension.
   */
  FieldDataSize get_est_result_size_variable_nonnull(
      std::string_view field_name);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  FieldDataSize get_est_result_size_fixed_nullable(std::string_view field_name);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized,
   * nullable attribute.
   */
  FieldDataSize get_est_result_size_variable_nullable(
      std::string_view field_name);

 private:
  /**
   * Common part of all `est_result_size_*` functions, called after argument
   * validation.
   *
   * @param field_name The name of a field
   * @return estimated result size
   */
  FieldDataSize internal_est_result_size(std::string_view field_name);

 public:
  /** Retrieves the number of written fragments. */
  Status get_written_fragment_num(uint32_t* num) const;

  /** Retrieves the URI of the written fragment with the input index. */
  Status get_written_fragment_uri(uint32_t idx, const char** uri) const;

  /**
   * Retrieves the timestamp range [t1,t2] of the written fragment with the
   * input index.
   */
  Status get_written_fragment_timestamp_range(
      uint32_t idx, uint64_t* t1, uint64_t* t2) const;

  /** Returns the array's smart pointer. */
  inline shared_ptr<Array> array_shared() {
    return array_shared_;
  }

  /** Returns the array. */
  const Array* array() const;

  /** Returns the array. */
  Array* array();

  /** Returns the array schema. */
  const ArraySchema& array_schema() const;

  /** Returns the array schema as a shared_ptr */
  const std::shared_ptr<const ArraySchema> array_schema_shared() const;

  /** Returns the names of the buffers set by the user for the query. */
  std::vector<std::string> buffer_names() const;

  /** Returns the names of dimension label buffers for the query. */
  std::vector<std::string> dimension_label_buffer_names() const;

  /** Returns the names of aggregate buffers for the query. */
  std::vector<std::string> aggregate_buffer_names() const;

  /**
   * Returns the names of the buffers set by the user for the query not already
   * written by a previous partial attribute write.
   */
  std::vector<std::string> unwritten_buffer_names() const;

  /**
   * Gets the query buffer for the input field.
   * An empty string means the special default attribute.
   */
  QueryBuffer buffer(const std::string& name) const;

  /**
   * Cancel a query.
   *
   * This does not immediately halt processing of a query, but does so at the
   * first point where it checks in to see if it should continue processing.
   */
  void cancel();

  /**
   * Predicate function whether the query has been cancelled.
   */
  bool cancelled();

  /**
   * Finalizes the query, flushing all internal state.
   * Applicable to write queries only.
   */
  Status finalize();

  /**
   * Submits and finalizes a query, flushing all internal state. Applicable
   * only to global layout writes, returns an error otherwise.
   */
  Status submit_and_finalize();

  /**
   * Retrieves the data buffer of a fixed/var-sized attribute/dimension.
   *
   * @param name The buffer attribute/dimension name. An empty string means
   *     the special default attribute/dimension.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_data_buffer(
      const char* name, void** buffer, uint64_t** buffer_size) const;

  /**
   * Retrieves the offset buffer for a var-sized attribute/dimension.
   *
   * @param name The buffer attribute/dimension name. An empty string means
   * the special default attribute/dimension.
   * @param buffer_off The offsets buffer to be retrieved. This buffer holds
   * the starting offsets of each cell value in the data buffer.
   * @param buffer_off_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_offsets_buffer(
      const char* name,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size) const;

  /**
   * Retrieves the validity buffer for a nullable attribute/dimension.
   *
   * @param name The buffer attribute/dimension name. An empty string means
   * the special default attribute/dimension.
   * @param buffer_validity_bytemap The buffer that either have the validity
   * bytemap associated with the input data to be written, or will hold the
   * validity bytemap to be read.
   * @param buffer_validity_bytemap_size In the case of writes, this is the size
   * of `buffer_validity_bytemap` in bytes. In the case of reads, this initially
   * contains the allocated size of `buffer_validity_bytemap`, but after the
   * termination of the query it will contain the size of the useful (read)
   * data in `buffer_validity_bytemap`.
   * @return Status
   */
  Status get_validity_buffer(
      const char* name,
      uint8_t** buffer_validity_bytemap,
      uint64_t** buffer_validity_bytemap_size) const;

  /**
   * Returns the serialization state associated with the given attribute.
   *
   * @param attribute Attribute to get serialization state for
   * @param state Set to point to the serialization state
   * @return Status
   */
  Status get_attr_serialization_state(
      const std::string& attribute, SerializationState::AttrState** state);

  /**
   * Used by serialization to get the map of result sizes
   * @return
   */
  std::unordered_map<std::string, Subarray::ResultSize>
  get_est_result_size_map();

  /**
   * Used by serialization to get the map of max mem sizes
   * @return
   */
  std::unordered_map<std::string, Subarray::MemorySize> get_max_mem_size_map();

  /**
   * Retrieve remote buffer storage for remote global order writes.
   *
   * @return QueryRemoteBufferStorage for this query.
   */
  inline std::optional<QueryRemoteBufferStorage>& get_remote_buffer_cache() {
    return query_remote_buffer_storage_;
  }

  /**
   * Returns `true` if the query has results. Applicable only to read
   * queries (it returns `false` for write queries).
   */
  bool has_results() const;

  /** Initializes the query. */
  void init();

  /** Returns the first fragment uri. */
  URI first_fragment_uri() const;

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Returns the cell layout. */
  Layout layout() const;

  /**
   * Returns the condition for filtering results in a read query.
   * @return QueryCondition
   */
  const std::optional<QueryCondition>& condition() const;

  /**
   * Returns the update values for an update query.
   * @return UpdateValues
   */
  const std::vector<UpdateValue>& update_values() const;

  /**
   * Returns true if this query requires the use of dimension labels.
   */
  bool uses_dimension_labels() const;

  /** Processes a query. */
  Status process();

  /** Gets the strategy of the query. */
  IQueryStrategy* strategy(bool skip_checks_serialization = false);

  /**
   * Switch the strategy depending on layout. Used by serialization.
   *
   * @param layout New layout
   * @param force_legacy_reader Force use of the legacy reader if the client
   *    requested it.
   * @return Status
   */
  Status reset_strategy_with_layout(Layout layout, bool force_legacy_reader);

  /**
   * Disables checking the global order and coordinate duplicates. Applicable
   * only to writes. This option will supercede the config.
   */
  Status disable_checks_consolidation();

  /**
   * Enables consolidation with timestamps.
   */
  Status set_consolidation_with_timestamps();

  /**
   * Set the processed conditions for writes.
   *
   * @param processed_conditions The processed conditions.
   */
  void set_processed_conditions(std::vector<std::string>& processed_conditions);

  /**
   * Sets the config for the Query
   *
   * This function overrides the config for Query-level parameters only.
   * Semantically, the initial query config is copied from the context
   * config upon construction. Note that The context config is immutable
   * at the C API level because tiledb_ctx_get_config always returns a copy.
   *
   * Config parameters set here will *only* be applied within the Query.
   *
   * @pre The function must be called before Query::init().
   */
  void set_config(const Config& config);

  /**
   * Sets the config for the Query
   *
   * @param config
   *
   * @note This is a potentially unsafe operation. Queries should be
   * unsubmitted and unfinalized when the config is set. Until C.41 compilance,
   * this is necessary for serialization.
   */
  inline void unsafe_set_config(const Config& config) {
    config_.inherit(config);
  }

  /**
   * Sets the (zipped) coordinates buffer (set with TILEDB_COORDS as the
   * buffer name).
   *
   * @param buffer The buffer that has the input data to be written.
   * @param buffer_size The size of `buffer` in bytes.
   * @return Status
   */
  Status set_coords_buffer(void* buffer, uint64_t* buffer_size);

  /**
   * Sets the data for a fixed/var-sized attribute/dimension.
   *
   * @param name The attribute/dimension to set the buffer for.
   * @param buffer The buffer that will hold the data to be read.
   * @param buffer_size This initially contains the allocated
   *     size of `buffer`, but after the termination of the function
   *     it will contain the size of the useful (read) data in `buffer`.
   * @param check_null_buffers If true (default), null buffers are not
   * allowed.
   * @param serialization_allow_new_attr If true, setting new attributes
   * is allowed in INITIALIZED state
   * @return Status
   */
  Status set_data_buffer(
      const std::string& name,
      void* const buffer,
      uint64_t* const buffer_size,
      const bool check_null_buffers = true,
      const bool serialization_allow_new_attr = false);

  /**
   * Wrapper to set the internal buffer for a dimension or attribute from a
   * QueryBuffer.
   *
   * This function is intended to be a convenience method for use setting
   * buffers for dimension labels.
   *
   * @WARNING Does not check for or copy validity data.
   *
   * @param name Name of the dimension or attribute to set the buffer for.
   * @param buffer The query buffer to get the data from.
   **/
  void set_dimension_label_buffer(
      const std::string& name, const QueryBuffer& buffer);

  /**
   * Sets the offset buffer for a var-sized attribute/dimension.
   *
   * @param name The attribute/dimension to set the buffer for.
   * @param buffer_offsets The buffer that will hold the data to be read.
   *     This buffer holds the starting offsets of each cell value in
   *     `buffer_val`.
   * @param buffer_offsets_size This initially contains
   *     the allocated size of `buffer_off`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_off`.
   * @param check_null_buffers If true (default), null buffers are not
   * allowed.
   * @param serialization_allow_new_attr If true, setting new attributes
   * is allowed in INITIALIZED state
   * @return Status
   */
  Status set_offsets_buffer(
      const std::string& name,
      uint64_t* const buffer_offsets,
      uint64_t* const buffer_offsets_size,
      const bool check_null_buffers = true,
      const bool serialization_allow_new_attr = false);

  /**
   * Sets the validity buffer for nullable attribute/dimension.
   *
   * @param name The attribute/dimension to set the buffer for.
   * @param buffer_validity_bytemap The buffer that either have the validity
   * bytemap associated with the input data to be written, or will hold the
   * validity bytemap to be read.
   * @param buffer_validity_bytemap_size In the case of writes, this is the size
   * of `buffer_validity_bytemap` in bytes. In the case of reads, this initially
   * contains the allocated size of `buffer_validity_bytemap`, but after the
   * termination of the query it will contain the size of the useful (read)
   * data in `buffer_validity_bytemap`.
   * @param check_null_buffers If true (default), null buffers are not
   * allowed.
   * @param serialization_allow_new_attr If true, setting new attributes
   * is allowed in INITIALIZED state
   * @return Status
   */
  Status set_validity_buffer(
      const std::string& name,
      uint8_t* const buffer_validity_bytemap,
      uint64_t* const buffer_validity_bytemap_size,
      const bool check_null_buffers = true,
      const bool serialization_allow_new_attr = false);

  /**
   * Get the config of the query.
   *
   * @return Config from query
   */
  const Config& config() const;

  /**
   * Used by serialization to set the estimated result size
   *
   * @param est_result_size map to set
   * @param max_mem_size map to set
   * @return Status
   */
  Status set_est_result_size(
      std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
      std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size);

  /**
   * Sets the cell layout of the query.
   */
  Status set_layout(Layout layout);

  /**
   * Sets the condition for filtering results in a read query.
   *
   * @param condition The condition object.
   * @return Status
   */
  Status set_condition(const QueryCondition& condition);

  /**
   * Adds an update value for an update query.
   *
   * @param field_name The attribute name.
   * @param update_value The value to set.
   * @param update_value_size The byte size of `update_value`.
   * @return Status
   */
  Status add_update_value(
      const char* field_name,
      const void* update_value,
      uint64_t update_value_size);

  /**
   * Adds ranges to a query initialize with label ranges.
   *
   * @param dim_idx The dimension to update.
   * @param is_point_ranges If ``true`` the data contains point ranges.
   *     Otherwise, it contains standard ranges.
   * @param start Pointer to the start of the range data.
   * @param count Number of total elements in the range data.
   */
  void add_index_ranges_from_label(
      uint32_t dim_idx,
      const bool is_point_range,
      const void* start,
      const uint64_t count);

  /**
   * Set query status, needed for json deserialization
   * @param status
   * @return Status
   */
  void set_status(QueryStatus status);

  /**
   * Sets the query subarray. If it is null, then the subarray will be set to
   * the entire domain.
   *
   * @param subarray The subarray to be set.
   */
  void set_subarray(const void* subarray);

  /** Returns the query subarray. */
  const Subarray* subarray() const;

  /**
   * Sets the query subarray.
   *
   * @param subarray The subarray to be set.
   */
  void set_subarray(const tiledb::sm::Subarray& subarray);

  /** Sets the query subarray, without performing any checks. */
  Status set_subarray_unsafe(const Subarray& subarray);

  /** Sets the query subarray, without performing any checks. */
  Status set_subarray_unsafe(const NDRange& subarray);

  /**
   * Sets the query subarray without performing any checks.
   *
   * Used for deserialize dense writes.
   *
   * @param subarray The subarray to be set.
   */
  void set_subarray_unsafe(const void* subarray);

  /** Submits the query to the storage manager. */
  Status submit();

  /** Returns the query status. */
  QueryStatus status() const;

  /** Returns the query status incomplete reason. */
  QueryStatusDetailsReason status_incomplete_reason() const;

  /** Returns the query type. */
  QueryType type() const;

  /** Returns the internal stats object. */
  stats::Stats* stats() const;

  /**
   * Populate the owned stats instance with data.
   * To be removed when the class will get a C41 constructor.
   *
   * @param data Data to populate the stats with.
   */
  void set_stats(const stats::StatsData& data);

  /** Returns the scratch space used for REST requests. */
  shared_ptr<Buffer> rest_scratch() const;

  /** Use the refactored dense reader or not. */
  bool use_refactored_dense_reader(
      const ArraySchema& array_schema, bool all_dense);

  /** Use the refactored sparse global order reader or not. */
  bool use_refactored_sparse_global_order_reader(
      Layout layout, const ArraySchema& array_schema);

  /** Use the refactored sparse unordered with dups reader or not. */
  bool use_refactored_sparse_unordered_with_dups_reader(
      Layout layout, const ArraySchema& array_schema);

  /** Returns if all ranges for this query are non overlapping. */
  bool non_overlapping_ranges();

  /** Returns true if this is a dense query */
  bool is_dense() const;

  /** Returns true if the config is set to allow separate attribute writes. */
  inline bool allow_separate_attribute_writes() const {
    return config_.get<bool>(
        "sm.allow_separate_attribute_writes", Config::must_find);
  }

  /** Returns a reference to the internal WrittenFragmentInfo list */
  std::vector<WrittenFragmentInfo>& get_written_fragment_info();

  /** Returns a reference to the internal written buffer set */
  std::unordered_set<std::string>& get_written_buffers();

  /** Called from serialization to mark the query as remote */
  void set_remote_query();

  /**
   * Set a flag to specify we are doing an ordered dimension label read.
   *
   * @param increasing_order Is the query on an array with increasing order? If
   * not assume decreasing order.
   */
  void set_dimension_label_ordered_read(bool increasing_order);

  /**
   * Check if the query is a dimension label ordered read.
   * If true, this query will use the OrderedDimLabelReader strategy.
   * Only applicable to dimension label read queries.
   *
   * @return True if the query is a dimension label ordered read, else False.
   */
  inline bool dimension_label_ordered_read() const {
    return is_dimension_label_ordered_read_;
  }

  /**
   * Check if the dimension label query is increasing or decreasing order.
   * Only applicable to dimension label read queries.
   *
   * @return True if increasing order, false if decreasing order.
   */
  inline bool dimension_label_increasing_order() const {
    return dimension_label_increasing_;
  }

  /**
   * Set the fragment size.
   *
   * @param fragment_size Fragment size.
   */
  void set_fragment_size(uint64_t fragment_size) {
    fragment_size_ = fragment_size;
  }

  /** Returns true if the output field is an aggregate. */
  bool is_aggregate(std::string output_field_name) const;

 private:
  /**
   * Create the aggregate channel object. This is split out because it's not
   * at construction time, but on demand, and in two different situations.
   */
  void create_aggregate_channel() {
    /*
     * Because we have an extremely simple way of choosing channel identifiers,
     * we can get away with hard-coding `1` here as the identifier for the
     * aggregate channel.
     */
    aggregate_channel_ = make_shared<QueryChannel>(HERE(), *this, 1);
  }

 public:
  /**
   * Adds an aggregator to the default channel.
   *
   * @param output_field_name Output field name for the aggregate.
   * @param aggregator Aggregator to add.
   */
  void add_aggregator_to_default_channel(
      std::string output_field_name, shared_ptr<IAggregator> aggregator) {
    if (default_channel_aggregates_.empty()) {
      /*
       * Assert: this is the first aggregate added.
       *
       * We create the aggregate channel on demand, and this is when we need to
       * do it.
       */
      create_aggregate_channel();
    }
    default_channel_aggregates_.emplace(output_field_name, aggregator);
  }

  /** Returns an aggregate based on the output field. */
  std::optional<shared_ptr<IAggregator>> get_aggregate(
      std::string output_field_name) const;

  /**
   * Get a list of all channels and their aggregates
   */
  std::vector<LegacyQueryAggregatesOverDefault> get_channels() {
    // Currently only the default channel is supported
    return {
        LegacyQueryAggregatesOverDefault(true, default_channel_aggregates_)};
  }

  /**
   * Add a channel to the query. Used only by capnp serialization to initialize
   * the aggregates list.
   */
  void add_channel(const LegacyQueryAggregatesOverDefault& channel) {
    if (channel.is_default()) {
      default_channel_aggregates_ = channel.aggregates();
      if (!default_channel_aggregates_.empty()) {
        create_aggregate_channel();
      }
      return;
    }
    throw QueryException(
        "We currently only support a default channel for queries");
  }

  /**
   * Returns true if the query has any aggregates on any channels
   */
  bool has_aggregates() {
    // We only need to check the default channel for now
    return !default_channel_aggregates_.empty();
  }

  /**
   * Returns the number of channels.
   *
   * Responsibility for choosing channel identifiers is the responsibility of
   * this class. At the present time the policy is very simple, since all
   * queries only draw from a single array.
   *   - Channel 0: All rows from the query. Always non-segmented, that is,
   *       without any grouping.
   *   - Channel 1: (optional) Simple aggregates, if any exist.
   */
  inline size_t number_of_channels() {
    return has_aggregates() ? 1 : 0;
  };

  /**
   * The default channel is initialized at construction and always exists.
   */
  inline std::shared_ptr<QueryChannel> default_channel() {
    return default_channel_;
  }

  inline std::shared_ptr<QueryChannel> aggegate_channel() {
    if (!has_aggregates()) {
      throw QueryException("Aggregate channel does not exist");
    }
    return aggregate_channel_;
  }

  /**
   * Returns the REST client configured in the context resources associated to
   * this query
   */
  RestClient* rest_client() const;

  /**
   * Returns the ContextResources associated to this query.
   */
  ContextResources& resources() const {
    return resources_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Resource used for operations. */
  ContextResources& resources_;

  /** The class stats. */
  stats::Stats* stats_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** The query memory tracker. */
  shared_ptr<MemoryTracker> query_memory_tracker_;

  /**
   * The state machine for processing local queries.
   *
   * At present this class combines both local and remote queries. This member
   * variable is essentially unused for remote queries.
   */
  LocalQueryStateMachine local_state_machine_{LocalQueryState::uninitialized};

  /** A smart pointer to the array the query is associated with.
   * Ensures that the Array object exists as long as the Query object exists. */
  shared_ptr<Array> array_shared_;

  /**
   * The array the query is associated with.
   * Cached copy of array_shared_.get().
   */
  Array* array_;

  /** Keeps a copy of the opened array object at construction. */
  shared_ptr<OpenedArray> opened_array_;

  /** The array schema. */
  shared_ptr<const ArraySchema> array_schema_;

  /** The config for query-level parameters only. */
  Config config_;

  /** A function that will be called upon the completion of an async query. */
  std::function<void(void*)> callback_;

  /** The data input to the callback function. */
  void* callback_data_;

  /** The query type. */
  QueryType type_;

  /** The layout of the cells in the result of the subarray. */
  Layout layout_;

  /** The query subarray (initially the whole domain by default). */
  Subarray subarray_;

  /** The query status. */
  QueryStatus status_;

  /**
   * The cancellation source. This will be the last vestige of the storage
   * manager.
   */
  CancellationSource cancellation_source_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The query strategy. */
  tdb_unique_ptr<IQueryStrategy> strategy_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /**
   * Maps attribute/dimension names to their buffers.
   * `TILEDB_COORDS` may be used for the special zipped coordinates
   * buffer.
   */
  std::unordered_map<std::string, QueryBuffer> buffers_;

  /** Maps label names to their buffers. */
  std::unordered_map<std::string, QueryBuffer> label_buffers_;

  /**
   * Maps aggregate names to their buffers.
   */
  std::unordered_map<std::string, QueryBuffer> aggregate_buffers_;

  /** Dimension label queries that are part of the main query. */
  tdb_unique_ptr<ArrayDimensionLabelQueries> dim_label_queries_;

  /** Keeps track of the coords data. */
  CoordsInfo coords_info_;

  /** Stores information about the written fragments. */
  std::vector<WrittenFragmentInfo> written_fragment_info_;

  /** The query condition. */
  std::optional<QueryCondition> condition_;

  /** The update values. */
  std::vector<UpdateValue> update_values_;

  /** Set of attributes that have an update value. */
  std::set<std::string> attributes_with_update_value_;

  /** The fragment metadata that this query will focus on. */
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata_;

  /** The current serialization state. */
  SerializationState serialization_state_;

  /** If the query has coords buffer set or not. */
  bool has_coords_buffer_;

  /** If the query has zipped coords buffer set or not. */
  bool has_zipped_coords_buffer_;

  /** True if at least one separate coordinate buffer is set. */
  bool coord_buffer_is_set_;

  /** True if at least one separate data coordinate buffer is set. */
  bool coord_data_buffer_is_set_;

  /** True if at least one separate offsets coordinate buffer is set. */
  bool coord_offsets_buffer_is_set_;

  /** Keeps track of the name of the data buffer once set. */
  std::string data_buffer_name_;

  /** Keeps track of the name of the offsets buffer once set. */
  std::string offsets_buffer_name_;

  /**
   * If `true`, it will not check if the written coordinates are
   * in the global order or have duplicates. This supercedes the config.
   */
  bool disable_checks_consolidation_;

  /**
   * If `true`, it will enable consolidation with timestamps on the reader.
   */
  bool consolidation_with_timestamps_;

  /* Scratch space used for REST requests. */
  shared_ptr<Buffer> rest_scratch_;

  /* Processed conditions, used for consolidation. */
  std::vector<std::string> processed_conditions_;

  /**
   * Flag to force legacy reader when strategy gets created. This is used by
   * the serialization codebase if a query comes from an older version of the
   * library that doesn't have the refactored readers, we need to run it with
   * the legacy reader.
   */
  bool force_legacy_reader_;

  /**
   * The name of the new fragment to be created for writes.
   *
   * If not set, the fragment name will be created using the latest array
   * timestamp and a generated UUID.
   */
  optional<std::string> fragment_name_;

  /** It tracks if this is a remote query */
  bool remote_query_;

  /** Flag to specify we are doing a dimension label ordered read. */
  bool is_dimension_label_ordered_read_;

  /**
   * Is the dimension label ordered read on an array with increasing order? If
   * not assume decreasing order.
   *
   * NOTE: Only used when is_dimension_label_order_read_ == true.
   */
  bool dimension_label_increasing_;

  /**
   * The desired fragment size. The writer will create a new fragment once
   * this size has been reached.
   *
   * Note: This is only used for global order writes.
   */
  uint64_t fragment_size_;

  /**
   * Memory budget. If set to nullopt, the value will be obtained from the
   * sm.mem.total_budget config option.
   */
  optional<uint64_t> memory_budget_;

  /** Already written buffers. */
  std::unordered_set<std::string> written_buffers_;

  /** Cache for tile aligned remote global order writes. */
  std::optional<QueryRemoteBufferStorage> query_remote_buffer_storage_;

  /** Aggregates for the default channel, by output field name. */
  std::unordered_map<std::string, shared_ptr<IAggregator>>
      default_channel_aggregates_;

  /*
   * Handles to channels use shared pointers, so the channels are allocated here
   * for ease of implementation.
   *
   * At present there's only one possible non-default channel, so we keep track
   * of it in its own variable. A fully C.41 class might simply store these in
   * a constant vector.
   */
  /**
   * The default channel is allocated in the constructor for simplicity.
   */
  std::shared_ptr<QueryChannel> default_channel_;

  /**
   * The aggegregate channel is optional, so we initialize it as empty with it
   * default constructor.
   */
  std::shared_ptr<QueryChannel> aggregate_channel_{};

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Create the strategy.
   *
   * @param skip_checks_serialization Skip checks during serialization.
   */
  Status create_strategy(bool skip_checks_serialization = false);

  Status check_set_fixed_buffer(const std::string& name);

  /** Checks if the buffers names have been appropriately set for the query. */
  Status check_buffer_names();

  /**
   * Internal routine for checking the completeness of all attribute
   * and dimensions buffers. Iteratively searches that all attributes &
   * dimensions buffers have been set correctly
   * @return Status
   */
  Status check_buffers_correctness();

  /**
   * Returns true if only querying dimension labels.
   *
   * The query will only query dimension labels if all the following are true:
   * 1. At most one dimension buffer is set.
   * 2. No attribute buffers are set.
   * 3. At least one label buffer or subarray label range is set.
   */
  bool only_dim_label_query() const;

  /**
   * Check if input buffers are tile aligned. This function should be called
   * only for remote global order writes and it should enforce tile alignment
   * for both dense and sparse arrays.
   */
  Status check_tile_alignment() const;

  /**
   * Reset coord buffer markers at end of a global write submit.
   * This will allow for the user to properly set the next write batch.
   */
  void reset_coords_markers();

  /** Copies the data from the aggregates to the user buffers. */
  void copy_aggregates_data_to_user_buffer();
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_H
