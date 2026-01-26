/**
 * @file   rest_client_remote.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares the server-enabled REST client class, used for remote
 * operation.
 *
 * This header is only to be included in ordinary library code in its companion
 * source file `rest_client_remote.cc`. This class declaration is broken out
 * into a separate file so that it can be linked in standalone unit tests.
 *
 * @section Maturity
 *
 * There are no standalone unit test for this class at the present time.
 */

// clang-format off
#include "tiledb/sm/serialization/capnp_utils.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/array_schema_evolution.h"
#include "tiledb/sm/serialization/config.h"
#include "tiledb/sm/serialization/consolidation.h"
#include "tiledb/sm/serialization/enumeration.h"
#include "tiledb/sm/serialization/fragment_info.h"
#include "tiledb/sm/serialization/fragments.h"
#include "tiledb/sm/serialization/group.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/query_plan.h"
#include "tiledb/sm/serialization/tiledb-rest.capnp.h"
#include "tiledb/sm/serialization/vacuum.h"
#include "tiledb/sm/serialization/rest_capabilities.h"
#include "tiledb/sm/rest/curl.h" // must be included last to avoid Windows.h
// clang-format on

// Something, somewhere seems to be defining TIME_MS as a macro
#if defined(TIME_MS)
#undef TIME_MS
#endif

#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/endian.h"
#include "tiledb/sm/misc/parse_argument.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_remote_buffer_storage.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/type/apply_with_type.h"

using namespace tiledb::common;

namespace tiledb::sm {

/**
 * Server-enabled `RestClient` class for remote operation.
 */
class RestClientRemote : public RestClient {
 public:
  /**
   * The default constructor is deleted.
   *
   * A server-enabled `RestClient` needs resources to operate. It doesn't make
   * sense to have an empty one.
   */
  RestClientRemote() = delete;

  /**
   * Ordinary constructor
   *
   * @pre
   * - Lifespan of `parent_stats` is longer than this object
   * - Lifespan of `config` is longer than this object
   * - Lifespan of `compute_tp` is longer than this object
   */
  RestClientRemote(
      stats::Stats& parent_stats,
      const Config& config,
      ThreadPool& compute_tp,
      Logger& logger,
      shared_ptr<MemoryTracker>&& tracker);

  /**
   * Check if use_refactored_array_open_and_query_submit is set in
   * input config so that rest_client chooses the right URI
   *
   * @param config Config to check
   */
  static bool use_refactored_query(const Config& config);

  /**
   * Provides context to the caller if this RestClient is enabled for remote
   * operations without throwing an exception.
   *
   * @return True in RestClientRemote, false for instances of RestClient.
   */
  bool rest_enabled() const override {
    return true;
  }

  /**
   * @return TileDB core version currently deployed to the REST server.
   */
  inline const std::optional<TileDBVersion>& rest_tiledb_version()
      const override {
    return get_capabilities_from_rest().rest_tiledb_version_;
  }

  /**
   * @return Minimum TileDB core version currently supported by the REST server.
   */
  inline const std::optional<TileDBVersion>&
  rest_minimum_supported_tiledb_version() const override {
    return get_capabilities_from_rest().rest_minimum_supported_version_;
  }

  /**
   * Check if REST capabilities are currently known to the RestClient. This
   * will not attempt to initialize them if they are currently unknown.
   *
   * @return True if RestCapabilities member has been initialized by a previous
   * REST capabilities endpoint request, else False.
   */
  inline bool rest_capabilities_detected() const override {
    return rest_capabilities_.detected_;
  }

  /**
   * Check if we are using legacy REST or TileDB-Server.
   *
   * @return True if we're using legacy REST, False if using TileDB-Server.
   */
  inline bool rest_legacy() const override {
    return get_capabilities_from_rest().legacy_;
  }

  /**
   * Check if an array exists by making a REST call. To start with this fetches
   * the schema but ignores the body returned if non-error
   *
   * @param uri to check
   * @return tuple of Status and if array exists
   */
  tuple<Status, std::optional<bool>> check_array_exists_from_rest(
      const URI& uri) override;

  /**
   * Check if an group exists by making a REST call. To start with this fetches
   * the schema but ignores the body returned if non-error
   *
   * @param uri to check
   * @return tuple of Status and if group exists
   */
  tuple<Status, std::optional<bool>> check_group_exists_from_rest(
      const URI& uri) override;

  /**
   * Get a data encoded array schema from rest server.
   *
   * @param uri of array being loaded
   * @return Status and new ArraySchema shared pointer.
   */
  tuple<Status, optional<shared_ptr<ArraySchema>>> get_array_schema_from_rest(
      const URI& uri) override;

  /**
   * Get an array schema from the rest server. This will eventually replace the
   * get_array_schema_from_rest after TileDB-Cloud-REST merges support for the
   * POST endpoint.
   *
   * @param config The TileDB config.
   * @param uri The Array URI to load the schema from.
   * @param timestamp_start The starting timestamp used to open the array.
   * @param timestamp_end The ending timestamp used to open the array.
   * @return Tuple containing the latest array schema, and all array schemas for
   *    the array opened with provided timestamps.
   */
  std::tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>>
  post_array_schema_from_rest(
      const Config& config,
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      bool include_enumerations) override;

  /**
   * Post the array config and get an array from rest server
   *
   * @param uri of array being loaded
   * @param resources the context resources
   * @param array array to load into
   */
  void post_array_from_rest(
      const URI& uri, ContextResources& resources, Array* array) override;

  /**
   * Post a data array schema to rest server
   *
   * @param uri of array being created
   * @param array_schema array schema to load into
   * @return Status Ok() on success Error() on failures
   */
  Status post_array_schema_to_rest(
      const URI& uri, const ArraySchema& array_schema) override;

  /**
   * Post an array creation request to the rest server.
   *
   * @param uri URI of array being created
   * @param array_schema Array schema to use for the created array
   */
  void post_array_create_to_rest(
      const URI& uri, const ArraySchema& array_schema) override;

  /**
   * Deletes all written data from array at the given URI from the REST server.
   *
   * @param uri Array URI to delete
   */
  void delete_array_from_rest(const URI& uri) override;

  /**
   * Deletes the fragments written between the given timestamps from the array
   * at the given URI from the REST server.
   *
   * @param uri Array URI to delete fragments from
   * @param array Array to delete fragments from
   * @param timestamp_start The start timestamp at which to delete fragments
   * @param timestamp_end The end timestamp at which to delete fragments
   */
  void post_delete_fragments_to_rest(
      const URI& uri,
      Array* array,
      uint64_t timestamp_start,
      uint64_t timestamp_end) override;

  /**
   * Deletes the fragments with the given URIs from the array at the given URI
   * from the REST server.
   *
   * @param uri Array URI to delete fragments from
   * @param array Array to delete fragments from
   * @param fragment_uris The uris of the fragments to be deleted
   */
  void post_delete_fragments_list_to_rest(
      const URI& uri,
      Array* array,
      const std::vector<URI>& fragment_uris) override;

  /**
   * Deregisters an array at the given URI from the REST server.
   *
   * @param uri Array URI to deregister
   * @return Status
   */
  Status deregister_array_from_rest(const URI& uri) override;

  /**
   * Get array's non_empty domain from rest server
   *
   * @param array Array model to fetch and set non empty domain on
   * @param timestamp_start Inclusive starting timestamp at which to open array
   * @param timestamp_end Inclusive ending timestamp at which to open array
   * @return Status Ok() on success Error() on failures
   */
  Status get_array_non_empty_domain(
      Array* array, uint64_t timestamp_start, uint64_t timestamp_end) override;

  /**
   * Gets the array's metadata from the REST server (and updates the in-memory
   * Metadata of the array to match the returned values).
   *
   * @param uri Array URI
   * @param timestamp_start Inclusive starting timestamp at which to open array
   * @param timestamp_end Inclusive ending timestamp at which to open array
   * @param array Array to fetch metadata for
   * @return Status
   */
  Status get_array_metadata_from_rest(
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      Array* array) override;

  /**
   * Posts the array's metadata to the REST server.
   *
   * @param uri Array URI
   * @param timestamp_start Inclusive starting timestamp at which to open array
   * @param timestamp_end Inclusive ending timestamp at which to open array
   * @param array Array to update/post metadata for.
   * @return Status
   */
  Status post_array_metadata_to_rest(
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      Array* array) override;

  /**
   * Get the requested enumerations from the REST server via POST request.
   *
   * @param uri Array URI.
   * @param timestamp_start Inclusive starting timestamp at which to open array.
   * @param timestamp_end Inclusive ending timestamp at which to open array.
   * @param config Config options
   * @param array_schema Array schema to fetch enumerations for.
   * @param enumeration_names The names of the enumerations to get.
   */
  std::unordered_map<std::string, std::vector<shared_ptr<const Enumeration>>>
  post_enumerations_from_rest(
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      const Config& config,
      const ArraySchema& array_schema,
      const std::vector<std::string>& enumeration_names,
      shared_ptr<MemoryTracker>) override;

  /**
   * Get the requested query plan from the REST server via POST request.
   *
   * @param uri Array URI.
   * @param query Query to fetch query plan for.
   * @param query_plan The requested query plan.
   */
  void post_query_plan_from_rest(
      const URI& uri, Query& query, QueryPlan& query_plan) override;

  /**
   * Post a data query to rest server
   *
   * @param uri of array being queried
   * @param query to send to server and store results in, this qill be modified
   * @return Status Ok() on success Error() on failures
   */
  Status submit_query_to_rest(const URI& uri, Query* query) override;

  /**
   * Post a data query to rest server
   *
   * @param uri of array being queried
   * @param query to send to server and store results in, this will be modified
   * @return Status Ok() on success Error() on failures
   */
  Status finalize_query_to_rest(const URI& uri, Query* query) override;

  /**
   * Submit and finalize a query to rest server. Used in global order
   * writes to submit the last tile-unaligned chunk and finalize the query.
   *
   * @param uri of array being queried
   * @param query to send to server and store results in
   * @return Status Ok() on success Error() on failures
   */
  Status submit_and_finalize_query_to_rest(
      const URI& uri, Query* query) override;

  /**
   * Get array's non_empty domain from rest server
   *
   * @param array Array model to fetch and set non empty domain on
   * @return Status Ok() on success Error() on failures
   */
  Status get_query_est_result_sizes(const URI& uri, Query* query) override;

  /**
   * Post array schema evolution to rest server
   *
   * @param uri of array being queried
   * @param array_schema_evolution to send to server
   * @return Status Ok() on success Error() on failures
   */
  Status post_array_schema_evolution_to_rest(
      const URI& uri, ArraySchemaEvolution* array_schema_evolution) override;

  /**
   * Get array's fragment info from rest server
   *
   * @param uri Array uri to query for
   * @param fragment_info Fragment info object to store the incoming info
   * @return Status Ok() on success Error() on failures
   */
  Status post_fragment_info_from_rest(
      const URI& uri, FragmentInfo* fragment_info) override;

  /**
   * Gets the group's metadata from the REST server (and updates the in-memory
   * Metadata of the group to match the returned values).
   *
   * @param uri Group URI
   * @param group Group to fetch metadata for
   * @return Status
   */
  Status post_group_metadata_from_rest(const URI& uri, Group* group) override;

  /**
   * Posts the group's metadata to the REST server.
   *
   * @param uri Group URI
   * @param group Group to update/post metadata for.
   * @return Status
   */
  Status put_group_metadata_to_rest(const URI& uri, Group* group) override;

  /**
   * Get group details from the REST server.
   *
   * @param uri Group UI
   * @param group Group to deserialize into
   * @return Status
   */
  Status post_group_from_rest(const URI& uri, Group* group) override;

  /**
   * Post group details to the REST server.
   *
   * @param uri Group UI
   * @param group Group to serialize
   * @return Status
   */
  Status patch_group_to_rest(const URI& uri, Group* group) override;

  /**
   * Deletes all written data from group at the given URI from the REST server.
   *
   * @param uri Group URI to delete
   * @param recursive True if all data inside the group is to be deleted
   */
  void delete_group_from_rest(const URI& uri, bool recursive) override;

  /**
   * Post group create to the REST server.
   *
   * @param uri Group UI
   * @param group Group to create
   * @return Status
   */
  Status post_group_create_to_rest(const URI& uri, Group* group) override;

  /**
   * Post array consolidation request to the REST server.
   *
   * @param uri Array URI
   * @param config config
   * @return
   */
  Status post_consolidation_to_rest(
      const URI& uri, const Config& config) override;

  /**
   * Post array vacuum request to the REST server.
   *
   * @param uri Array URI
   * @param config config
   * @return
   */
  Status post_vacuum_to_rest(const URI& uri, const Config& config) override;

  /**
   * Get consolidation plan from the REST server via POST request.
   *
   * @param uri Array URI.
   * @param config Config of the array.
   * @param fragment_size Maximum fragment size for constructing the plan.
   * @return The requested consolidation plan
   */
  std::vector<std::vector<std::string>> post_consolidation_plan_from_rest(
      const URI& uri, const Config& config, uint64_t fragment_size) override;

  /**
   * Get REST capabilities from the REST server.
   *
   * @return RestCapabilities object initialized with context from REST server.
   */
  const RestCapabilities& get_capabilities_from_rest() const override;

 private:
  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** The class stats. */
  stats::Stats* stats_;

  /** The TileDB config options (contains server and auth info). */
  const Config* config_;

  /** The thread pool for compute-bound tasks. */
  ThreadPool* compute_tp_;

  /** Serialization type. */
  SerializationType serialization_type_;

  /**
   * If true (the default), automatically resubmit incomplete queries on the
   * server-side. This guarantees that the user only receive a complete query
   * result from the server.
   *
   * When this is turned on, it is currently an error if the user buffers on the
   * client are too small to receive all data received from the server
   * (regardless of how many times the query is resubmitted).
   */
  bool resubmit_incomplete_;

  /** Array URI to redirected server mapping. */
  std::unordered_map<std::string, std::string> redirect_meta_;

  /** Mutex for thread-safety. */
  mutable std::mutex redirect_mtx_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The class MemoryTracker. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** REST supported TileDB versions and capabilities. */
  mutable RestCapabilities rest_capabilities_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * POSTs a query submit request to the REST server and deserializes the
   * response into the same query object.
   *
   * For read queries, this also updates the given copy state with the number of
   * bytes copied for each attribute, which allows for automatic resubmission of
   * incomplete queries while concatenating to the user buffers.
   *
   * @param uri URI of array being queried
   * @param query Query to send to server and store results in, this will be
   *    modified.
   * @param copy_state Map of copy state per attribute. As attribute data is
   *    copied into user buffers on reads, the state of each attribute in this
   *    map is updated accordingly.
   * @return
   */
  Status post_query_submit(
      const URI& uri, Query* query, serialization::CopyState* copy_state);

  /**
   * Callback to invoke as partial, buffered response data is received from
   * posting a query.
   *
   * This is not thread-safe. It expects the response data to be ordered. The
   * response must contain serialized query objects, prefixed by an 8-byte
   * unsigned integer that contains the byte-size of the serialized query object
   * it is prefixing. The scratch space must be empty before the first
   * invocation, and must not change until the last invocation has completed.
   *
   * @param reset True if the callback must wipe the in-memory state
   * @param contents the partial response data
   * @param content_nbytes the size of the response data in 'contents'
   * @param skip_retries Output argument that can be set to true to
   *    prevent the curl layer from retrying this request.
   * @scratch scratch space to use between invocations of this callback
   * @query the query object used for deserializing the serialized query
   *    objects in the response data.
   * @param copy_state Map of copy state per attribute. As attribute data is
   *    copied into user buffers on reads, the state of each attribute in this
   *    map is updated accordingly.
   * @param status Status used to communicate callback failures within tiledb
   *    library code.
   * @return Number of acknowledged bytes
   */
  size_t query_post_call_back(
      const bool reset,
      void* const contents,
      const size_t content_nbytes,
      bool* const skip_retries,
      shared_ptr<Buffer> scratch,
      Query* query,
      serialization::CopyState* copy_state,
      Status* status);

  /**
   * Returns a string representation of the given subarray. The format is:
   *
   *   "dim0min,dim0max,dim1min,dim1max,..."
   *
   * @param schema Array schema to use for domain information
   * @param subarray Subarray to convert to string
   * @param subarray_str Will be set to the CSV string
   * @return Status
   */
  static Status subarray_to_str(
      const ArraySchema& schema,
      const void* subarray,
      std::string* subarray_str);

  /**
   * Sets the buffer sizes on the given query using the given state mapping (per
   * attribute). Applicable only when deserializing read queries on the client.
   *
   * @param copy_state State map of attribute to buffer sizes.
   * @param query Query to update buffers for
   * @return Status
   */
  Status update_attribute_buffer_sizes(
      const serialization::CopyState& copy_state, Query* query) const;

  /**
   * Helper function encapsulating the functionality of looking up for cached
   * redirected rest server addresses to avoid the redirection overhead
   *
   * @param array_ns Array namespace
   * @param array_uri Array URI
   * @return Returns the redirection URI if exists and empty string otherwise
   */
  std::string redirect_uri(const std::string& cache_key);
};

}  // namespace tiledb::sm
