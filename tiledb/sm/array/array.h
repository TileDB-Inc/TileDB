/**
 * @file   array.h
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
 This file defines class Array.
 */

#ifndef TILEDB_ARRAY_H
#define TILEDB_ARRAY_H

#include <atomic>
#include <unordered_map>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array/consistency.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/sm/metadata/metadata.h"

using namespace tiledb::common;

namespace tiledb::sm {

class ArraySchema;
class ArraySchemaEvolution;
class FragmentMetadata;
class MemoryTracker;
enum class QueryType : uint8_t;

/**
 * Class to store opened array resources. The class is not C.41 compliant as the
 * serialization code doesn't allow it. It needs to be refactored. This object
 * will not change after the array as been opened though (see note below for the
 * consolidation exception), so eventually we can make this object C.41
 * compliant. That object allows queries or subarrays can take a reference to
 * the resources which is going to be stored in a shared pointer and guarantee
 * that the resources will be kept alive for the whole duration of the query.
 *
 * For consolidation, the opened array resources will be changed after the array
 * is opened. The consolidator does load fragments separately once it figured
 * out what it wants to consolidate. This is not an issue as we control the
 * consolidator code and that code is built around that functionality.
 */
class OpenedArray {
 public:
  /* No default constructor. */
  OpenedArray() = delete;

  /**
   * Construct a new Opened Array object.
   *
   * @param resources The context resources to use.
   * @param memory_tracker The array's MemoryTracker.
   * @param array_uri The URI of the array.
   * @param encryption_type Encryption type.
   * @param key_bytes Encryption key data.
   * @param key_length Encryption key length.
   * @param timestamp_start Start timestamp used to load the array directory.
   * @param timestamp_end_opened_at Timestamp at which the array was opened.
   * @param is_remote Is the array remote?
   */
  OpenedArray(
      ContextResources& resources,
      shared_ptr<MemoryTracker> memory_tracker,
      const URI& array_uri,
      EncryptionType encryption_type,
      const void* key_bytes,
      uint32_t key_length,
      uint64_t timestamp_start,
      uint64_t timestamp_end_opened_at,
      bool is_remote)
      : resources_(resources)
      , array_dir_(ArrayDirectory(resources, array_uri))
      , array_schema_latest_(nullptr)
      , metadata_(memory_tracker)
      , metadata_loaded_(false)
      , non_empty_domain_computed_(false)
      , encryption_key_(make_shared<EncryptionKey>(HERE()))
      , timestamp_start_(timestamp_start)
      , timestamp_end_opened_at_(timestamp_end_opened_at)
      , is_remote_(is_remote) {
    throw_if_not_ok(
        encryption_key_->set_key(encryption_type, key_bytes, key_length));
  }

  /** Returns the context resources. */
  inline ContextResources& resources() {
    return resources_;
  }

  /** Returns the array directory object. */
  inline const ArrayDirectory& array_directory() const {
    return array_dir_.value();
  }

  /** Sets the array directory. */
  inline void set_array_directory(const ArrayDirectory&& dir) {
    array_dir_ = dir;
  }

  /** Returns the latest array schema. */
  inline const ArraySchema& array_schema_latest() const {
    return *(array_schema_latest_.get());
  }

  /** Returns the latest array schema as a shared pointer. */
  inline shared_ptr<ArraySchema> array_schema_latest_ptr() const {
    return array_schema_latest_;
  }

  /** Sets the latest array schema. */
  inline void set_array_schema_latest(
      const shared_ptr<ArraySchema>& array_schema) {
    array_schema_latest_ = array_schema;
  }

  /** Returns all array schemas. */
  inline const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
  array_schemas_all() const {
    return array_schemas_all_;
  }

  /** Sets all array schema. */
  inline void set_array_schemas_all(
      std::unordered_map<std::string, shared_ptr<ArraySchema>>&& all_schemas) {
    array_schemas_all_ = std::move(all_schemas);
  }

  /** Gets a reference to the metadata. */
  inline Metadata& metadata() {
    return metadata_;
  }

  /** Get a reference to the `metadata_loaded_` value. */
  inline bool& metadata_loaded() {
    return metadata_loaded_;
  }

  /** Get a reference to the `non_empty_domain_computed_` value. */
  inline bool& non_empty_domain_computed() {
    return non_empty_domain_computed_;
  }

  /** Gets a reference to the non empty domain. */
  inline NDRange& non_empty_domain() {
    return non_empty_domain_;
  }

  /** Gets a reference to the fragment metadata. */
  inline std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata() {
    return fragment_metadata_;
  }

  /** Returns a constant pointer to the encryption key. */
  inline const EncryptionKey* encryption_key() const {
    return encryption_key_.get();
  }

  /**
   * Returns a reference to the private encryption key.
   */
  inline const EncryptionKey& get_encryption_key() const {
    return *encryption_key_;
  }

  /** Returns the start timestamp used to load the array directory. */
  inline uint64_t timestamp_start() const {
    return timestamp_start_;
  }

  /**
   * Returns the timestamp at which the array was opened.
   *
   * WARNING: This is a legacy function that is needed to support the current
   * API and REST calls. Do not use in new code.
   */
  inline uint64_t timestamp_end_opened_at() const {
    return timestamp_end_opened_at_;
  }

  /** Returns if the array is remote or not. */
  inline bool is_remote() const {
    return is_remote_;
  }

 private:
  /** The context resources. */
  ContextResources& resources_;

  /** The array directory object for listing URIs. */
  optional<ArrayDirectory> array_dir_;

  /** The latest array schema. */
  shared_ptr<ArraySchema> array_schema_latest_;

  /**
   * A map of all array_schemas_all
   */
  std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas_all_;

  /** The array metadata. */
  Metadata metadata_;

  /** True if the array metadata is loaded. */
  bool metadata_loaded_;

  /** True if the non_empty_domain has been computed */
  bool non_empty_domain_computed_;

  /** The non-empty domain of the array. */
  NDRange non_empty_domain_;

  /** The metadata of the fragments the array was opened with. */
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata_;

  /**
   * The private encryption key used to encrypt the array.
   *
   * Note: This is the only place in TileDB where the user's private key
   * bytes should be stored. Whenever a key is needed, a pointer to this
   * memory region should be passed instead of a copy of the bytes.
   */
  shared_ptr<EncryptionKey> encryption_key_;

  /** The start timestamp used to load the array directory. */
  uint64_t timestamp_start_;

  /** The timestamp at which the array was opened. */
  uint64_t timestamp_end_opened_at_;

  /** Is this a remote array? */
  bool is_remote_;
};

/**
 * Free function that returns a reference to the ConsistencyController object.
 */
ConsistencyController& controller();

/**
 * An array object to be opened for reads/writes. An ``Array`` instance
 * is associated with the timestamp it is opened at.
 *
 * @invariant is_opening_or_closing_ is false outside of the body of
 * an open or close function.
 *
 * @invariant is_opening_or_closing_ is true when the class is either
 * partially open or partially closed.
 *
 * @invariant atomicity must be maintained between the following:
 * 1. an open Array.
 * 2. the is_open_ flag.
 * 3. the existence of a ConsistencySentry object, which represents
 * open Array registration.
 *
 * @invariant mtx_ must not be locked outside of the scope of a member function.
 */
class Array {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Array(
      ContextResources& resources,
      const URI& array_uri,
      ConsistencyController& cc = controller());

  /** Destructor. */
  ~Array() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(Array);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Array);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the context resources. */
  inline ContextResources& resources() {
    return resources_;
  }

  /** Returns the opened array. */
  inline const shared_ptr<OpenedArray> opened_array() const {
    return opened_array_;
  };

  /** Returns the array directory object. */
  inline const ArrayDirectory& array_directory() const {
    return opened_array_->array_directory();
  }

  /** Set the array directory. */
  inline void set_array_directory(const ArrayDirectory&& dir) {
    opened_array_->set_array_directory(std::move(dir));
  }

  /** Sets the latest array schema.
   * @param array_schema The array schema to set.
   */
  inline void set_array_schema_latest(
      const shared_ptr<ArraySchema>& array_schema) {
    opened_array_->set_array_schema_latest(array_schema);
  }

  /** Returns the latest array schema. */
  inline const ArraySchema& array_schema_latest() const {
    return opened_array_->array_schema_latest();
  }

  /** Returns the latest array schema as a shared pointer. */
  inline shared_ptr<const ArraySchema> array_schema_latest_ptr() const {
    return opened_array_->array_schema_latest_ptr();
  }

  /** Returns array schemas map. */
  inline const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
  array_schemas_all() const {
    return opened_array_->array_schemas_all();
  }

  /**
   * Sets all array schemas.
   * @param all_schemas The array schemas to set.
   */
  inline void set_array_schemas_all(
      std::unordered_map<std::string, shared_ptr<ArraySchema>>&& all_schemas) {
    opened_array_->set_array_schemas_all(std::move(all_schemas));
  }

  /**
   * Evolve a TileDB array schema and store its new schema.
   *
   * @param resources The context resources.
   * @param array_uri The uri of the array whose schema is to be evolved.
   * @param schema_evolution The schema evolution.
   * @param encryption_key The encryption key to use.
   */
  static void evolve_array_schema(
      ContextResources& resources,
      const URI& array_uri,
      ArraySchemaEvolution* array_schema,
      const EncryptionKey& encryption_key);

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Returns the serialized array URI, this is for backwards compatibility with
   * serialization in pre TileDB 2.4 */
  const URI& array_uri_serialized() const;

  /**
   * Creates a TileDB array, storing its schema.
   *
   * @param resources The context resources.
   * @param array_uri The URI of the array to be created.
   * @param array_schema The array schema.
   * @param encryption_key The encryption key to use.
   */
  static void create(
      ContextResources& resources,
      const URI& array_uri,
      const shared_ptr<ArraySchema>& array_schema,
      const EncryptionKey& encryption_key);

  /**
   * Opens the array for reading at a timestamp retrieved from the config
   * or for writing.
   *
   * @param query_type The mode in which the array is opened.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   */
  Status open(
      QueryType query_type,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Opens the array for reading without fragments.
   *
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open_without_fragments(
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /**
   * Reload the array with the specified fragments.
   *
   * @param fragments_to_load The list of fragments to load.
   */
  void load_fragments(const std::vector<TimestampedURI>& fragments_to_load);

  /**
   * Opens the array for reading.
   *
   * @param query_type The query type. This should always be READ. It
   *    is here only for sanity check.
   * @param timestamp_start The start timestamp at which to open the array.
   * @param timestamp_end The end timestamp at which to open the array.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @return Status
   *
   * @note Applicable only to reads.
   */
  Status open(
      QueryType query_type,
      uint64_t timestamp_start,
      uint64_t timestamp_end,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length);

  /** Closes the array and frees all memory. */
  Status close();

  /**
   * Performs deletion of local fragments with the given parent URI,
   * between the provided timestamps.
   *
   * @param resources The context resources.
   * @param uri The uri of the Array whose fragments are to be deleted.
   * @param timestamp_start The start timestamp at which to delete fragments.
   * @param timestamp_end The end timestamp at which to delete fragments.
   * @param array_dir An optional ArrayDirectory from which to delete fragments.
   *
   * @section Maturity Notes
   * This is legacy code, ported from StorageManager during its removal process.
   * Its existence supports the non-static `delete_fragments` API below,
   * performing the actual deletion of fragments. This function is slated for
   * removal and should be directly integrated into the function below.
   */
  static void delete_fragments(
      ContextResources& resources,
      const URI& uri,
      uint64_t timestamp_start,
      uint64_t timstamp_end,
      std::optional<ArrayDirectory> array_dir = std::nullopt);

  /**
   * Handles local and remote deletion of fragments between the provided
   * timestamps from an open array with the given URI.
   *
   * @param uri The uri of the Array whose fragments are to be deleted.
   * @param timestamp_start The start timestamp at which to delete fragments.
   * @param timestamp_end The end timestamp at which to delete fragments.
   *
   * @pre The Array must be open for exclusive writes
   *
   * @section Maturity Notes
   * This API is an interim version of its final product, awaiting rewrite.
   * As is, it handles the incoming URI and invokes the remote or local function
   * call accordingly. The local, static function above is legacy code which
   * exists only to support this function. A rewrite should integrate the two
   * and remove the need for any static APIs.
   */
  void delete_fragments(
      const URI& uri, uint64_t timestamp_start, uint64_t timstamp_end);

  /**
   * Performs deletion of the local array with the given parent URI.
   *
   * @param resources The context resources.
   * @param uri The uri of the Array whose data is to be deleted.
   *
   * @section Maturity Notes
   * This is legacy code, ported from StorageManager during its removal process.
   * Its existence supports the non-static `delete_array` API below,
   * performing the actual deletion of array data. This function is slated for
   * removal and should be directly integrated into the function below.
   */
  static void delete_array(ContextResources& resources, const URI& uri);

  /**
   * Handles local and remote deletion of an open array with the given URI.
   *
   * @param uri The uri of the Array whose data is to be deleted.
   *
   * @pre The Array must be open for exclusive writes
   *
   * @section Maturity Notes
   * This API is an interim version of its final product, awaiting rewrite.
   * As is, it handles the incoming URI and invokes the remote or local function
   * call accordingly. The local, static function above is legacy code which
   * exists only to support this function. A rewrite should integrate the two
   * and remove the need for any static APIs.
   */
  void delete_array(const URI& uri);

  /**
   * Deletes the fragments with the given URIs from the Array with given URI.
   *
   * @param fragment_uris The uris of the fragments to be deleted.
   *
   * @pre The Array must be open for exclusive writes
   */
  void delete_fragments_list(const std::vector<URI>& fragment_uris);

  /** Returns a constant pointer to the encryption key. */
  const EncryptionKey* encryption_key() const;

  /**
   * Accessor to the fragment metadata of the array.
   */
  inline std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata() {
    return opened_array_->fragment_metadata();
  }

  /**
   * Sets fragment metadata.
   * @param fragment_metadata The fragment metadata.
   */
  inline void set_fragment_metadata(
      std::vector<shared_ptr<FragmentMetadata>>&& fragment_metadata) {
    opened_array_->fragment_metadata() = fragment_metadata;
  }

  /**
   * Retrieves the encryption type.
   *
   * @param resources The context resources.
   * @param uri The URI of the array.
   * @param encryption_type Set to the encryption type.
   */
  static void encryption_type(
      ContextResources& resources,
      const URI& uri,
      EncryptionType* encryption_type);

  /**
   * Get the enumeration for the given name.
   *
   * This function retrieves the enumeration for the given name. If the
   * corresponding enumeration has not been loaded from storage it is
   * loaded before this function returns.
   *
   * @param enumeration_name The name of the enumeration.
   * @return shared_ptr<const Enumeration> or nullptr on failure.
   */
  shared_ptr<const Enumeration> get_enumeration(
      const std::string& enumeration_name);

  /**
   * Get the enumerations with the given names.
   *
   * This function retrieves the enumerations with the given names. If the
   * corresponding enumerations have not been loaded from storage they are
   * loaded before this function returns.
   *
   * @param enumeration_names The names of the enumerations.
   * @param schema The ArraySchema to store loaded enumerations in.
   * @return std::vector<shared_ptr<const Enumeration>> The loaded enumerations.
   */
  std::vector<shared_ptr<const Enumeration>> get_enumerations(
      const std::vector<std::string>& enumeration_names,
      shared_ptr<ArraySchema> schema);

  /** Load all enumerations for the array. */
  void load_all_enumerations();

  /**
   * Returns `true` if the array is empty at the time it is opened.
   * The funciton returns `false` if the array is not open.
   */
  bool is_empty() const;

  /** Returns `true` if the array is open. */
  bool is_open();

  /** Returns `true` if the array is remote */
  bool is_remote() const;

  /** Retrieves the array schema. Errors if the array is not open. */
  tuple<Status, optional<shared_ptr<ArraySchema>>> get_array_schema() const;

  /** Retrieves the query type. Throws if the array is not open. */
  QueryType get_query_type() const;

  /**
   * Returns the max buffer size given a fixed-sized attribute/dimension and
   * a subarray. Errors if the array is not open.
   */
  Status get_max_buffer_size(
      const char* name, const void* subarray, uint64_t* buffer_size);

  /**
   * Returns the max buffer size given a var-sized attribute/dimension and
   * a subarray. Errors if the array is not open.
   */
  Status get_max_buffer_size(
      const char* name,
      const void* subarray,
      uint64_t* buffer_off_size,
      uint64_t* buffer_val_size);

  /**
   * Returns a reference to the private encryption key.
   */
  inline const EncryptionKey& get_encryption_key() const {
    return opened_array_->get_encryption_key();
  }

  /**
   * Re-opens the array. This effectively updates the "view" of the array,
   * by loading the fragments potentially written after the last time
   * the array was opened. Errors if the array is not open.
   *
   * @note Applicable only for reads, it errors if the array was opened
   *     for writes.
   */
  Status reopen();

  /**
   * Re-opens the array between two specific timestamps.
   *
   * @note Applicable only for reads, it errors if the array was opened
   *     for writes.
   */
  Status reopen(uint64_t timestamp_start, uint64_t timestamp_end);

  /** Returns the start timestamp used to load the array directory. */
  inline uint64_t timestamp_start() const {
    return array_dir_timestamp_start_;
  }

  /**
   * Returns the end timestamp as set by the user.
   *
   * This may differ from the actual timestamp in use if the array has not yet
   * been opened, the user has changed this value, or if using the sentinel
   * value of `UINT64_MAX`.
   */
  inline uint64_t timestamp_end() const {
    return user_set_timestamp_end_.value_or(UINT64_MAX);
  }

  /**
   * Returns the timestamp at which the array was opened.
   *
   * WARNING: This is a legacy function that is needed to support the current
   * API and REST calls. Do not use in new code.
   */
  inline uint64_t timestamp_end_opened_at() const {
    return query_type_ == QueryType::READ ?
               array_dir_timestamp_end_ :
               new_component_timestamp_.value_or(0);
  }

  /** Directly set the timestamp start value. */
  inline void set_timestamp_start(uint64_t timestamp_start) {
    array_dir_timestamp_start_ = timestamp_start;
  }

  /** Directly set the timestamp end value. */
  inline void set_timestamp_end(uint64_t timestamp_end) {
    if (timestamp_end == UINT64_MAX) {
      user_set_timestamp_end_ = nullopt;
    } else {
      user_set_timestamp_end_ = timestamp_end;
    }
  }

  /**
   * Set the internal timestamps.
   *
   * Note for sentinel values for `timestamp_end`:
   * * `timestamp_end == UINT64_MAX`:
   *     The array directory end timestamp will be set to the current time if
   *     ``set_current_time=True``. New components will use the time at query
   *     submission.
   * * `timestamp_end` == 0:
   *     The new component timestamp will use the time at query submission.
   *
   * @param timestamp_start The starting timestamp for opening the array
   * directory.
   * @param timstamp_end The ending timestamp for opening the array directory
   * and setting new components. See above comments for sentinel values `0` and
   * `UINT64_MAX`.
   */
  void set_timestamps(
      uint64_t timetamp_start, uint64_t timestamp_end, bool set_current_time);

  /** Directly set the array config.
   *
   * @pre The array must be closed.
   */
  void set_config(Config config);

  /**
   * Directly set the array config.
   *
   * @param config
   *
   * @note This is a potentially unsafe operation. Arrays should be closed when
   * setting a config. This is necessary to maintain current serialization
   * behavior.
   */
  inline void unsafe_set_config(Config config) {
    config_.inherit(config);
  }

  /** Retrieves a reference to the array config. */
  inline Config config() const {
    return config_;
  }

  /** Directly set the array URI for serialized compatibility with pre
   * TileDB 2.5 clients */
  void set_uri_serialized(const std::string& uri) {
    array_uri_serialized_ = tiledb::sm::URI(uri);
  }

  /** Sets the array URI. */
  void set_array_uri(const URI& array_uri) {
    array_uri_ = array_uri;
  }

  /**
   * Deletes metadata from an array opened in WRITE mode.
   *
   * @param key The key of the metadata item to be deleted.
   */
  void delete_metadata(const char* key);

  /**
   * Puts metadata into an array opened in WRITE mode.
   *
   * @param key The key of the metadata item to be added. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   */
  void put_metadata(
      const char* key,
      Datatype value_type,
      uint32_t value_num,
      const void* value);

  /**
   * Gets metadata from an array opened in READ mode. If the key does not
   * exist, then `value` will be `nullptr`.
   *
   * @param key The key of the metadata item to be retrieved. UTF-8 encodings
   *     are acceptable.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   */
  void get_metadata(
      const char* key,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /**
   * Gets metadata from an array opened in READ mode using an index.
   *
   * @param index The index used to retrieve the metadata.
   * @param key The metadata key.
   * @param key_len The metadata key length.
   * @param value_type The datatype of the value.
   * @param value_num The value may consist of more than one items of the
   *     same datatype. This argument indicates the number of items in the
   *     value component of the metadata.
   * @param value The metadata value in binary form.
   */
  void get_metadata(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /** Returns the number of array metadata items. */
  uint64_t metadata_num();

  /** Gets the type of the given metadata or nullopt if it does not exist. */
  std::optional<Datatype> metadata_type(const char* key);

  /** Retrieves the array metadata object. */
  Metadata& metadata();

  /**
   * Retrieves the array metadata object.
   *
   * @note This is potentially an unsafe operation
   * it could have contention with locks from lazy loading of metadata.
   * This should only be used by the serialization class
   * (tiledb/sm/serialization/array_schema_latest.cc). In that class we need to
   * fetch the underlying Metadata object to set the values we are loading from
   * REST. A lock should already by taken before load_metadata is called.
   */
  Metadata* unsafe_metadata();

  /** Set if array metadata is loaded already for this array or not */
  inline void set_metadata_loaded(const bool is_loaded) {
    opened_array_->metadata_loaded() = is_loaded;
  }

  /** Check if array metadata is loaded already for this array or not */
  inline bool metadata_loaded() {
    return opened_array_->metadata_loaded();
  }

  /** Check if non emtpy domain is loaded already for this array or not */
  inline bool non_empty_domain_computed() {
    return opened_array_->non_empty_domain_computed();
  }

  /**
   * Returns the non-empty domain of the opened array. If the non_empty_domain
   * has not been computed or loaded it will be loaded first.
   */
  const NDRange non_empty_domain();

  /**
   * Retrieves the non-empty domain from the opened array.
   * This is the union of the non-empty domains of the array fragments.
   *
   * @param domain The domain to be retrieved.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain(NDRange* domain, bool* is_empty);

  /**
   * Retrieves the non-empty domain from the opened array.
   * This is the union of the non-empty domains of the array fragments.
   *
   * @pre The array dimensions are numeric and of the same type.
   *
   * @param domain The domain to be retrieved.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain(void* domain, bool* is_empty);

  /**
   * Returns the non-empty domain of the opened array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   *
   * @param idx The dimension index.
   * @param domain The domain to be retrieved.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain_from_index(unsigned idx, void* domain, bool* is_empty);

  /**
   * Returns the non-empty domain size of the opened array on the given
   * dimension. This is the union of the non-empty domains of the array
   * fragments. Applicable only to var-sized dimensions.
   *
   * @param idx The dimension index.
   * @param start_size The size in bytes of the range start.
   * @param end_size The size in bytes of the range end.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain_var_size_from_index(
      unsigned idx, uint64_t* start_size, uint64_t* end_size, bool* is_empty);

  /**
   * Returns the non-empty domain of the opened array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * @param idx The dimension index.
   * @param start The domain range start to set.
   * @param end The domain range end to set.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain_var_from_index(
      unsigned idx, void* start, void* end, bool* is_empty);

  /**
   * Returns the non-empty domain of the opened array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   *
   * @param field_name The dimension name.
   * @param domain The domain to be retrieved.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain_from_name(
      std::string_view field_name, void* domain, bool* is_empty);

  /**
   * Returns the non-empty domain size of the open array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * @param field_name The dimension name.
   * @param start_size The size in bytes of the range start.
   * @param end_size The size in bytes of the range end.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain_var_size_from_name(
      std::string_view field_name,
      uint64_t* start_size,
      uint64_t* end_size,
      bool* is_empty);

  /**
   * Returns the non-empty domain of the opened array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.

   * @param field_name The dimension name.
   * @param start The domain range start to set.
   * @param end The domain range end to set.
   * @param is_empty `true` if the non-empty domain (and array) is empty.
   */
  void non_empty_domain_var_from_name(
      std::string_view field_name, void* start, void* end, bool* is_empty);

  /**
   * Retrieves the array metadata object that is already loaded. If it's not yet
   * loaded it will be empty.
   */
  inline NDRange& loaded_non_empty_domain() {
    return opened_array_->non_empty_domain();
  }

  /** Returns the non-empty domain of the opened array. */
  inline void set_non_empty_domain(const NDRange& non_empty_domain) {
    opened_array_->non_empty_domain() = non_empty_domain;
  }

  /** Set if the non_empty_domain is computed already for this array or not */
  inline void set_non_empty_domain_computed(const bool is_computed) {
    opened_array_->non_empty_domain_computed() = is_computed;
  }

  /** Returns the memory tracker. */
  inline shared_ptr<MemoryTracker> memory_tracker() {
    return memory_tracker_;
  }

  /**
   * Checks the config to see if non empty domain should be serialized on array
   * open.
   */
  bool serialize_non_empty_domain() const;

  /**
   * Checks the config to se if enumerations should be serialized on array open.
   */
  bool serialize_enumerations() const;

  /**
   * Checks the config to see if metadata should be serialized on array open.
   */
  bool serialize_metadata() const;

  /** Checks the config to see if refactored array open should be used. */
  bool use_refactored_array_open() const;

  /** Checks the config to see if refactored query submit should be used. */
  bool use_refactored_query_submit() const;

  /**
   * Sets the array state as open.
   *
   * @param query_type The QueryType of the Array.
   */
  void set_array_open(const QueryType& query_type);

  /**
   * Sets the array state as open, used in serialization
   */
  void set_serialized_array_open();

  /** Set the query type to open the array for. */
  void set_query_type(QueryType query_type);

  /**
   * Checks the array is open, in MODIFY_EXCLUSIVE mode, before deleting data.
   */
  void ensure_array_is_valid_for_delete(const URI& uri);

  /**
   * Returns a map of the computed average cell size for var size
   * dimensions/attributes.
   */
  std::unordered_map<std::string, uint64_t> get_average_var_cell_sizes() const;

  /** Load array directory for non-remote arrays */
  const ArrayDirectory& load_array_directory();

  /* Get the REST client */
  [[nodiscard]] inline shared_ptr<RestClient> rest_client() const {
    return resources_.rest_client();
  }

  /**
   * Upgrade a TileDB array to latest format version.
   *
   * @param resources The context resources.
   * @param uri The URI of the array.
   * @param config Configuration parameters for the upgrade
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   */
  static void upgrade_version(
      ContextResources& resources, const URI& uri, const Config& config);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** TileDB Context Resources. */
  ContextResources& resources_;

  /** The opened array that can be used by queries. */
  shared_ptr<OpenedArray> opened_array_;

  /** The array URI. */
  URI array_uri_;

  /** This is a backwards compatible URI from serialization
   *  In TileDB 2.5 we removed sending the URI but 2.4 and older were
   * unconditionally setting the URI, so things got set to an empty stirng Now
   * we store the serialized URI so we avoid the empty string with older
   * clients.
   */
  URI array_uri_serialized_;

  /** `True` if the array has been opened. */
  std::atomic<bool> is_open_;

  /** `True` if the array is currently in the process of opening or closing. */
  std::atomic<bool> is_opening_or_closing_;

  /** The query type the array was opened for. Default: READ */
  QueryType query_type_ = QueryType::READ;

  /**
   * Starting timestamp to open fragments between.
   *
   * Timestamps are ms elapsed since 1970-01-01 00:00:00 +0000 (UTC).
   */
  uint64_t array_dir_timestamp_start_;

  /**
   * Timestamp set by the user.
   *
   * This is used when setting the end timestamp for loading the array directory
   * and the timestamp to use when creating fragments, metadata, etc. This may
   * be changed by the user at any time.
   *
   * Timestamps are ms elapsed since 1970-01-01 00:00:00 +0000 (UTC). If set to
   * `nullopt`, use the current time.
   */
  optional<uint64_t> user_set_timestamp_end_;

  /**
   * Ending timestamp to open fragments between.
   *
   * Timestamps are ms elapsed since 1970-01-01 00:00:00 +0000 (UTC). Set to a
   * sentinel value of UINT64_MAX before the array is opened.
   */
  uint64_t array_dir_timestamp_end_;

  /**
   * The timestamp to use when creating fragments, delete/update commits,
   * metadata, etc.
   *
   * Timestamps are ms elapsed since 1970-01-01 00:00:00 +0000 (UTC). If set to
   * `nullopt`, use the current time.
   */
  optional<uint64_t> new_component_timestamp_;

  /** The array config. */
  Config config_;

  /** Stores the max buffer sizes requested last time by the user .*/
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>
      last_max_buffer_sizes_;

  /**
   * This is the last subarray used by the user to retrieve the
   * max buffer sizes.
   */
  std::vector<uint8_t> last_max_buffer_sizes_subarray_;

  /** True if the array is remote (has `tiledb://` URI scheme). */
  bool remote_;

  /** Memory tracker for the array. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** A reference to the object which controls the present Array instance. */
  ConsistencyController& consistency_controller_;

  /** Lifespan maintenance of an open array in the ConsistencyController. */
  std::optional<ConsistencySentry> consistency_sentry_;

  /**
   * Mutex that protects atomicity between the existence of the
   * ConsistencySentry registration and the is_open_ flag.
   */
  std::mutex mtx_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Opens an array for reads at a timestamp. All the metadata of the
   * fragments created before or at the input timestamp are retrieved.
   *
   * If a timestamp_start is provided, this API will open the array between
   * `timestamp_start` and `timestamp_end`.
   *
   * @param array The array to be opened.
   * @return tuple latest ArraySchema, map of all array schemas and
   * vector of FragmentMetadata
   *        ArraySchema The array schema to be retrieved after the
   *           array is opened.
   *        ArraySchemaMap Map of all array schemas found keyed by name
   *        fragment_metadata The fragment metadata to be retrieved
   *           after the array is opened.
   */
  tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>,
      std::vector<shared_ptr<FragmentMetadata>>>
  open_for_reads();

  /**
   * Opens an array for reads without fragments.
   *
   * @param array The array to be opened.
   * @return tuple of latest ArraySchema and map of all array schemas
   *        ArraySchema The array schema to be retrieved after the
   *          array is opened.
   *        ArraySchemaMap Map of all array schemas found keyed by name
   */
  tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>>
  open_for_reads_without_fragments();

  /**
   * Opens an array for writes.
   *
   * @param array The array to open.
   * @return tuple of latest ArraySchema and map of all array schemas
   *        ArraySchema The array schema to be retrieved after the
   *          array is opened.
   *        ArraySchemaMap Map of all array schemas found keyed by name
   */
  tuple<
      shared_ptr<ArraySchema>,
      std::unordered_map<std::string, shared_ptr<ArraySchema>>>
  open_for_writes();

  /** Clears the cached max buffer sizes and subarray. */
  void clear_last_max_buffer_sizes();

  /**
   * Computes the maximum buffer sizes for all attributes given a subarray,
   * which are cached locally in the instance.
   */
  Status compute_max_buffer_sizes(const void* subarray);

  /**
   * Computes an upper bound on the buffer sizes required for a read
   * query, for a given subarray and set of attributes. Note that
   * the attributes are already set in `max_buffer_sizes`
   *
   * @param subarray The subarray to focus on. Note that it must have the same
   *     underlying type as the array domain.
   * @param max_buffer_sizes The buffer sizes to be retrieved. This is a map
   * from the attribute (or coordinates) name to a pair of sizes (in bytes). For
   * fixed-sized attributes, the second size is ignored. For var-sized
   *     attributes, the first is the offsets size and the second is the
   *     values size.
   * @return Status
   */
  Status compute_max_buffer_sizes(
      const void* subarray,
      std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>*
          max_buffer_sizes_) const;

  /**
   * Load non-remote array metadata.
   */
  void do_load_metadata();

  /**
   * Load array metadata, handles remote arrays vs non-remote arrays
   * @return  Status
   */
  Status load_metadata();

  /**
   * Loads non empty domain from remote array
   * @return Status
   */
  Status load_remote_non_empty_domain();

  /** Computes the non-empty domain of the array. */
  Status compute_non_empty_domain();

  /** Sets the array state as closed.
   *
   * Note: the Sentry object will also be released upon Array destruction.
   **/
  void set_array_closed();
};

}  // namespace tiledb::sm

#endif  // TILEDB_ARRAY_H
