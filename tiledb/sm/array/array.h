/**
 * @file   array.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array/consistency.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/fragment/fragment_info.h"
#include "tiledb/sm/metadata/metadata.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ArraySchema;
class SchemaEvolution;
class FragmentMetadata;
enum class QueryType : uint8_t;

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
      const URI& array_uri,
      StorageManager* storage_manager,
      ConsistencyController& cc = controller());

  /** Destructor. */
  ~Array() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(Array);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Array);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the array directory object. */
  const ArrayDirectory& array_directory() const;

  /** Set the array directory. */
  void set_array_directory(const ArrayDirectory& dir) {
    array_dir_ = dir;
  }

  /** Sets the latest array schema.
   * @param array_schema The array schema to set.
   */
  void set_array_schema_latest(const shared_ptr<ArraySchema>& array_schema);

  /** Returns the latest array schema. */
  const ArraySchema& array_schema_latest() const;

  /** Returns the latest array schema as a shared pointer. */
  shared_ptr<const ArraySchema> array_schema_latest_ptr() const;

  /** Returns array schemas map. */
  inline const std::unordered_map<std::string, shared_ptr<ArraySchema>>&
  array_schemas_all() const {
    return array_schemas_all_;
  }

  /**
   * Sets all array schemas.
   * @param all_schemas The array schemas to set.
   */
  void set_array_schemas_all(
      std::unordered_map<std::string, shared_ptr<ArraySchema>>& all_schemas);

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Returns the serialized array URI, this is for backwards compatibility with
   * serialization in pre TileDB 2.4 */
  const URI& array_uri_serialized() const;

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
   * @return Status
   */
  Status load_fragments(const std::vector<TimestampedURI>& fragments_to_load);

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
   * Deletes the Array data with given URI.
   *
   * @param uri The uri of the Array whose data is to be deleted.
   *
   * @pre The Array must be open for exclusive writes
   */
  void delete_array(const URI& uri);

  /**
   * Deletes the fragments from the Array with given URI.
   *
   * @param uri The uri of the Array whose fragments are to be deleted.
   * @param timestamp_start The start timestamp at which to delete fragments.
   * @param timestamp_end The end timestamp at which to delete fragments.
   *
   * @pre The Array must be open for exclusive writes
   */
  void delete_fragments(
      const URI& uri, uint64_t timestamp_start, uint64_t timstamp_end);

  /**
   * Deletes the fragments with the given URIs from the Array with given URI.
   *
   * @param uri The uri of the Array whose fragments are to be deleted.
   * @param fragment_uris The uris of the fragments to be deleted.
   *
   * @pre The Array must be open for exclusive writes
   */
  void delete_fragments_list(
      const URI& uri, const std::vector<URI>& fragment_uris);

  /** Returns a constant pointer to the encryption key. */
  const EncryptionKey* encryption_key() const;

  /**
   * Returns the fragment metadata of the array. If the array is not open,
   * an empty vector is returned.
   */
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata() const;

  /**
   * Accessor to the fragment metadata of the array.
   */
  inline std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata() {
    return fragment_metadata_;
  }

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
   * Load all enumerations for the array.
   *
   * Ensure that all enumerations have been loaded. If latest_only is true
   * (the default) then only enumerations for the latest schema are loaded.
   * When latest_only is false, all schemas have their enumerations loaded.
   *
   * @param latest_only Whether to load enumerations for just the latest
   * schema or all schemas.
   */
  void load_all_enumerations(bool latest_only = true);

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
  const EncryptionKey& get_encryption_key() const;

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

  /** Returns the start timestamp. */
  uint64_t timestamp_start() const;

  /** Returns the end timestamp. */
  uint64_t timestamp_end() const;

  /** Returns the timestamp at which the array was opened. */
  uint64_t timestamp_end_opened_at() const;

  /** Directly set the timestamp start value. */
  Status set_timestamp_start(uint64_t timestamp_start);

  /** Directly set the timestamp end value. */
  Status set_timestamp_end(uint64_t timestamp_end);

  /** Directly set the timestamp end opened at value. */
  Status set_timestamp_end_opened_at(const uint64_t timestamp_end_opened_at);

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
  Config config() const;

  /** Directly set the array URI for serialized compatibility with pre
   * TileDB 2.5 clients */
  Status set_uri_serialized(const std::string& uri);

  /** Sets the array URI. */
  void set_array_uri(const URI& array_uri) {
    array_uri_ = array_uri;
  }

  /**
   * Deletes metadata from an array opened in WRITE mode.
   *
   * @param key The key of the metadata item to be deleted.
   * @return Status
   */
  Status delete_metadata(const char* key);

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
   * @return Status
   */
  Status put_metadata(
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
   * @return Status
   */
  Status get_metadata(
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
   * @return Status
   */
  Status get_metadata(
      uint64_t index,
      const char** key,
      uint32_t* key_len,
      Datatype* value_type,
      uint32_t* value_num,
      const void** value);

  /** Returns the number of array metadata items. */
  Status get_metadata_num(uint64_t* num);

  /** Sets has_key == 1 and corresponding value_type if the array has key. */
  Status has_metadata_key(const char* key, Datatype* value_type, bool* has_key);

  /** Retrieves the array metadata object. */
  Status metadata(Metadata** metadata);

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
    metadata_loaded_ = is_loaded;
  }

  /** Check if array metadata is loaded already for this array or not */
  inline bool& metadata_loaded() {
    return metadata_loaded_;
  }

  /** Check if non emtpy domain is loaded already for this array or not */
  inline bool& non_empty_domain_computed() {
    return non_empty_domain_computed_;
  }

  /** Returns the non-empty domain of the opened array.
   *  If the non_empty_domain has not been computed or loaded
   *  it will be loaded first
   * */
  tuple<Status, optional<const NDRange>> non_empty_domain();

  /**
   * Retrieves the array metadata object that is already loadad.
   * If it's not yet loaded it will be empty.
   */
  NDRange* loaded_non_empty_domain();

  /** Returns the non-empty domain of the opened array. */
  void set_non_empty_domain(const NDRange& non_empty_domain);

  /** Set if the non_empty_domain is computed already for this array or not */
  inline void set_non_empty_domain_computed(const bool is_computed) {
    non_empty_domain_computed_ = is_computed;
  }

  /** Returns the memory tracker. */
  MemoryTracker* memory_tracker();

  /**
   * Checks the config to see if non empty domain should be serialized on array
   * open. */
  bool serialize_non_empty_domain() const;

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
  inline void set_query_type(QueryType query_type) {
    query_type_ = query_type;
  }

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
  ArrayDirectory& load_array_directory();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The latest array schema. */
  shared_ptr<ArraySchema> array_schema_latest_;

  /**
   * A map of all array_schemas_all
   */
  std::unordered_map<std::string, shared_ptr<ArraySchema>> array_schemas_all_;

  /** The array URI. */
  URI array_uri_;

  /** The array directory object for listing URIs. */
  ArrayDirectory array_dir_;

  /** This is a backwards compatible URI from serialization
   *  In TileDB 2.5 we removed sending the URI but 2.4 and older were
   * unconditionally setting the URI, so things got set to an empty stirng Now
   * we store the serialized URI so we avoid the empty string with older
   * clients.
   */
  URI array_uri_serialized_;

  /**
   * The private encryption key used to encrypt the array.
   *
   * Note: This is the only place in TileDB where the user's private key
   * bytes should be stored. Whenever a key is needed, a pointer to this
   * memory region should be passed instead of a copy of the bytes.
   */
  shared_ptr<EncryptionKey> encryption_key_;

  /** The metadata of the fragments the array was opened with. */
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata_;

  /** `True` if the array has been opened. */
  std::atomic<bool> is_open_;

  /** `True` if the array is currently in the process of opening or closing. */
  std::atomic<bool> is_opening_or_closing_;

  /** The query type the array was opened for. Default: READ */
  QueryType query_type_ = QueryType::READ;

  /**
   * The starting timestamp between to open `open_array_` at.
   * In TileDB, timestamps are in ms elapsed since
   * 1970-01-01 00:00:00 +0000 (UTC).
   */
  uint64_t timestamp_start_;

  /**
   * The ending timestamp between to open `open_array_` at.
   * In TileDB, timestamps are in ms elapsed since
   * 1970-01-01 00:00:00 +0000 (UTC). A value of UINT64_T
   * will be interpretted as the current timestamp.
   */
  uint64_t timestamp_end_;

  /**
   * The ending timestamp that the array was last opened
   * at. This is useful when `timestamp_end_` has been
   * set to UINT64_T. In this scenario, this variable will
   * store the timestamp for the time that the array was
   * opened.
   */
  uint64_t timestamp_end_opened_at_;

  /** TileDB storage manager. */
  StorageManager* storage_manager_;

  /** TileDB Context Resources. */
  ContextResources& resources_;

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

  /** The array metadata. */
  Metadata metadata_;

  /** True if the array metadata is loaded. */
  bool metadata_loaded_;

  /** True if the non_empty_domain has been computed */
  bool non_empty_domain_computed_;

  /** The non-empty domain of the array. */
  NDRange non_empty_domain_;

  /** Memory tracker for the array. */
  MemoryTracker memory_tracker_;

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
   * @return tuple of Status, latest ArraySchema, map of all array schemas and
   * vector of FragmentMetadata
   *        Status Ok on success, else error
   *        ArraySchema The array schema to be retrieved after the
   *           array is opened.
   *        ArraySchemaMap Map of all array schemas found keyed by name
   *        fragment_metadata The fragment metadata to be retrieved
   *           after the array is opened.
   */
  tuple<
      optional<shared_ptr<ArraySchema>>,
      optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>,
      optional<std::vector<shared_ptr<FragmentMetadata>>>>
  open_for_reads();

  /**
   * Opens an array for reads without fragments.
   *
   * @param array The array to be opened.
   * @return tuple of Status, latest ArraySchema and map of all array schemas
   *        Status Ok on success, else error
   *        ArraySchema The array schema to be retrieved after the
   *          array is opened.
   *        ArraySchemaMap Map of all array schemas found keyed by name
   */
  tuple<
      optional<shared_ptr<ArraySchema>>,
      optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>>
  open_for_reads_without_fragments();

  /** Opens an array for writes.
   *
   * @param array The array to open.
   * @return tuple of Status, latest ArraySchema and map of all array schemas
   *        Status Ok on success, else error
   *        ArraySchema The array schema to be retrieved after the
   *          array is opened.
   *        ArraySchemaMap Map of all array schemas found keyed by name
   */
  tuple<
      Status,
      optional<shared_ptr<ArraySchema>>,
      optional<std::unordered_map<std::string, shared_ptr<ArraySchema>>>>
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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_H
