/**
 * @file   storage_manager_canonical.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines class StorageManagerCanonical.
 */

#ifndef TILEDB_STORAGE_MANAGER_CANONICAL_H
#define TILEDB_STORAGE_MANAGER_CANONICAL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/filtered_buffer.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class OpenedArray;
class ArrayDirectory;
class ArraySchema;
class ArraySchemaEvolution;
class Buffer;
class Consolidator;
class EncryptionKey;
class FragmentMetadata;
class FragmentInfo;
class GroupDetails;
class Metadata;
class MemoryTracker;
class Query;
class QueryCondition;
class RestClient;
class UpdateValue;
class VFS;

enum class EncryptionType : uint8_t;
enum class ObjectType : uint8_t;

/** The storage manager that manages pretty much everything in TileDB. */
class StorageManagerCanonical {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Enables iteration over TileDB objects in a path. */
  class ObjectIter {
   public:
    /**
     * There is a one-to-one correspondence between `expanded_` and `objs_`.
     * An `expanded_` value is `true` if the corresponding `objs_` path
     * has been expanded to the paths it contains in a post ored traversal.
     * This is not used in a preorder traversal.
     */
    std::list<bool> expanded_;
    /** The next URI in string format. */
    std::string next_;
    /** The next objects to be visited. */
    std::list<URI> objs_;
    /** The traversal order of the iterator. */
    WalkOrder order_;
    /** `True` if the iterator will recursively visit the directory tree. */
    bool recursive_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Complete, C.41-compliant constructor
   *
   * @param config The configuration parameters.
   */
  StorageManagerCanonical(
      ContextResources& resources,
      shared_ptr<Logger> logger,
      const Config& config);

 private:
  /**
   * Initializes the storage manager. Only used in the constructor.
   *
   * TODO: Integrate what this function does into the constructor directly.
   * TODO: Eliminate this function.
   *
   * @return Status
   */
  Status init();

 public:
  /** Destructor. */
  ~StorageManagerCanonical();

  DISABLE_COPY_AND_COPY_ASSIGN(StorageManagerCanonical);
  DISABLE_MOVE_AND_MOVE_ASSIGN(StorageManagerCanonical);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Closes an group opened for reads.
   *
   * @param group The group to be closed.
   * @return Status
   */
  Status group_close_for_reads(tiledb::sm::Group* group);

  /**
   * Closes an group opened for writes.
   *
   * @param group The group to be closed.
   * @return Status
   */
  Status group_close_for_writes(tiledb::sm::Group* group);

  /**
   * Loads the group metadata from persistent storage based on
   * the input URI manager.
   */
  void load_group_metadata(
      const tdb_shared_ptr<GroupDirectory>& group_dir,
      const EncryptionKey& encryption_key,
      Metadata* metadata);

  /**
   * Load a group detail from URI
   *
   * @param group_uri group uri
   * @param uri location to load
   * @param encryption_key encryption key
   * @return tuple Status and pointer to group deserialized
   */
  tuple<Status, optional<shared_ptr<GroupDetails>>> load_group_from_uri(
      const URI& group_uri,
      const URI& uri,
      const EncryptionKey& encryption_key);

  /**
   * Load a group detail from URIs
   *
   * @param group_uri group uri
   * @param uri location to load
   * @param encryption_key encryption key
   * @return tuple Status and pointer to group deserialized
   */
  tuple<Status, optional<shared_ptr<GroupDetails>>> load_group_from_all_uris(
      const URI& group_uri,
      const std::vector<TimestampedURI>& uris,
      const EncryptionKey& encryption_key);

  /**
   * Load group details based on group directory
   *
   * @param group_directory
   * @param encryption_key encryption key
   *
   * @return tuple Status and pointer to group deserialized
   */
  tuple<Status, optional<shared_ptr<GroupDetails>>> load_group_details(
      const shared_ptr<GroupDirectory>& group_directory,
      const EncryptionKey& encryption_key);

  /**
   * Store the group details
   *
   * @param group_detail_folder_uri group details folder
   * @param group_detail_uri uri for detail file to write
   * @param group to serialize and store
   * @param encryption_key encryption key for at-rest encryption
   * @return status
   */
  Status store_group_detail(
      const URI& group_detail_folder_uri,
      const URI& group_detail_uri,
      tdb_shared_ptr<GroupDetails> group,
      const EncryptionKey& encryption_key);

  /**
   * Opens an group for reads.
   *
   * @param group The group to be opened.
   * @return tuple of Status, latest GroupSchema and map of all group schemas
   *        Status Ok on success, else error
   */
  std::tuple<Status, std::optional<tdb_shared_ptr<GroupDetails>>>
  group_open_for_reads(Group* group);

  /** Opens an group for writes.
   *
   * @param group The group to open.
   * @return tuple of Status, latest GroupSchema and map of all group schemas
   *        Status Ok on success, else error
   */
  std::tuple<Status, std::optional<tdb_shared_ptr<GroupDetails>>>
  group_open_for_writes(Group* group);

  /**
   * Cleans up the array data.
   *
   * @param array_name The name of the array whose data is to be deleted.
   */
  void delete_array(const char* array_name);

  /**
   * Cleans up the array fragments.
   *
   * @param array_name The name of the array whose fragments are to be deleted.
   * @param timestamp_start The start timestamp at which to delete.
   * @param timestamp_end The end timestamp at which to delete.
   */
  void delete_fragments(
      const char* array_name, uint64_t timestamp_start, uint64_t timestamp_end);

  /**
   * Cleans up the group data.
   *
   * @param group_name The name of the group whose data is to be deleted.
   */
  void delete_group(const char* group_name);

  /**
   * Cleans up the array, such as its consolidated fragments and array
   * metadata. Note that this will coarsen the granularity of time traveling
   * (see docs for more information).
   *
   * @param array_name The name of the array to be vacuumed.
   * @param config Configuration parameters for vacuuming.
   */
  void array_vacuum(const char* array_name, const Config& config);

  /**
   * Creates a TileDB array storing its schema.
   *
   * @param array_uri The URI of the array to be created.
   * @param array_schema The array schema.
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status array_create(
      const URI& array_uri,
      const shared_ptr<ArraySchema>& array_schema,
      const EncryptionKey& encryption_key);

  /**
   * Evolve a TileDB array schema and store its new schema.
   *
   * @param array_dir The ArrayDirectory object used to retrieve the
   *     various URIs in the array directory.
   * @param schema_evolution The schema evolution.
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status array_evolve_schema(
      const URI& uri,
      ArraySchemaEvolution* array_schema,
      const EncryptionKey& encryption_key);

  /**
   * Upgrade a TileDB array to latest format version.
   *
   * @param array_dir The ArrayDirectory object used to retrieve the
   *     various URIs in the array directory.
   * @param config Configuration parameters for the upgrade
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   * @return Status
   */
  Status array_upgrade_version(const URI& uri, const Config& config);

  /**
   * Retrieves the non-empty domain from an array. This is the union of the
   * non-empty domains of the array fragments.
   *
   * @param array An open array object (must be already open).
   * @param domain The domain to be retrieved.
   * @param is_empty `true` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain(
      Array* array, NDRange* domain, bool* is_empty);

  /**
   * Retrieves the non-empty domain from an array. This is the union of the
   * non-empty domains of the array fragments.
   *
   * @param array An open array object (must be already open).
   * @param domain The domain to be retrieved.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain(Array* array, void* domain, bool* is_empty);

  /**
   * Retrieves the non-empty domain from an array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   *
   * @param array An open array object (must be already open).
   * @param idx The dimension index.
   * @param domain The domain to be retrieved.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain_from_index(
      Array* array, unsigned idx, void* domain, bool* is_empty);

  /**
   * Retrieves the non-empty domain from an array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   *
   * @param array An open array object (must be already open).
   * @param name The dimension name.
   * @param domain The domain to be retrieved.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain_from_name(
      Array* array, const char* name, void* domain, bool* is_empty);

  /**
   * Retrieves the non-empty domain size from an array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * @param array An open array object (must be already open).
   * @param idx The dimension index.
   * @param start_size The size in bytes of the range start.
   * @param end_size The size in bytes of the range end.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain_var_size_from_index(
      Array* array,
      unsigned idx,
      uint64_t* start_size,
      uint64_t* end_size,
      bool* is_empty);

  /**
   * Retrieves the non-empty domain size from an array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * @param array An open array object (must be already open).
   * @param name The dimension name.
   * @param start_size The size in bytes of the range start.
   * @param end_size The size in bytes of the range end.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain_var_size_from_name(
      Array* array,
      const char* name,
      uint64_t* start_size,
      uint64_t* end_size,
      bool* is_empty);

  /**
   * Retrieves the non-empty domain from an array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * @param array An open array object (must be already open).
   * @param idx The dimension index.
   * @param start The domain range start to set.
   * @param end The domain range end to set.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain_var_from_index(
      Array* array, unsigned idx, void* start, void* end, bool* is_empty);

  /**
   * Retrieves the non-empty domain from an array on the given dimension.
   * This is the union of the non-empty domains of the array fragments.
   * Applicable only to var-sized dimensions.
   *
   * @param array An open array object (must be already open).
   * @param name The dimension name.
   * @param start The domain range start to set.
   * @param end The domain range end to set.
   * @param is_empty `ture` if the non-empty domain is empty (the array
   *     is empty).
   * @return Status
   */
  Status array_get_non_empty_domain_var_from_name(
      Array* array, const char* name, void* start, void* end, bool* is_empty);

  /**
   * Retrieves the encryption type from an array.
   *
   * @param array_dir The ArrayDirectory object used to retrieve the
   *     various URIs in the array directory.
   * @param encryption_type Set to the encryption type of the array.
   * @return Status
   */
  Status array_get_encryption(
      const ArrayDirectory& array_dir, EncryptionType* encryption_type);

  /**
   * Pushes an async query to the queue.
   *
   * @param query The async query.
   * @return Status
   */
  Status async_push_query(Query* query);

  /** Cancels all background tasks. */
  Status cancel_all_tasks();

  /** Returns true while all tasks are being cancelled. */
  bool cancellation_in_progress();

  /** Returns the configuration parameters. */
  const Config& config() const;

  /** Returns the current map of any set tags. */
  const std::unordered_map<std::string, std::string>& tags() const;

  /**
   * Creates a TileDB group.
   *
   * @param group The URI of the group to be created.
   * @return Status
   */
  Status group_create(const std::string& group);

  /** Returns the thread pool for compute-bound tasks. */
  [[nodiscard]] inline ThreadPool* compute_tp() const {
    return &(resources_.compute_tp());
  }

  /** Returns the thread pool for io-bound tasks. */
  [[nodiscard]] inline ThreadPool* io_tp() const {
    return &(resources_.io_tp());
  }

  /**
   * If the storage manager was configured with a REST server, return the
   * client instance. Else, return nullptr.
   */
  inline RestClient* rest_client() const {
    return resources_.rest_client().get();
  }

  /**
   * Checks if the input URI represents an array.
   *
   * @param The URI to be checked.
   * @return bool
   */
  bool is_array(const URI& uri) const;

  /**
   * Checks if the input URI represents a group.
   *
   * @param The URI to be checked.
   * @param is_group Set to `true` if the URI is a group and `false`
   *     otherwise.
   * @return Status
   */
  Status is_group(const URI& uri, bool* is_group) const;

  /**
   * Loads the delete and update conditions from storage.
   *
   * @param opened_array The opened array.
   * @return Status, vector of the conditions, vector of the update values.
   */
  tuple<
      Status,
      optional<std::vector<QueryCondition>>,
      optional<std::vector<std::vector<UpdateValue>>>>
  load_delete_and_update_conditions(const OpenedArray& opened_array);

  /** Removes a TileDB object (group, array). */
  Status object_remove(const char* path) const;

  /**
   * Renames a TileDB object (group, array). If
   * `new_path` exists, `new_path` will be overwritten.
   */
  Status object_move(const char* old_path, const char* new_path) const;

  /**
   * Creates a new object iterator for the input path. The iteration
   * in this case will be recursive in the entire directory tree rooted
   * at `path`.
   *
   * @param obj_iter The object iterator to be created (memory is allocated for
   *     it by the function).
   * @param path The path the iterator will target at.
   * @param order The traversal order of the iterator.
   * @return Status
   */
  Status object_iter_begin(
      ObjectIter** obj_iter, const char* path, WalkOrder order);

  /**
   * Creates a new object iterator for the input path. The iteration will
   * not be recursive, and only the children of `path` will be visited.
   *
   * @param obj_iter The object iterator to be created (memory is allocated for
   *     it by the function).
   * @param path The path the iterator will target at.
   * @return Status
   */
  Status object_iter_begin(ObjectIter** obj_iter, const char* path);

  /** Frees the object iterator. */
  void object_iter_free(ObjectIter* obj_iter);

  /**
   * Retrieves the next object path and type.
   *
   * @param obj_iter The object iterator.
   * @param path The object path that is retrieved.
   * @param type The object type that is retrieved.
   * @param has_next True if an object path was retrieved and false otherwise.
   * @return Status
   */
  Status object_iter_next(
      ObjectIter* obj_iter,
      const char** path,
      ObjectType* type,
      bool* has_next);

  /**
   * Retrieves the next object in the post-order traversal.
   *
   * @param obj_iter The object iterator.
   * @param path The object path that is retrieved.
   * @param type The object type that is retrieved.
   * @param has_next True if an object path was retrieved and false otherwise.
   * @return Status
   */
  Status object_iter_next_postorder(
      ObjectIter* obj_iter,
      const char** path,
      ObjectType* type,
      bool* has_next);

  /**
   * Retrieves the next object in the post-order traversal.
   *
   * @param obj_iter The object iterator.
   * @param path The object path that is retrieved.
   * @param type The object type that is retrieved.
   * @param has_next True if an object path was retrieved and false otherwise.
   * @return Status
   */
  Status object_iter_next_preorder(
      ObjectIter* obj_iter,
      const char** path,
      ObjectType* type,
      bool* has_next);

  /**
   * Returns the tiledb object type
   *
   * @param uri Path to TileDB object resource
   * @param type The ObjectType to be retrieved.
   * @return Status
   */
  Status object_type(const URI& uri, ObjectType* type) const;

  /** Submits a query for (sync) execution. */
  Status query_submit(Query* query);

  /**
   * Submits a query for async execution.
   *
   * @param query The query to submit.
   * @return Status
   */
  Status query_submit_async(Query* query);

  /**
   * Sets a string/string KV "tag" on the storage manager instance.
   *
   * This is currently only meant for internal TileDB Inc. usage.
   *
   * @param key Tag key
   * @param value Tag value
   * @return Status
   */
  Status set_tag(const std::string& key, const std::string& value);

  /**
   * Stores an array schema into persistent storage.
   *
   * @param array_schema The array metadata to be stored.
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status store_array_schema(
      const shared_ptr<ArraySchema>& array_schema,
      const EncryptionKey& encryption_key);

  /**
   * Stores the metadata into persistent storage.
   *
   * @param uri The object URI.
   * @param encryption_key The encryption key to use.
   * @param metadata The  metadata.
   * @return Status
   */
  Status store_metadata(
      const URI& uri, const EncryptionKey& encryption_key, Metadata* metadata);

  /**
   * Stores data into persistent storage.
   *
   * @param tile Tile to store.
   * @param uri The object URI.
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status store_data_to_generic_tile(
      WriterTile& tile, const URI& uri, const EncryptionKey& encryption_key);

  [[nodiscard]] inline ContextResources& resources() const {
    return resources_;
  }

  /** Returns the virtual filesystem object. */
  [[nodiscard]] inline VFS* vfs() const {
    return &(resources_.vfs());
  }

  /** Returns `stats_`. */
  [[nodiscard]] inline stats::Stats* stats() {
    return &(resources_.stats());
  }

  /** Returns the internal logger object. */
  shared_ptr<Logger> logger() const;

  /**
   * Consolidates the metadata of a group into a single file.
   *
   * @param group_name The name of the group whose metadata will be
   *     consolidated.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   * @return Status
   */
  Status group_metadata_consolidate(
      const char* group_name, const Config& config);

  /**
   * Vacuums the consolidated metadata files of a group.
   *
   * @param group_name The name of the group whose metadata will be
   *     vacuumed.
   * @param config Configuration parameters for vacuuming
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   */
  void group_metadata_vacuum(const char* group_name, const Config& config);

 private:
  /* ********************************* */
  /*        PRIVATE DATATYPES          */
  /* ********************************* */

  /**
   * Helper RAII struct that increments 'queries_in_progress' in the constructor
   * and decrements in the destructor, on the given StorageManagerCanonical
   * instance.
   *
   * This ensures that the counter is decremented even in the case of
   * exceptions.
   */
  struct QueryInProgress {
    /** The StorageManagerCanonical instance. */
    StorageManagerCanonical* sm;

    /**
     * Constructor. Calls increment_in_progress() on given
     * StorageManagerCanonical.
     */
    QueryInProgress(StorageManagerCanonical* sm)
        : sm(sm) {
      sm->increment_in_progress();
    }

    /**
     * Destructor. Calls decrement_in_progress() on given
     * StorageManagerCanonical.
     */
    ~QueryInProgress() {
      sm->decrement_in_progress();
    }
  };

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

  /** Context create resources object. */
  ContextResources& resources_;

  /** The class logger. */
  shared_ptr<Logger> logger_;

  /** Set to true when tasks are being cancelled. */
  bool cancellation_in_progress_;

  /** Mutex protecting cancellation_in_progress_. */
  std::mutex cancellation_in_progress_mtx_;

  /**
   * The condition variable for exlcusively locking arrays. This is used
   * to wait for an array to be closed, before being exclusively locked
   * by `array_xlock`.
   */
  std::condition_variable xlock_cv_;

  /** Mutex for providing thread-safety upon creating TileDB objects. */
  std::mutex object_create_mtx_;

  /** Stores the TileDB configuration parameters. */
  Config config_;

  /** Keeps track of which groups are open. */
  std::set<Group*> open_groups_;

  /** Mutex for managing open groups. */
  std::mutex open_groups_mtx_;

  /** Count of the number of queries currently in progress. */
  uint64_t queries_in_progress_;

  /** Guards queries_in_progress_ counter. */
  std::mutex queries_in_progress_mtx_;

  /** Guards queries_in_progress_ counter. */
  std::condition_variable queries_in_progress_cv_;

  /** Tracks all scheduled tasks that can be safely cancelled before execution.
   */
  CancelableTasks cancelable_tasks_;

  /** Tags for the context object. */
  std::unordered_map<std::string, std::string> tags_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /** Decrement the count of in-progress queries. */
  void decrement_in_progress();

  /** Increment the count of in-progress queries. */
  void increment_in_progress();

  /** Block until there are zero in-progress queries. */
  void wait_for_zero_in_progress();

  /** Sets default tag values on this StorageManagerCanonical. */
  Status set_default_tags();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STORAGE_MANAGER_CANONICAL_H
