/**
 * @file   storage_manager_canonical.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/group/group.h"
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/storage_manager/context_resources.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class ArrayDirectory;
class ArraySchema;
class ArraySchemaEvolution;
class Consolidator;
class EncryptionKey;
class Query;
class RestClient;

enum class EncryptionType : uint8_t;

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

  /** Returns the current map of any set tags. */
  const std::unordered_map<std::string, std::string>& tags() const;

  /**
   * Creates a TileDB group.
   *
   * @param group The URI of the group to be created.
   * @return Status
   */
  Status group_create(const std::string& group);

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

  [[nodiscard]] inline ContextResources& resources() const {
    return resources_;
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

  /** Mutex for providing thread-safety upon creating TileDB objects. */
  std::mutex object_create_mtx_;

  /** Stores the TileDB configuration parameters. */
  Config config_;

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

}  // namespace tiledb::sm

#endif  // TILEDB_STORAGE_MANAGER_CANONICAL_H
