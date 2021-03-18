/**
 * @file   storage_manager.h
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
 * This file defines class StorageManager.
 */

#ifndef TILEDB_STORAGE_MANAGER_H
#define TILEDB_STORAGE_MANAGER_H

#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

#ifdef HAVE_TBB
#include <tbb/task_scheduler_init.h>
#endif

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/walk_order.h"
#include "tiledb/sm/filesystem/filelock.h"
#include "tiledb/sm/fragment/single_fragment_info.h"
#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/sm/misc/uri.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class Buffer;
class BufferLRUCache;
class ChunkedBuffer;
class Consolidator;
class EncryptionKey;
class FragmentMetadata;
class FragmentInfo;
class Metadata;
class OpenArray;
class Query;
class RestClient;
class VFS;

enum class EncryptionType : uint8_t;
enum class ObjectType : uint8_t;

/** The storage manager that manages pretty much everything in TileDB. */
class StorageManager {
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

  /** Constructor. */
  StorageManager(ThreadPool* compute_tp, ThreadPool* io_tp);

  /** Destructor. */
  ~StorageManager();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Closes an array opened for reads.
   *
   * @param array_uri The array URI
   * @return Status
   */
  Status array_close_for_reads(const URI& array_uri);

  /**
   * Closes an array opened for writes.
   *
   * @param array_uri The array URI
   * @param encryption_key The array encryption key.
   * @param array_metadata The array metadata, which the function
   *     flush to persistent storage.
   * @return Status
   */
  Status array_close_for_writes(
      const URI& array_uri,
      const EncryptionKey& encryption_key,
      Metadata* array_metadata);

  /**
   * Opens an array for reads at a timestamp. All the metadata of the
   * fragments created before or at the input timestamp are retrieved.
   *
   * @param array_uri The array URI.
   * @param timestamp The timestamp at which the array will be opened.
   *     In TileDB, timestamps are in ms elapsed since
   *     1970-01-01 00:00:00 +0000 (UTC).
   * @param enc_key The encryption key to use.
   * @param array_schema The array schema to be retrieved after the
   *     array is opened.
   * @param fragment_metadata The fragment metadata to be retrieved
   *     after the array is opened.
   * @return Status
   */
  Status array_open_for_reads(
      const URI& array_uri,
      uint64_t timestamp,
      const EncryptionKey& enc_key,
      ArraySchema** array_schema,
      std::vector<FragmentMetadata*>* fragment_metadata);

  /**
   * Opens an array for reads, focusing only on a given list of fragments.
   * Only the metadata of the input fragments are retrieved.
   *
   * @param array_uri The array URI.
   * @param fragment_info Info about the fragments to open the array with.
   * @param enc_key The encryption key to use.
   * @param array_schema The array schema to be retrieved after the
   *     array is opened.
   * @param fragment_metadata The fragment metadata to be retrieved
   *     after the array is opened.
   * @return Status
   */
  Status array_open_for_reads(
      const URI& array_uri,
      const FragmentInfo& fragment_info,
      const EncryptionKey& enc_key,
      ArraySchema** array_schema,
      std::vector<FragmentMetadata*>* fragment_metadata);

  /** Opens an array for writes.
   *
   * @param array_uri The array URI.
   * @param enc_key The encryption key.
   * @param array_schema The array schema to be retrieved after the
   *     array is opened.
   * @return
   */
  Status array_open_for_writes(
      const URI& array_uri,
      const EncryptionKey& enc_key,
      ArraySchema** array_schema);

  /**
   * Reopens an already open array at a potentially new timestamp,
   * retrieving the fragment metadata of any new fragments written
   * in the array.
   *
   * @param array_uri The array URI.
   * @param timestamp The timestamp at which the array will be opened.
   *     In TileDB, timestamps are in ms elapsed since
   *     1970-01-01 00:00:00 +0000 (UTC).
   * @param enc_key The encryption key to use.
   * @param array_schema The array schema to be retrieved after the
   *     array is opened.
   * @param fragment_metadata The fragment metadata to be retrieved
   *     after the array is opened.
   * @return Status
   */
  Status array_reopen(
      const URI& array_uri,
      uint64_t timestamp,
      const EncryptionKey& enc_key,
      ArraySchema** array_schema,
      std::vector<FragmentMetadata*>* fragment_metadata);

  /**
   * Consolidates the fragments of an array into a single one.
   *
   * @param array_name The name of the array to be consolidated.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   * @return Status
   */
  Status array_consolidate(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      const Config* config);

  /**
   * Cleans up the array, such as its consolidated fragments and array
   * metadata. Note that this will coarsen the granularity of time traveling
   * (see docs for more information).
   *
   * @param array_name The name of the array to be vacuumed.
   * @param config Configuration parameters for vacuuming.
   * @return Status
   */
  Status array_vacuum(const char* array_name, const Config* config);

  /**
   * Cleans up fragments that took part in consolidation. Note that this
   * will coarsen the granularity of time traveling (see docs for more
   * information).
   *
   * @param array_name The name of the array to be vacuumed.
   * @return Status
   */
  Status array_vacuum_fragments(const char* array_name);

  /**
   * Cleans up consolidated fragment metadata (all except the last one).
   *
   * @param array_name The name of the array to be consolidated.
   * @return Status
   */
  Status array_vacuum_fragment_meta(const char* array_name);

  /**
   * Cleans up consolidated array metadata.
   *
   * @param array_name The name of the array to be consolidated.
   * @return Status
   */
  Status array_vacuum_array_meta(const char* array_name);

  /**
   * Consolidates the metadata of an array into a single file.
   *
   * @param array_name The name of the array whose metadata will be
   *     consolidated.
   * @param encryption_type The encryption type of the array
   * @param encryption_key If the array is encrypted, the private encryption
   *    key. For unencrypted arrays, pass `nullptr`.
   * @param key_length The length in bytes of the encryption key.
   * @param config Configuration parameters for the consolidation
   *     (`nullptr` means default, which will use the config associated with
   *      this instance).
   * @return Status
   */
  Status array_metadata_consolidate(
      const char* array_name,
      EncryptionType encryption_type,
      const void* encryption_key,
      uint32_t key_length,
      const Config* config);

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
      ArraySchema* array_schema,
      const EncryptionKey& encryption_key);

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
   * @param array_uri URI of the array
   * @param encryption_type Set to the encryption type of the array.
   * @return Status
   */
  Status array_get_encryption(
      const std::string& array_uri, EncryptionType* encryption_type);

  /**
   * Exclusively locks an array preventing it from being opened in
   * read mode. This function will wait on the array to
   * be closed if it is already open (always in read mode). After an array
   * is xlocked, any attempt to open an array in read mode will have to wait
   * until the array is unlocked with `xunlock_array`.
   *
   * An array is exclusively locked only for a short time upon consolidation,
   * during removing the directories of the old fragments that got consolidated.
   *
   * @note Arrays that are opened in write mode need not be xlocked. The
   *     reason is that the `OpenArray` objects created when opening
   *     in write mode do not store any fragment metadata and, hence,
   *     are not affected by a potentially concurrent consolidator deleting
   *     fragment directories.
   */
  Status array_xlock(const URI& array_uri);

  /** Releases an exclusive lock for the input array. */
  Status array_xunlock(const URI& array_uri);

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

  /** Creates a directory with the input URI. */
  Status create_dir(const URI& uri);

  /** Creates an empty file with the input URI. */
  Status touch(const URI& uri);

  /**
   * Gets the fragment information for a given array at a particular
   * timestamp.
   *
   * @param array_schema The array schema.
   * @param timestamp The function will consider fragments created
   *     at or before this timestamp.
   * @param encryption_key The encryption key in case the array is encrypted.
   * @param fragment_info The fragment information to be retrieved.
   *     The fragments are sorted in chronological creation order.
   * @param get_to_vacuum Whether or not to receive information about
   *     fragments to vacuum.
   * @return Status
   */
  Status get_fragment_info(
      const ArraySchema* array_schema,
      uint64_t timestamp,
      const EncryptionKey& encryption_key,
      FragmentInfo* fragment_info,
      bool get_to_vacuum = false);

  /**
   * Gets the fragment information for a given array at a particular
   * timestamp.
   *
   * @param array_uri The array URI.
   * @param timestamp The function will consider fragments created
   *     at or before this timestamp.
   * @param encryption_key The encryption key in case the array is encrypted.
   * @param fragment_info The fragment information to be retrieved.
   *     The fragments are sorted in chronological creation order.
   * @param get_to_vacuum Whether or not to receive information about
   *     fragments to vacuum.
   * @return Status
   */
  Status get_fragment_info(
      const URI& array_uri,
      uint64_t timestamp,
      const EncryptionKey& encryption_key,
      FragmentInfo* fragment_info,
      bool get_to_vacuum = false);

  /**
   * Gets the fragment info for a single fragment URI.
   *
   * @param array_schema The array schema.
   * @param encryption_key The encryption key.
   * @param fragment_uri The fragment URI.
   * @param fragment_info The fragment info to retrieve.
   * @return Status
   */
  Status get_fragment_info(
      const ArraySchema* array_schema,
      const EncryptionKey& encryption_key,
      const URI& fragment_uri,
      SingleFragmentInfo* fragment_info);

  /**
   * Retrieves all the fragment URIs of an array, along with the latest
   * consolidated fragment metadata URI `meta_uri`.
   */
  Status get_fragment_uris(
      const URI& array_uri,
      std::vector<URI>* fragment_uris,
      URI* meta_uri) const;

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
   * Initializes the storage manager.
   *
   * @param config The configuration parameters.
   * @return Status
   */
  Status init(const Config* config);

  /** Returns the thread pool for compute-bound tasks. */
  ThreadPool* compute_tp();

  /** Returns the thread pool for io-bound tasks. */
  ThreadPool* io_tp();

  /**
   * If the storage manager was configured with a REST server, return the
   * client instance. Else, return nullptr.
   */
  RestClient* rest_client() const;

  /**
   * Checks if the input URI represents an array.
   *
   * @param The URI to be checked.
   * @param is_array Set to `true` if the URI is an array and `false` otherwise.
   * @return Status
   */
  Status is_array(const URI& uri, bool* is_array) const;

  /**
   * Checks if the input URI represents a directory.
   *
   * @param The URI to be checked.
   * @param is_dir Set to `true` if the URI is a directory and `false`
   *     otherwise.
   * @return Status
   */
  Status is_dir(const URI& uri, bool* is_dir) const;

  /**
   * Checks if the input URI represents a fragment. The functions takes into
   * account the fragment version, which is retrived directly from the URI.
   * For versions >= 5, the function checks whether the URI is included
   * in `ok_uris`. For versions < 5, `ok_uris` is empty so the function
   * checks for the existence of the fragment metadata file in the fragment
   * URI directory. Therefore, the function is more expensive for earlier
   * fragment versions.
   *
   * @param The URI to be checked.
   * @param ok_uris For checking URI existence of versions >= 5.
   * @param is_fragment Set to `1` if the URI is a fragment and `0`
   *     otherwise.
   * @return Status
   */
  Status is_fragment(
      const URI& uri, const std::set<URI>& ok_uris, int* is_fragment) const;

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
   * Checks if the input URI represents a file.
   *
   * @param The URI to be checked.
   * @param is_file Set to `true` if the URI is a file and `false`
   *     otherwise.
   * @return Status
   */
  Status is_file(const URI& uri, bool* is_file) const;

  /**
   * Check if a URI is a vacuum file or not based on the file suffix
   * @param uri
   * @return true is vacuum file, false otherwise
   */
  bool is_vacuum_file(const URI& uri) const;

  /**
   * Loads the schema of an array from persistent storage into memory.
   *
   * @param array_uri The URI path of the array.
   * @param encryption_key The encryption key to use.
   * @param array_schema The array schema to be retrieved.
   * @return Status
   */
  Status load_array_schema(
      const URI& array_uri,
      const EncryptionKey& encryption_key,
      ArraySchema** array_schema);

  /**
   * Loads the array metadata from persistent storage that were created
   * at or before `timestamp`.
   */
  Status load_array_metadata(
      const URI& array_uri,
      const EncryptionKey& encryption_key,
      uint64_t timestamp,
      Metadata* metadata);

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
   * Reads from the cache into the input buffer. `uri` and `offset` collectively
   * form the key of the cached object to be read. Essentially, this is used
   * to read potentially cached tiles. `uri` is the URI of the attribute the
   * tile belongs to, and `offset` is the offset in the attribute file where
   * the tile is located. Observe that the `uri`, `offset` pair is unique.
   *
   * @param uri The URI of the cached object.
   * @param offset The offset of the cached object.
   * @param buffer The buffer to write into. The function reallocates memory
   *     for the buffer, sets its size to *nbytes* and resets its offset.
   * @param nbytes Number of bytes to be read.
   * @param in_cache This is set to `true` if the object is in the cache,
   *     and `false` otherwise.
   * @return Status.
   */
  Status read_from_cache(
      const URI& uri,
      uint64_t offset,
      Buffer* buffer,
      uint64_t nbytes,
      bool* in_cache) const;

  /**
   * Reads from a file into the input buffer.
   *
   * @param uri The URI file to read from.
   * @param offset The offset in the file the read will start from.
   * @param buffer The buffer to write into. The function reallocates memory
   *     for the buffer, sets its size to *nbytes* and resets its offset.
   * @param nbytes The number of bytes to read.
   * @return Status.
   */
  Status read(
      const URI& uri, uint64_t offset, Buffer* buffer, uint64_t nbytes) const;

  /**
   * Reads from a file into the raw input buffer.
   *
   * @param uri The URI file to read from.
   * @param offset The offset in the file the read will start from.
   * @param buffer The buffer to write into.
   * @param nbytes The number of bytes to read.
   * @return Status.
   */
  Status read(
      const URI& uri, uint64_t offset, void* buffer, uint64_t nbytes) const;

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
      ArraySchema* array_schema, const EncryptionKey& encryption_key);

  /**
   * Stores the array metadata into persistent storage.
   *
   * @param array_uri The array URI.
   * @param encryption_key The encryption key to use.
   * @param array_metadata The array metadata.
   * @return Status
   */
  Status store_array_metadata(
      const URI& array_uri,
      const EncryptionKey& encryption_key,
      Metadata* array_metadata);

  /** Closes a file, flushing its contents to persistent storage. */
  Status close_file(const URI& uri);

  /** Syncs a file or directory, flushing its contents to persistent storage. */
  Status sync(const URI& uri);

  /** Returns the virtual filesystem object. */
  VFS* vfs() const;

  /**
   * Writes the contents of a buffer into the cache. `uri` and `offset`
   * collectively form the key of the object to be cached. Essentially, this is
   * used to cach tiles. `uri` is the URI of the attribute the
   * tile belongs to, and `offset` is the offset in the attribute file where
   * the tile is located. Observe that the `uri`, `offset` pair is unique.
   *
   * @param uri The URI of the cached object.
   * @param offset The offset of the cached object.
   * @param buffer The buffer whose contents will be cached.
   * @return Status.
   */
  Status write_to_cache(const URI& uri, uint64_t offset, Buffer* buffer) const;

  /**
   * Writes the contents of a buffer into a URI file.
   *
   * @param uri The file to write into.
   * @param buffer The buffer to write.
   * @return Status.
   */
  Status write(const URI& uri, Buffer* buffer) const;

  /**
   * Writes the input data into a URI file.
   *
   * @param uri The file to write into.
   * @param data The data to write.
   * @param size The data size in bytes.
   * @return Status.
   */
  Status write(const URI& uri, void* data, uint64_t size) const;

 private:
  /* ********************************* */
  /*        PRIVATE DATATYPES          */
  /* ********************************* */

  /**
   * Helper RAII struct that increments 'queries_in_progress' in the constructor
   * and decrements in the destructor, on the given StorageManager instance.
   *
   * This ensures that the counter is decremented even in the case of
   * exceptions.
   */
  struct QueryInProgress {
    /** The StorageManager instance. */
    StorageManager* sm;

    /** Constructor. Calls increment_in_progress() on given StorageManager. */
    QueryInProgress(StorageManager* sm)
        : sm(sm) {
      sm->increment_in_progress();
    }

    /** Destructor. Calls decrement_in_progress() on given StorageManager. */
    ~QueryInProgress() {
      sm->decrement_in_progress();
    }
  };

  /* ********************************* */
  /*        PRIVATE ATTRIBUTES         */
  /* ********************************* */

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

  /** Stores exclusive filelocks for arrays. */
  std::unordered_map<std::string, filelock_t> xfilelocks_;

  /** Mutex for managing OpenArray objects for reads. */
  std::mutex open_array_for_reads_mtx_;

  /** Mutex for managing OpenArray objects for writes. */
  std::mutex open_array_for_writes_mtx_;

  /** Mutex for managing exclusive locks. */
  std::mutex xlock_mtx_;

  /** Stores the currently open arrays for reads. */
  std::map<std::string, OpenArray*> open_arrays_for_reads_;

  /** Stores the currently open arrays for writes. */
  std::map<std::string, OpenArray*> open_arrays_for_writes_;

  /** Count of the number of queries currently in progress. */
  uint64_t queries_in_progress_;

  /** Guards queries_in_progress_ counter. */
  std::mutex queries_in_progress_mtx_;

  /** Guards queries_in_progress_ counter. */
  std::condition_variable queries_in_progress_cv_;

  /** The thread pool for compute-bound tasks. */
  ThreadPool* const compute_tp_;

  /** The thread pool for io-bound tasks. */
  ThreadPool* const io_tp_;

  /** Tracks all scheduled tasks that can be safely cancelled before execution.
   */
  CancelableTasks cancelable_tasks_;

  /** Tags for the context object. */
  std::unordered_map<std::string, std::string> tags_;

  /** A tile cache. */
  tdb_unique_ptr<BufferLRUCache> tile_cache_;

  /**
   * Virtual filesystem handler. It directs queries to the appropriate
   * filesystem backend. Note that this is stateful.
   */
  VFS* vfs_;

  /** The rest client (may be null if none was configured). */
  tdb_unique_ptr<RestClient> rest_client_;

  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * This is an auxiliary function to the other `array_open*` functions.
   * It opens the array, retrieves an `OpenArray` instance, acquires
   * its mutex and increases its counter. The array schema of the array
   * is loaded, but not any fragment metadata at this point.
   *
   * @param array_uri The array URI.
   * @param encryption_key The encryption key.
   * @param open_array The `OpenArray` instance retrieved after opening
   *      the array. Note that its mutex will be locked and its counter
   *      will have been incremented when the function returns.
   * @return Status
   */
  Status array_open_without_fragments(
      const URI& array_uri,
      const EncryptionKey& encryption_key,
      OpenArray** open_array);

  /** Decrement the count of in-progress queries. */
  void decrement_in_progress();

  /** Retrieves all the array metadata URI's of an array. */
  Status get_array_metadata_uris(
      const URI& array_uri, std::vector<URI>* array_metadata_uris) const;

  /** Increment the count of in-progress queries. */
  void increment_in_progress();

  /**
   * Loads the array schema into an open array.
   *
   * @param array_uri The array URI.
   * @param open_array The open array object.
   * @param encryption_key The encryption key to use.
   * @return Status
   */
  Status load_array_schema(
      const URI& array_uri,
      OpenArray* open_array,
      const EncryptionKey& encryption_key);

  /**
   * Loads the array metadata whose URIs are specified in the input.
   * The array metadata are cached in binary form in `open_array`.
   * Only the metadata that have not yet been loaded will be fetched
   * into the open array object. Eventually, the function will
   * deserialize the appropriate array metadata into `metadata`.
   *
   * @param open_array The open array object.
   * @param encryption_key The encryption key to use.
   * @param array_metadata_to_load The timestamped URIs of the array
   *     metadata to load.
   * @param metadata The array metadata to be retrieved.
   * @return Status
   */
  Status load_array_metadata(
      OpenArray* open_array,
      const EncryptionKey& encryption_key,
      const std::vector<TimestampedURI>& array_metadata_to_load,
      Metadata* metadata);

  /**
   * Loads the fragment metadata of an open array given a vector of
   * fragment URIs `fragments_to_load`. If the fragment metadata
   * are not already loaded into the array, the function loads them.
   * The function stores the fragment metadata of each fragment
   * in `fragments_to_load` into vector `fragment_metadata`, such
   * that there is a one-to-one correspondence between the two vectors.
   *
   * @param open_array The open array object.
   * @param encryption_key The encryption key to use.
   * @param fragments_to_load The fragments whose metadata to load.
   * @param meta_buff A buffer that contains the consolidated fragment
   *     metadata.
   * @param offsets A map from a fragment name to an offset in `meta_buff`
   *     where the basic metadata can be found. If the offset cannot be
   *     found, then the metadata of that fragment will be loaded from
   *     storage instead.
   * @param fragment_metadata The fragment metadata retrieved in a
   *     vector.
   * @return Status
   */
  Status load_fragment_metadata(
      OpenArray* open_array,
      const EncryptionKey& encryption_key,
      const std::vector<TimestampedURI>& fragments_to_load,
      Buffer* meta_buff,
      const std::unordered_map<std::string, uint64_t>& offsets,
      std::vector<FragmentMetadata*>* fragment_metadata);

  /**
   * Loads the latest consolidated fragment metadata from storage.
   *
   * @param uri The URI of the consolidated fragment metadata.
   * @param enc_key The encryption key that may be needed to access the file.
   * @param f_buff The buffer to hold the consolidated fragment metadata.
   * @param offsets A map from the fragment name to the offset in `f_buff` where
   *     the basic fragment metadata starts.
   * @return Status
   */
  Status load_consolidated_fragment_meta(
      const URI& uri,
      const EncryptionKey& enc_key,
      Buffer* f_buff,
      std::unordered_map<std::string, uint64_t>* offsets);

  /**
   * Retrieves the URI of the latest consolidated fragment metadata,
   * among the URIs in `uris`.
   */
  Status get_consolidated_fragment_meta_uri(
      const std::vector<URI>& uris, URI* meta_uri) const;

  /**
   * Applicable to fragment and array metadata URIs.
   *
   * Gets the sorted URIs in ascending first timestamp order,
   * breaking ties with lexicographic
   * sorting of UUID. Only the URIs with timestamp smaller than or
   * equal to `timestamp` are considered. The sorted URIs are
   * stored in the last input, including their timestamps.
   */
  Status get_sorted_uris(
      const std::vector<URI>& uris,
      uint64_t timestamp,
      std::vector<TimestampedURI>* sorted_uris) const;

  /**
   * It computes the URIs `to_vacuum` from the input `uris`, considering
   * only the URIs whose second timestamp is smaller than or equal to
   * `timestamp`. The function also retrieves the `vac_uris` (files with
   * `.vac` suffix) that were used to compute `to_vaccum`.
   */
  Status get_uris_to_vacuum(
      const std::vector<URI>& uris,
      uint64_t timestamp,
      std::vector<URI>* to_vacuum,
      std::vector<URI>* vac_uris) const;

  /** Block until there are zero in-progress queries. */
  void wait_for_zero_in_progress();

  /** Initializes a REST client, if one was configured. */
  Status init_rest_client();

  /** Sets default tag values on this StorageManager. */
  Status set_default_tags();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_STORAGE_MANAGER_H
