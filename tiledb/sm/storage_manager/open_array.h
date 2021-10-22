/**
 * @file   open_array.h
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
 * This file defines class OpenArray.
 */

#ifndef TILEDB_OPEN_ARRAY_H
#define TILEDB_OPEN_ARRAY_H

#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

#include "tiledb/sm/crypto/encryption_key_validation.h"
#include "tiledb/sm/filesystem/filelock.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/storage_manager/open_array_memory_tracker.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ArraySchema;
class Buffer;
class VFS;

enum class QueryType : uint8_t;

/**
 * Stores information about an open array.
 *
 * Each time an open array object is opened, the potentially new fragment
 * metadata (that did not exist before) are loaded and stored in the
 * object. Each fragment metadata is timestamped.
 * This class then enables retrieving all the metadata loaded before or
 * at a given timestamp. This attributes flexibility to handling consistency
 * of queries associated with open array objects at different times during
 * the lifetime of these objects. In TileDB, asll timestamps are in ms
 * elapsed since 1970-01-01 00:00:00 +0000 (UTC).
 */
class OpenArray {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  explicit OpenArray(const URI& array_uri, QueryType query_type);

  /** Destructor. */
  ~OpenArray();

  /* ********************************* */
  /*               API                 */
  /* ********************************* */

  /** Returns the array schema. */
  ArraySchema* array_schema() const;

  /** Returns array schemas map. */
  std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>>& array_schemas();

  /** Gets the array schema given a array schema name. */
  Status get_array_schema(
      const std::string& schema_name,
      tdb_shared_ptr<ArraySchema>* array_schema);

  /** Returns the array URI. */
  const URI& array_uri() const;

  /**
   * If it is the first time this function is called, the input key
   * is set to the open array without explicitly storing the key
   * for future validity checks. Otherwise, the input key is securely
   * checked if it matches the already set one.
   */
  Status set_encryption_key(const EncryptionKey& encryption_key);

  /** Returns the counter. */
  uint64_t cnt() const;

  /** Decrements the counter. */
  void cnt_decr();

  /** Increments the counter. */
  void cnt_incr();

  /**
   * Returns true if the array is empty at the given timestamp. The array is
   * empty if there are no fragments at or before the given timestamp.
   */
  bool is_empty(uint64_t timestamp) const;

  /** Retrieves a (shared) filelock for the array. */
  Status file_lock(VFS* vfs);

  /** Retrieves a (shared) filelock for the array. */
  Status file_unlock(VFS* vfs);

  /**
   * Returns the `FragmentMetadata` object of the input fragment URI,
   * or `nullptr` if the fragment metadata do no exist.
   */
  tdb_shared_ptr<FragmentMetadata> fragment_metadata(const URI& uri) const;

  /** Returns the memory tracker to be used by all fragment metadatas. */
  OpenArrayMemoryTracker* memory_tracker();

  /**
   * Returns the constant buffer storing the serialized array metadata
   * of the input URI, or `nullptr` if the array metadata do not exist.
   */
  tdb_shared_ptr<Buffer> array_metadata(const URI& uri) const;

  /** Locks the array mutex. */
  void mtx_lock();

  /** Unlocks the array mutex. */
  void mtx_unlock();

  /** The query type the array was opened with. */
  QueryType query_type() const;

  /**
   * Inserts the input fragment metadata. Note that all fragment
   * metadata must be sorted in ascending timestamp of creation.
   * This function will guarantee that the ordering is maintained.
   */
  void insert_fragment_metadata(tdb_shared_ptr<FragmentMetadata> metadata);

  /**
   * Inserts the input array metadata (serialized in a share constant
   * buffer) with the input URI.
   */
  void insert_array_metadata(
      const URI& uri, const tdb_shared_ptr<Buffer>& metadata);

  /** Sets an array schema. */
  void set_array_schema(ArraySchema* array_schema);

  /** Custom comparator for comparing fragment metadata pointers. */
  struct cmp_frag_meta_ptr {
    /**
     * Returns `true` if the timestamp of the first operand is smaller,
     * breaking ties based on the URI string.
     */
    bool operator()(
        const tdb_shared_ptr<FragmentMetadata>& meta_a,
        const tdb_shared_ptr<FragmentMetadata>& meta_b) const {
      return *meta_a < *meta_b;
    }
  };

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The latest array schema. */
  ArraySchema* array_schema_;

  /**
   * A map of all array_schemas
   */
  std::unordered_map<std::string, tdb_shared_ptr<ArraySchema>> array_schemas_;

  /** The array URI. */
  URI array_uri_;

  /** Counts how many times the array has been opened. */
  uint64_t cnt_;

  /** Used to validate keys when opening an already opened array. */
  EncryptionKeyValidation key_validation_;

  /** Filelock handle. */
  filelock_t filelock_;

  /** The fragment metadata of the open array. */
  std::set<tdb_shared_ptr<FragmentMetadata>, cmp_frag_meta_ptr>
      fragment_metadata_;

  /**
   * A map of URI strings to the fragment metadata that already have been
   * loaded in `fragment_metadata_`.
   */
  std::unordered_map<std::string, tdb_shared_ptr<FragmentMetadata>>
      fragment_metadata_set_;

  /** The memory tracker for the fragment metadata. */
  OpenArrayMemoryTracker memory_tracker_;

  /**
   * A map of URI strings to array metadata. The map stores the serialized
   * (decompressed, decrypted) array metadata into constant buffers.
   */
  std::unordered_map<std::string, tdb_shared_ptr<Buffer>> array_metadata_;

  /**
   * A mutex used to lock the array for thread-safe open/close of the array
   * by the StorageManager.
   */
  mutable std::mutex mtx_;

  /**
   * A mutex used to protect private instance variables (e.g. fragment
   * metadata).
   */
  mutable std::mutex local_mtx_;

  /** The query type the array was opened with. */
  QueryType query_type_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_H
