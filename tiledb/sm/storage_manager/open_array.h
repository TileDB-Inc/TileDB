/**
 * @file   open_array.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include <mutex>
#include <unordered_map>
#include <vector>

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/uri.h"

namespace tiledb {
namespace sm {

/**
 * Stores information about an open array.
 *
 * Each time an open array object is opened, the potentially new fragment
 * metadata (that did not exist before) are loaded and stored in the
 * object. These metadata are stored separately in a different "snapshot".
 * This class then enables retrieving all the metadata loaded before or
 * at a given snapshot. This attributes flexibility to handling consistency
 * of queries associated with open array objects at different times during
 * the lifetime of these objects.
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

  /** Returns the array URI. */
  const URI& array_uri() const;

  /** Returns the counter. */
  uint64_t cnt() const;

  /** Decrements the counter. */
  void cnt_decr();

  /** Increments the counter. */
  void cnt_incr();

  /**
   * Returns true if the array is empty at the given snapshot. The array is
   * empty if there are no fragments at or before the given snapshot.
   */
  bool is_empty(uint64_t snapshot) const;

  /** Retrieves a (shared) filelock for the array. */
  Status file_lock(VFS* vfs);

  /** Retrieves a (shared) filelock for the array. */
  Status file_unlock(VFS* vfs);

  /**
   * Returns the fragment metadata. In case the array was opened for writes,
   * an empty vector is returned. In case of reads, all the fragment metadata
   * loaded at or before `snapshot` will be returned.
   *
   * @param snapshot The snapshot at or before the result fragment metadata
   *     got loaded into the open array.
   * @return
   */
  std::vector<FragmentMetadata*> fragment_metadata(uint64_t snapshot) const;

  /**
   * Returns the fragment metadata object given a URI, or `nullptr` if
   * the fragment metadata have not been loaded for that URI yet.
   *
   * @param fragment_uri The fragment URI.
   * @return Status
   */
  FragmentMetadata* fragment_metadata_get(const URI& fragment_uri) const;

  /** Locks the array mutex. */
  void mtx_lock();

  /** Unlocks the array mutex. */
  void mtx_unlock();

  /**
   * Returns the next snapshot identifier. This is essentially equal to
   * the size of `fragment_metadata_`, since it will be used to identify
   * at which point new fragment metadata got loaded into the open array
   * object.
   */
  uint64_t next_snapshot() const;

  /** The query type the array was opened with. */
  QueryType query_type() const;

  /** Sets an array schema. */
  void set_array_schema(ArraySchema* array_schema);

  /**
   * Pushes the fragment metadata at the end of the vector of
   * fragment metadata snapshots.
   */
  void push_back_fragment_metadata(
      const std::vector<FragmentMetadata*>& metadata);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array schema. */
  ArraySchema* array_schema_;

  /** The array URI. */
  URI array_uri_;

  /** Counts how many times the array has been opened. */
  uint64_t cnt_;

  /** Filelock handle. */
  filelock_t filelock_;

  /**
   * The fragment metadata of the open array. Each outer vector element is
   * created when `push_back_fragment_metadata` is called, which effectively
   * means that a new set of fragment metadata is loaded into the already
   * open array.
   *
   * In other words, there is one element in this vector per opening of the
   * array. E.g. suppose the array is empty when it was opened, this will
   * contain one element corresponding to the opening, but that element will be
   * an empty vector (since there were no fragments). Suppose then there were
   * two writes made and the array was then reopened. This vector will then have
   * two elements: the first empty vector from the first opening, and then a
   * vector from the second opening. The second opening's vector will have two
   * elements (one per fragment).
   */
  std::vector<std::vector<FragmentMetadata*>> fragment_metadata_;

  /** A map from fragment URI to metadata object. */
  std::unordered_map<std::string, FragmentMetadata*> fragment_metadata_map_;

  /**
   * A mutex used to lock the array when loading the array metadata and
   * any fragment metadata structures from the disk.
   */
  mutable std::mutex mtx_;

  /** The query type the array was opened with. */
  QueryType query_type_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_H
