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

/** Stores information about an open array. */
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

  /** Retrieves a (shared) filelock for the array. */
  Status file_lock(VFS* vfs);

  /** Retrieves a (shared) filelock for the array. */
  Status file_unlock(VFS* vfs);

  /** Returns the fragment metadata. */
  const std::vector<FragmentMetadata*>& fragment_metadata() const;

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

  /** The query type the array was opened with. */
  QueryType query_type() const;

  /** Sets an array schema. */
  void set_array_schema(ArraySchema* array_schema);

  /** Sets the fragment metadata. */
  void set_fragment_metadata(const std::vector<FragmentMetadata*>& metadata);

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

  /** The fragment metadata of the open array. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /** A map from fragment URI to metadata object. */
  std::unordered_map<std::string, FragmentMetadata*> fragment_metadata_map_;

  /**
   * A mutex used to lock the array when loading the array metadata and
   * any fragment metadata structures from the disk.
   */
  std::mutex mtx_;

  /** The query type the array was opened with. */
  QueryType query_type_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OPEN_ARRAY_H
