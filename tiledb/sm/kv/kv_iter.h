/**
 * @file   kv_iter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 This file defines class KVIter.
 */

#ifndef TILEDB_KV_ITER_H
#define TILEDB_KV_ITER_H

#include "tiledb/sm/kv/kv.h"
#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

namespace tiledb {
namespace sm {

/**
 * This is a key-value store iterator. It is used for reading
 * the stored key-value items one-by-one.
 */
class KVIter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  explicit KVIter(StorageManager* storage_manager);

  /** Destructor. */
  ~KVIter();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns `true` if the iterator is done. */
  bool done() const;

  /** Retrieves the current key-value item (creating a new item). */
  Status here(KVItem** kv_item) const;

  /**
   * Initializes the key-value store iterator. The pointer is placed to the
   * first item in the store.
   *
   * @param kv The kv the iterator is associated with.
   * @return Status
   */
  Status init(KV* kv);

  /** Moves to the next key-value item. */
  Status next();

  /** Resets the iterator, so that it can start iterating from the beginning. */
  Status reset();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** A key-value store that will help reading items one-by-one using a hash. */
  KV* kv_;

  /** TileDB storage manager. */
  StorageManager* storage_manager_;

  /** Buffer for storing coordinates (i.e., key hashes). */
  uint64_t* coords_buffer_;

  /** The allocared size for the coordinates buffer. */
  uint64_t coords_buffer_alloced_size_;

  /** The current item from the read ones. */
  uint64_t current_item_;

  /** The read query. */
  Query* query_;

  /** The read query status. */
  QueryStatus status_;

  /**
   * Maximum number of items to be read at a time. This is
   * set equal to tile capacity of the underlying KV in `init()`.
   */
  uint64_t max_item_num_;

  /** Number of items read. */
  uint64_t item_num_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Clears the iterator. */
  void clear();

  /** Submits a read query, recording its status. */
  Status submit_read_query();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_KV_H
