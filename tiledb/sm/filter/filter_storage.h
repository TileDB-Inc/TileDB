/**
 * @file   filter_storage.h
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
 * This file declares class FilterStorage.
 */

#ifndef TILEDB_FILTER_STORAGE_H
#define TILEDB_FILTER_STORAGE_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/misc/status.h"

#include <list>
#include <memory>
#include <unordered_map>

namespace tiledb {
namespace sm {

/**
 * Manages a ref-counted pool of buffers, used for filter I/O.
 */
class FilterStorage {
 public:
  /**
   * Return a buffer from the pool, allocating a new one if necessary. The
   * buffer returned by this function will not be available for reuse until it
   * is reclaimed by this instance via the reclaim() method.
   *
   * @return Buffer from the pool
   */
  std::shared_ptr<Buffer> get_buffer();

  /** Return the number of buffers in the internal available list. */
  uint64_t num_available() const;

  /** Return the number of buffers in the internal in-use list. */
  uint64_t num_in_use() const;

  /**
   * Reclaims the given buffer, marking it as available to return with a
   * subsequent call to get_buffer().
   *
   * If the given buffer is not managed by this instance, reclaiming it is a
   * no-op.
   *
   * @param buffer Pointer to underlying buffer to reclaim.
   * @return Status
   */
  Status reclaim(Buffer* buffer);

 private:
  /** List of buffers that are available to be used (may be empty). */
  std::list<std::shared_ptr<Buffer>> available_;

  /** List of buffers that are currently in use (may be empty). */
  std::list<std::shared_ptr<Buffer>> in_use_;

  /**
   * Mapping of underlying Buffer pointer to linked list node in the in_use_
   * list.
   */
  std::unordered_map<Buffer*, std::list<std::shared_ptr<Buffer>>::iterator>
      in_use_list_map_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_STORAGE_H
