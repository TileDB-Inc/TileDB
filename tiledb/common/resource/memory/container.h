/**
 * @file   tiledb/common/resource/memory/memory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

#ifndef TILEDB_COMMON_RESOURCE_CONTAINER_H
#define TILEDB_COMMON_RESOURCE_CONTAINER_H

#include "./memory.h"
#include <vector>

namespace tiledb::common {

/**
 * Resource-managed vector
 *
 * This class is
 *
 * @tparam T type of the vector contents
 */
template <class T>
class vector : public std::vector<T, pmr_allocator<T>> {
  using base = std::vector<T, pmr_allocator<T>>;
 public:
  /**
   * Default constructor is deleted.
   *
   * It would violate policy for this class to have a default constructor. All
   * `tdb` containers require an allocator for construction; they do not
   * allocate on the global heap.
   */
  vector() = delete;

  /**
   * Allocator-only constructor is the closest thing to a default constructor.
   */
  explicit vector(pmr_allocator<T>& a)
      : base(a) {
  }
};

}  // namespace tiledb::common

#endif