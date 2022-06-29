/**
 * @file   data_block_allocator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares the DataBlock allocator class for dag.
 */

#ifndef TILEDB_DAG_DATA_BLOCK_ALLOCATOR_H
#define TILEDB_DAG_DATA_BLOCK_ALLOCATOR_H

#include "datablock.h"

namespace tiledb::common {
#if 0
/**
   According to cpprefence.com,
    need

            value_type size_type == std::size_t difference_type ==
        std::ptrdiff_t propagate_on_container_move_assignment ==
        std::true_type is_always_equal ==
        std::true_type constructor destructor

            allocate deallocate

            operator==
            operator!=
*/

template <class T>
pool_allocator {
  using value_type = T;
  using pointer = value_type*;

  template <class U>
  allocator(allocator<U> const&) noexcept {
  }

  pointer allocate(size_t n) {
    return static_cast<pointer>(::operator new(n * sizeof(value_type)));
  }

  void deallocate(pointer p, size_t) noexcept {
    ::operator delete(p);
  }
}

template <class T, class U>
bool operator==(const pool_allocator<T>&, const pool_allocator<U>&) {
  return true;
}
template <class T, class U>
bool operator!=(const pool_allocator<T>&, const pool_allocator<U>&) {
  return false;
}
#endif

//
}

#endif  // TILEDB_DAG_DATA_BLOCK_ALLOCATOR_H
