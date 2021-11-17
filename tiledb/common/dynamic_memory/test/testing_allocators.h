/**
 * @file tiledb/common/dynamic_memory/test/testing_allocators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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

#ifndef TILEDB_COMMON_DYNAMIC_MEMORY_TEST_TESTING_ALLOCATORS_H
#define TILEDB_COMMON_DYNAMIC_MEMORY_TEST_TESTING_ALLOCATORS_H

#include <new>

/**
 * An allocator that always throws bad_alloc for all allocations.
 *
 * The nickname for this allocator is Rage Against The Machine: "I won't do what
 * you tell me."
 *
 * @tparam T The allocator's value_type
 */
template <class T>
class ThrowingAllocator {
 public:
  using value_type = T;

  ThrowingAllocator() = default;
  template <class U>
  ThrowingAllocator(ThrowingAllocator<U> const&) noexcept {
  }

  T* allocate(std::size_t) {
    throw std::bad_alloc();
  }

  void deallocate(T*, std::size_t) noexcept {
  }
};

#endif  // TILEDB_COMMON_DYNAMIC_MEMORY_TEST_TESTING_ALLOCATORS_H
