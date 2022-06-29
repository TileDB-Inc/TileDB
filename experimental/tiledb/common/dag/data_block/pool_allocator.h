/**
 * @file   pool_allocator.h
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
 * This file declares a pool memory allocator.
 */

#ifndef TILEDB_DAG_POOL_ALLOCATOR_H
#define TILEDB_DAG_POOL_ALLOCATOR_H

#include <iostream>
#include <mutex>

namespace tiledb::common {

template <class Chunk>
class pool_allocator {
  bool debug_{false};

  using value_type = Chunk;
  using pointer = value_type*;

  std::mutex mutex_;

  /**
   * Book keeping posize_ters
   */
  pointer the_free_list = nullptr;
  pointer the_array_list = nullptr;
  size_t num_arrays = 0;
  size_t num_free = 0;

  constexpr static ptrdiff_t page_size{4096};
  constexpr static size_t align{page_size};
  constexpr static ptrdiff_t page_mask{~(page_size - 1)};

  /* 32M / sizeof(chunk) */
  constexpr static size_t mem_size{32 * 1024 * 1024};
  constexpr static size_t chunks_per_array{mem_size / sizeof(value_type)};

  /* Add some padding so that we can align on page boundary */
  constexpr static size_t array_size{mem_size + align + sizeof(pointer)};

  /**
   * Get a chunk from the free list
   */
  pointer pop_chunk() {
    if ((num_free == 0) || (the_free_list == NULL))
      free_list_more();

    pointer the_new_chunk = the_free_list;

    /* "Next" is stored at the beginning of the chunk */
    the_free_list = *(pointer*)the_free_list;

    return the_new_chunk;
  }

  /**
   * Put a chunk back into the free list
   */
  void push_chunk(pointer finished_chunk) {
    /* "Next" is stored at the beginning of the chunk */
    *(pointer*)finished_chunk = the_free_list;
    the_free_list = finished_chunk;
  }

  /**
   * Allocates a new array of chunks and puts them on the free list
   */
  size_t free_list_more() {
    auto new_bytes = reinterpret_cast<std::byte*>(malloc(array_size));
    pointer new_array = reinterpret_cast<pointer>(new_bytes);

    /* "Next" is stored at the beginning of the array */
    *(pointer*)new_array = the_array_list;
    the_array_list = new_array;

    /* Force page alignment -- skip past the pointer, add page_mask, and then
     * mask off the lower bits */
    pointer aligned_start = reinterpret_cast<pointer>(
        reinterpret_cast<ptrdiff_t>(new_bytes + sizeof(pointer) + (align - 1)) &
        reinterpret_cast<ptrdiff_t>(page_mask));

    for (size_t i = 0; i < chunks_per_array; ++i) {
      push_chunk(aligned_start + i);
    }

    num_arrays++;

    return 0;
  }

 public:
  pointer allocate() {
    std::scoped_lock lock(mutex_);

    num_free--;
    return pop_chunk();
  }

  void deallocate(pointer p) {
    push_chunk(p);
    num_free++;
  }

  void mark(pointer) {
    std::cout << "mark" << std::endl;
  }

  void sweep(pointer) {
    std::cout << "sweep" << std::endl;
  }

  /* Iterate through every element that has been allocated */
  void scan_all(void (*f)(pointer)) {
    pointer start_value_type = (pointer)NULL;
    pointer first_array = the_array_list;

    for (size_t j = 0; j < num_arrays; ++j) {
      start_value_type = (pointer)((char*)first_array + sizeof(pointer));
      for (size_t i = 0; i < chunks_per_array; ++i) {
        f(start_value_type + i);
        ; /* Do something -- your code here */

        if (debug_) {
          std::cout << "scanning " << j << " " << i << std::endl;
        }
      }
      first_array = *(pointer*)first_array;
    }
  }
};
}  // namespace tiledb::common

#endif  // TILEDB_DAG_POOL_ALLOCATOR_H
