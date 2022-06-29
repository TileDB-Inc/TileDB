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

namespace {
/**
 *
 *
 * @tparam chunk_size Number of Ts to allocate per chunk
 */
template <size_t chunk_size>
class PoolAllocatorImpl {
 public:
  using value_type = std::byte;
  using pointer = value_type*;

 private:
  bool debug_{false};
  std::mutex mutex_;

  /**
   * Book keeping pointers
   */
  pointer the_free_list = nullptr;
  pointer the_array_list = nullptr;
  size_t num_arrays = 0;
  size_t num_free = 0;

  constexpr static ptrdiff_t page_size{4096};
  constexpr static ptrdiff_t align{page_size};
  constexpr static ptrdiff_t page_mask{~(page_size - 1)};

  /* 32M / chunk_size */
  constexpr static size_t mem_size{32 * 1024 * 1024};
  constexpr static size_t chunks_per_array{mem_size / chunk_size};

  static_assert(mem_size % chunk_size == 0);
  static_assert(chunk_size <= mem_size);

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
  void free_list_more() {
    auto new_bytes{reinterpret_cast<std::byte*>(malloc(array_size))};
    pointer new_array{reinterpret_cast<pointer>(new_bytes)};

    /* "Next" is stored at the beginning of the array */
    *(pointer*)new_array = the_array_list;
    the_array_list = new_array;

    /*
     * Force page alignment -- skip past the pointer, add (alignment-1),
     * and then mask off the lower bits
     */
    auto aligned_start{reinterpret_cast<std::byte*>(
        reinterpret_cast<ptrdiff_t>(new_bytes + sizeof(pointer) + (align - 1)) &
        reinterpret_cast<ptrdiff_t>(page_mask))};

    for (size_t i = 0; i < chunks_per_array; ++i) {
      push_chunk(aligned_start + i * chunk_size);
    }

    num_arrays++;
  }

  /**
   * Go through list of arrays, freeing each one
   */
  void free_list_free() {
    pointer first_array = the_array_list;

    auto num_to_free{num_arrays};
    for (size_t j = 0; j < num_to_free; ++j) {
      auto next_array = *(pointer*)first_array;
      free(first_array);
      first_array = next_array;
      --num_arrays;
    }
    the_array_list = first_array;
    the_free_list = first_array;
  }

 public:
  /**
   * Use default constructor
   */
  PoolAllocatorImpl() = default;

  /**
   * Release allocated memory (free each array)
   */
  ~PoolAllocatorImpl() {
    free_list_free();
    assert(num_arrays == 0);
    assert(the_free_list == nullptr);
    assert(the_array_list == nullptr);
  }

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
    pointer first_array = the_array_list;

    for (size_t j = 0; j < num_arrays; ++j) {
      auto start = (pointer)((char*)first_array + sizeof(pointer));
      for (size_t i = 0; i < chunks_per_array; ++i) {
        f(start + i * chunk_size);
        ; /* Do something -- your code here */

        if (debug_) {
          std::cout << "scanning " << j << " " << i << std::endl;
        }
      }
      first_array = *(pointer*)first_array;
    }
  }
};

template <size_t chunk_size>
class SingletonPoolAllocator : public PoolAllocatorImpl<chunk_size> {
  PoolAllocatorImpl<chunk_size> pool_allocator_;

  // Private constructor so that no objects can be created.
  SingletonPoolAllocator() {
  }

  static SingletonPoolAllocator instance;

 public:
  static SingletonPoolAllocator* get_instance() {
    return &instance;
  }
};

template <size_t chunk_size>
SingletonPoolAllocator<chunk_size> SingletonPoolAllocator<chunk_size>::instance;

template <size_t chunk_size>
SingletonPoolAllocator<chunk_size>* my_allocator =
    SingletonPoolAllocator<chunk_size>::get_instance();
}  // namespace

template <size_t chunk_size>
class PoolAllocator {
 public:
  using value_type = typename SingletonPoolAllocator<chunk_size>::value_type;
  PoolAllocator() {
  }
  value_type* allocate() {
    return my_allocator<chunk_size>->allocate();
  }
  value_type* allocate(size_t) {
    return my_allocator<chunk_size>->allocate();
  }
  void deallocate(value_type* a) {
    return my_allocator<chunk_size>->deallocate(a);
  }
  void deallocate(value_type* a, size_t) {
    return my_allocator<chunk_size>->deallocate(a);
  }
};

// template <size_t chunk_size>
// using PoolAllocator = SingletonPoolAllocator<chunk_size>::get_instance();

// template <size_t chunk_size>
// using PoolAllocator = SingletonPoolAllocator<chunk_size>;
// struct PoolAllocator : SingletonPoolAllocator<chunk_size> {
//  PoolAllocator() {
//  }
//};
}  // namespace tiledb::common

#endif  // TILEDB_DAG_POOL_ALLOCATOR_H
