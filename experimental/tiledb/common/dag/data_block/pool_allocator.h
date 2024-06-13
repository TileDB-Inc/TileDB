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
 * This file declares a simple pool memory allocator, intended for use with
 * DataBlocks.  The allocator initially uses malloc to get a 32MB array (plus
 * some space for a superblock, plus some space to allow page alignment).
 * The page-aligned portion of each array is subdivided into specified
 * fixed-size chunks. The chunks are expected to be a power of two size.
 * The arrays are kept in a linked list, with 8 bytes of the superblock
 * used as a pointer to the next array.  Blocks are kept in a linked list
 * in a similar fashion.
 *
 * The pool allocator is implemented with the PoolAllocatorImpl, which is
 * private to this file.  A singleton object, SingletonPoolAllocator, is
 * used to ensure that there is only one PoolAllocatorImpl in the application.
 * Access to the pool allocator is via PoolAllocator objects.  There can
 * be multiple PoolAllocator objects in an application -- any such objects
 * will use the SingletonPoolAllocator.
 *
 * The `PoolAllocator` implements an interface conforming to C++ standard
 * library named requirements for `Allocator`.

 * According to Howard Hinnant
 * (https://howardhinnant.github.io/allocator_boilerplate.html), an allocator
 * needs the following minimal set of type  aliases and associated functions
 * (the rest will be provided as defaults by `allocator_traits`), as shown
 * by a simple example class:
 *
 * template <class T>
 * class allocator {
 *  public:
 *   using value_type = T;
 *
 *   allocator() noexcept {
 *   }  // not required, unless used
 *   template <class U>
 *   allocator(allocator<U> const&) noexcept {
 *   }
 *
 *   value_type* allocate(std::size_t n) {
 *     return static_cast<value_type*>(::operator new(n * sizeof(value_type)));
 *   }

 *   void deallocate(
 *       value_type* p,
 *       std::size_t) noexcept  // Use pointer if pointer is not a value_type*
 *   {
 *     ::operator delete(p);
 *   }
 * };
 *
 * template <class T, class U>
 * bool operator==(allocator<T> const&, allocator<U> const&) noexcept {
 *   return true;
 * }
 *
 * template <class T, class U>
 * bool operator!=(allocator<T> const& x, allocator<U> const& y) noexcept {
 *   return !(x == y);
 * }
 */

#ifndef TILEDB_DAG_POOL_ALLOCATOR_H
#define TILEDB_DAG_POOL_ALLOCATOR_H

#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>

namespace tiledb::common {

namespace {

/**
 * The PoolAllocator implementation class.  Allocates a fixed-size block
 * of bytes.
 *
 * @tparam chunk_size Number of Ts to allocate per chunk* /
 */

template <size_t chunk_size>
class PoolAllocatorImpl {
 public:
  using value_type = std::byte;
  using pointer = value_type*;

 private:
  bool debug_{false};
  mutable std::mutex mutex_;

  /*
   * Pointers and counters for managing the pool
   */
  pointer the_free_list{nullptr};
  pointer the_array_list{nullptr};
  size_t num_arrays_{0};
  size_t num_free_{0};

  /*
   * Data chunks will be aligned to page boundaries.
   * Assumed to be 4k.
   *
   * @todo Determine page size at compile-time for the target architecture.
   */
  constexpr static ptrdiff_t page_size{4096};
  constexpr static ptrdiff_t align{page_size};
  constexpr static ptrdiff_t page_mask{~(page_size - 1)};

  /* Size of each array in the memory pool is 32 MB */
  constexpr static size_t mem_size{32 * 1024 * 1024};

  /* The number of chunks contained in each array */
  constexpr static size_t chunks_per_array{mem_size / chunk_size};

  /* Check that chunks evenly divide the array */
  static_assert(mem_size % chunk_size == 0);
  static_assert(chunk_size <= mem_size);

  /* Add some padding to the space we will allocate for the array so that we can
   * align chunks taken from it on page boundary */
  constexpr static size_t array_size{mem_size + align + sizeof(pointer)};

  /*
   * Counters for statistics / diagnostics.  Declare these `inline` so we don't
   * need to define themm outside of the class itself.
   */
  static inline std::atomic<size_t> num_instances_{0};
  static inline std::atomic<size_t> num_allocations_{0};
  static inline std::atomic<size_t> num_deallocations_{0};
  static inline std::atomic<size_t> num_allocated_{0};
  ;

  /**
   * Get a chunk from the free list.  The first `sizeof(pointer)` bytes in the
   * chunk are used as a pointer to create a linked list of chunks.  Those bytes
   * will be overwritten by the user of the chunk, which is fine -- a pointer
   * will be written into those bytes when the chunk is returned to the pool.
   *
   * @pre a lock is held by function calling this one.
   */
  pointer pop_chunk() {
    if ((num_free_ == 0) || (the_free_list == nullptr))
      free_list_more();

    pointer the_new_chunk = the_free_list;

    /* "Next" is stored at the beginning of the chunk */
    the_free_list = *(pointer*)the_free_list;

    return the_new_chunk;
  }

  /**
   * Return a chunk back into the pool's free list.
   *
   * @pre a lock is held by function calling this one.
   */
  void push_chunk(pointer finished_chunk) {
    /* "Next" is stored at the beginning of the chunk */
    *reinterpret_cast<pointer*>(finished_chunk) = the_free_list;
    the_free_list = finished_chunk;
  }

  /**
   * Allocate a new array of chunks and puts the chunks into the free list. Like
   * we do with chunks, the first `sizeof(pointer)` bytes in each array are used
   * as a pointer to create a linked list of arrays.
   *
   * @pre a lock is held by function calling this one.
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
        static_cast<ptrdiff_t>(page_mask))};

    for (size_t i = 0; i < chunks_per_array; ++i) {
      push_chunk(aligned_start + i * chunk_size);
    }

    ++num_arrays_;
  }

  /**
   * Go through the list of arrays, freeing each array
   *
   * @pre a lock is held by function calling this one.
   */
  void free_list_free() {
    pointer first_array = the_array_list;

    auto num_to_free{num_arrays_};
    for (size_t j = 0; j < num_to_free; ++j) {
      auto next_array = *(pointer*)first_array;
      free(first_array);
      first_array = next_array;
      --num_arrays_;
    }
    num_free_ = 0;
    num_allocated_ = 0;

    the_array_list = first_array;
    the_free_list = first_array;
  }

 public:
  /**
   * Any members in the class are default constructed.  We have an
   * implementation of the default constructor here so that we can increment the
   * class `num_instance` counter (which we use for debugging and testing that
   * we, in fact, have a singleton allocator.
   */
  PoolAllocatorImpl() {
    ++num_instances_;
  }

  /**
   * Destructor. Releases allocated memory (frees each array).
   */
  ~PoolAllocatorImpl() {
    free_list_free();
    assert(num_arrays_ == 0);
    assert(the_free_list == nullptr);
    assert(the_array_list == nullptr);
  }

  /**
   * Function to actually allocate a chunk.  Returns a chunk from the pool.  The
   * function `pop_chunk` may allocate more chunks (by allocating another array)
   * if there are no free chunks available.
   */
  pointer allocate() {
    std::lock_guard lock(mutex_);
    --num_free_;
    ++num_allocated_;
    ++num_allocations_;
    return pop_chunk();
  }

  /**
   * Return a chunk to the pool.
   */
  void deallocate(pointer p) {
    std::lock_guard lock(mutex_);
    push_chunk(p);
    ++num_free_;
    --num_allocated_;
    ++num_deallocations_;
  }

  /**
   * Get the number of instances of the allocator. Should always be equal to
   * one. Note that allocators for different chunk sizes are different
   * allocators. Singletons are on a per chunk size basis.
   */
  size_t num_instances() {
    return num_instances_;
  }

  /**
   * Get the total number of chunks that have been allocated during the lifetime
   * of this allocator.
   */
  size_t num_allocations() {
    return num_allocations_;
  }

  /**
   * Get the total number of chunks that have been deallocated during the
   * lifetime of this allocator.
   *
   * @invariant `num_allocations` + `num_deallocations` == `num_free_`
   */
  size_t num_deallocations() {
    return num_deallocations_;
  }

  /**
   * Get the number of chunks that are currently in use.
   */
  size_t num_allocated() {
    return num_allocated_;
  }

  /**
   * Get the number of chunks that are currently free (available for allocation
   * in the pool).
   *
   * @invariant `num_free_` + `num_allocated` == `num_arrays_` *
   * `chunks_per_array`
   */
  size_t num_free() {
    return num_free_;
  }

  /**
   * Get the number of chunk arrays that have been malloc'd to create the pool.
   */
  size_t num_arrays() {
    return num_arrays_;
  }

  /**
   * Some legacy instrumentation code.
   */
  void mark(pointer) {
    std::cout << "mark" << std::endl;
  }

  void sweep(pointer) {
    std::cout << "sweep" << std::endl;
  }

  /* Iterate through every element that has been allocated */
  void scan_all(void (*f)(pointer)) {
    pointer first_array = the_array_list;

    for (size_t j = 0; j < num_arrays_; ++j) {
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
};  // namespace

/**
 * Define a class that will only allow the creation of a single instance.
 */
template <size_t chunk_size>
class SingletonPoolAllocator : public PoolAllocatorImpl<chunk_size> {
  // Private constructor so that no objects can be created.
  SingletonPoolAllocator() = default;

 public:
  SingletonPoolAllocator(const SingletonPoolAllocator&) = delete;
  void operator=(const SingletonPoolAllocator&) = delete;

  static SingletonPoolAllocator& get_instance() {
    static SingletonPoolAllocator instance;
    return instance;
  }
};

/**
 * Access the single `PoolAllocator` instance.
 */
template <size_t chunk_size>
SingletonPoolAllocator<chunk_size>& _allocator{
    SingletonPoolAllocator<chunk_size>::get_instance()};

}  // namespace

/**
 * The external interface for pool allocation (done via the singleton pool
 * allocator object).  See the documentation for `PoolAllocatorImpl` for
 * each of these functions.
 */
template <size_t chunk_size>
class PoolAllocator {
 public:
  using value_type = typename SingletonPoolAllocator<chunk_size>::value_type;
  using pointer_type = value_type*;
  PoolAllocator() = default;
  pointer_type allocate() {
    return _allocator<chunk_size>.allocate();
  }
  void deallocate(pointer_type a) {
    return _allocator<chunk_size>.deallocate(a);
  }
  size_t num_instances() {
    return _allocator<chunk_size>.num_instances();
  }
  size_t num_allocations() {
    return _allocator<chunk_size>.num_allocations();
  }
  size_t num_deallocations() {
    return _allocator<chunk_size>.num_deallocations();
  }
  size_t num_allocated() {
    return _allocator<chunk_size>.num_allocated();
  }
  size_t num_free() {
    return _allocator<chunk_size>.num_free();
  }
  size_t num_arrays() {
    return _allocator<chunk_size>.num_arrays();
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_POOL_ALLOCATOR_H
