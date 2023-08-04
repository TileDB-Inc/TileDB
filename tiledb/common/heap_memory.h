/**
 * @file   heap_memory.h
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
 *
 * Defines TileDB-variants of dynamic (heap) memory allocation routines. When
 * the global `heap_profiler` is enabled, these routines will record memory
 * stats. Should allocation fail, stats will print and exit the program.
 */

#ifndef TILEDB_HEAP_MEMORY_H
#define TILEDB_HEAP_MEMORY_H

#include <cstdlib>
#include <memory>
#include <string>

#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/heap_profiler.h"

namespace tiledb {
namespace common {

extern std::recursive_mutex __tdb_heap_mem_lock;

/** TileDB variant of `malloc`. */
void* tiledb_malloc(size_t size, const std::string& label);

/** TileDB variant of `calloc`. */
void* tiledb_calloc(size_t num, size_t size, const std::string& label);

/** TileDB variant of `realloc`. */
void* tiledb_realloc(void* p, size_t size, const std::string& label);

/** TileDB variant of `free`. */
void tiledb_free(void* p);

/** TileDB variant of `operator new`. */
template <typename T, typename... Args>
T* tiledb_new(const std::string& label, Args&&... args) {
  if (!heap_profiler.enabled()) {
    return new T(std::forward<Args>(args)...);
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  T* const p = new T(std::forward<Args>(args)...);

  if (!p)
    heap_profiler.dump_and_terminate();

  heap_profiler.record_alloc(p, sizeof(T), label);

  return p;
}

/** TileDB variant of `operator delete`. */
template <typename T>
void tiledb_delete(T* const p) {
  if (!heap_profiler.enabled()) {
    delete p;
    return;
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  auto p_orig{std::launder(reinterpret_cast<const char*>(p))};
  delete p;
  heap_profiler.record_dealloc(p_orig);
}

/** TileDB variant of `operator new[]`. */
template <typename T>
T* tiledb_new_array(const std::size_t size, const std::string& label) {
  if (!heap_profiler.enabled()) {
    return new T[size];
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  T* const p = new T[size];

  if (!p)
    heap_profiler.dump_and_terminate();

  heap_profiler.record_alloc(p, sizeof(T) * size, label);

  return p;
}

/** TileDB variant of `operator delete[]`. */
template <typename T>
void tiledb_delete_array(T* const p) {
  if (!heap_profiler.enabled()) {
    delete[] p;
    return;
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  delete[] p;
  heap_profiler.record_dealloc(p);
}

/** TileDB variant of `std::unique_ptr`. */
template <class T>
struct TileDBUniquePtrDeleter {
  TileDBUniquePtrDeleter() = default;
  template <class U>
  TileDBUniquePtrDeleter(const TileDBUniquePtrDeleter<U>&) noexcept {
  }
  void operator()(T* const p) {
    tiledb_delete<T>(p);
  }
};
template <class T>
using tiledb_unique_ptr = std::unique_ptr<T, TileDBUniquePtrDeleter<T>>;

template <class T, class... Args>
tiledb_unique_ptr<T> make_unique(const std::string& label, Args&&... args) {
  return {tiledb_new<T>(label, std::forward<Args>(args)...), {}};
}

template <class T, int n, class... Args>
tiledb_unique_ptr<T> make_unique(const char (&label)[n], Args&&... args) {
  return {tiledb_new<T>(label, std::forward<Args>(args)...), {}};
}

}  // namespace common
}  // namespace tiledb

#define TILEDB_HEAP_MEM_LABEL HERE()

#define tdb_malloc(size) \
  tiledb::common::tiledb_malloc(size, TILEDB_HEAP_MEM_LABEL)

#define tdb_calloc(num, size) \
  tiledb::common::tiledb_calloc(num, size, TILEDB_HEAP_MEM_LABEL)

#define tdb_realloc(p, size) \
  tiledb::common::tiledb_realloc(p, size, TILEDB_HEAP_MEM_LABEL)

#define tdb_free(p) tiledb::common::tiledb_free(p)

#ifdef _MSC_VER
#define tdb_new(T, ...) \
  tiledb::common::tiledb_new<T>(TILEDB_HEAP_MEM_LABEL, __VA_ARGS__)
#else
#define tdb_new(T, ...) \
  tiledb::common::tiledb_new<T>(TILEDB_HEAP_MEM_LABEL, ##__VA_ARGS__)
#endif

#define tdb_delete(p) tiledb::common::tiledb_delete(p);

#define tdb_new_array(T, size) \
  tiledb::common::tiledb_new_array<T>(size, TILEDB_HEAP_MEM_LABEL)

#define tdb_delete_array(p) tiledb::common::tiledb_delete_array(p)

#define tdb_unique_ptr tiledb::common::tiledb_unique_ptr

#endif  // TILEDB_HEAP_MEMORY_H
