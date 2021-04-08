/**
 * @file   heap_memory.cc
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
 * Implements TileDB-variants of dynamic (heap) memory allocation routines.
 */

#include <cstdlib>

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/heap_profiler.h"

namespace tiledb {
namespace common {

// Protects against races between memory management APIs
// and the HeapProfiler API.
std::recursive_mutex __tdb_heap_mem_lock;

void* tiledb_malloc(const size_t size, const std::string& label) {
  if (!heap_profiler.enabled()) {
    return std::malloc(size);
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  void* const p = std::malloc(size);

  if (!p)
    heap_profiler.dump_and_terminate();

  heap_profiler.record_alloc(p, size, label);

  return p;
}

void* tiledb_calloc(
    const size_t num, const size_t size, const std::string& label) {
  if (!heap_profiler.enabled()) {
    return std::calloc(num, size);
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  void* const p = std::calloc(num, size);

  if (!p)
    heap_profiler.dump_and_terminate();

  heap_profiler.record_alloc(p, num * size, label);

  return p;
}

void* tiledb_realloc(
    void* const p, const size_t size, const std::string& label) {
  if (!heap_profiler.enabled()) {
    return std::realloc(p, size);
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  void* const p_realloc = std::realloc(p, size);

  if (!p_realloc)
    heap_profiler.dump_and_terminate();

  heap_profiler.record_dealloc(p);
  heap_profiler.record_alloc(p_realloc, size, label);

  return p_realloc;
}

void tiledb_free(void* const p) {
  if (!heap_profiler.enabled()) {
    free(p);
    return;
  }

  std::unique_lock<std::recursive_mutex> ul(__tdb_heap_mem_lock);

  free(p);
  heap_profiler.record_dealloc(p);
}

}  // namespace common
}  // namespace tiledb
